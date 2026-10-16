package main

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"testing"
	"time"

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const ns = "velero"

func mkBackup(name string, ts time.Time, phase velerov1.BackupPhase, schedule string) *velerov1.Backup {
	return &velerov1.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         ns,
			UID:               types.UID("uid-" + name),
			Labels:            map[string]string{velerov1.ScheduleNameLabel: schedule},
			CreationTimestamp: metav1.NewTime(ts),
		},
		Status: velerov1.BackupStatus{Phase: phase, CompletionTimestamp: completionOf(ts)},
	}
}

func mkSchedule(name string, annotations map[string]string) *velerov1.Schedule {
	return &velerov1.Schedule{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID("uid-sch-" + name), Annotations: annotations},
	}
}

// enforcing adds the opt-in annotation so retention actually creates DeleteBackupRequests in
// tests; without it every schedule is evaluated in dry run even with DryRun=false.
func enforcing(annotations map[string]string) map[string]string {
	out := make(map[string]string, len(annotations)+1)
	for k, v := range annotations {
		out[k] = v
	}
	out[Enforce] = "true"
	return out
}

var testBase = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

// dailyBackups builds n Completed backups, one per day, newest first.
func dailyBackups(n int, schedule string) []*velerov1.Backup {
	out := make([]*velerov1.Backup, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, mkBackup(fmt.Sprintf("%s-%03d", schedule, i), testBase.AddDate(0, 0, -i), velerov1.BackupPhaseCompleted, schedule))
	}
	return out
}

func toObjects(objs ...any) []client.Object {
	out := make([]client.Object, 0, len(objs))
	for _, o := range objs {
		out = append(out, o.(client.Object))
	}
	return out
}

func newReconciler(objs ...client.Object) (*ScheduleReconciler, client.Client) {
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &ScheduleReconciler{Client: c, Scheme: scheme, now: func() time.Time { return testBase },
		Recorder: record.NewFakeRecorder(100)}, c
}

// collectEvents drains the FakeRecorder channel; FakeRecorder formats "type reason message".
func collectEvents(r *ScheduleReconciler) []string {
	rec, ok := r.Recorder.(*record.FakeRecorder)
	if !ok {
		return nil
	}
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func eventsContaining(events []string, substr string) []string {
	var out []string
	for _, e := range events {
		if strings.Contains(e, substr) {
			out = append(out, e)
		}
	}
	return out
}

func TestParsePolicy(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   map[string]string
		want RetentionPolicy
		// none expects errNoRetentionPolicy; invalid expects any other error.
		none    bool
		invalid bool
	}{
		{name: "nil", in: nil, none: true},
		{name: "empty", in: map[string]string{}, none: true},
		{name: "unrelated annotation is ignored", in: map[string]string{"some.other/annotation": "5"}, none: true},
		{name: "all levels zero", in: map[string]string{KeepHourly: "0"}, none: true},
		{name: "single level", in: map[string]string{KeepDaily: "7"}, want: RetentionPolicy{Daily: 7}},
		{
			name: "all levels",
			in:   map[string]string{KeepHourly: "6", KeepDaily: "7", KeepWeekly: "3", KeepMonthly: "4", KeepYearly: "2"},
			want: RetentionPolicy{Hourly: 6, Daily: 7, Weekly: 3, Monthly: 4, Yearly: 2},
		},
		{name: "explicit zero disables one level", in: map[string]string{KeepDaily: "7", KeepHourly: "0"}, want: RetentionPolicy{Daily: 7}},
		{name: "surrounding whitespace tolerated", in: map[string]string{KeepDaily: " 7 "}, want: RetentionPolicy{Daily: 7}},
		{name: "leading zero", in: map[string]string{KeepDaily: "07"}, want: RetentionPolicy{Daily: 7}},
		{name: "max allowed", in: map[string]string{KeepDaily: "10000"}, want: RetentionPolicy{Daily: 10000}},

		// Values an operator plausibly types that do not unambiguously mean "keep N".
		{name: "empty value", in: map[string]string{KeepDaily: ""}, invalid: true},
		{name: "whitespace only", in: map[string]string{KeepDaily: "   "}, invalid: true},
		{name: "negative", in: map[string]string{KeepDaily: "-3"}, invalid: true},
		{name: "not a number", in: map[string]string{KeepDaily: "abc"}, invalid: true},
		{name: "number with suffix", in: map[string]string{KeepDaily: "7x"}, invalid: true},
		{name: "scientific notation", in: map[string]string{KeepDaily: "1.153e-6"}, invalid: true},
		{name: "float", in: map[string]string{KeepDaily: "7.5"}, invalid: true},
		{name: "float with integer value", in: map[string]string{KeepDaily: "7.0"}, invalid: true},
		{name: "plus sign", in: map[string]string{KeepDaily: "+7"}, invalid: true},
		{name: "hex", in: map[string]string{KeepDaily: "0x07"}, invalid: true},
		{name: "digit separator", in: map[string]string{KeepDaily: "1_000"}, invalid: true},
		{name: "boolean", in: map[string]string{KeepDaily: "true"}, invalid: true},
		{name: "quoted", in: map[string]string{KeepDaily: `"7"`}, invalid: true},
		{name: "range", in: map[string]string{KeepDaily: "7-10"}, invalid: true},
		{name: "non-ascii digits", in: map[string]string{KeepDaily: "٧"}, invalid: true},
		{name: "overflows int", in: map[string]string{KeepDaily: "99999999999999999999"}, invalid: true},
		{name: "above maxKeep", in: map[string]string{KeepDaily: "10001"}, invalid: true},
		{name: "one bad value invalidates the policy", in: map[string]string{KeepDaily: "7", KeepWeekly: "abc"}, invalid: true},

		// keep-last is a protection floor: parsed with the same strictness, but alone it is not
		// a retention policy.
		{name: "keep-last alone is not a policy", in: map[string]string{KeepLast: "5"}, none: true},
		{name: "keep-last alongside a quota", in: map[string]string{KeepDaily: "7", KeepLast: "3"}, want: RetentionPolicy{Daily: 7, KeepLast: 3}},
		{name: "keep-last zero", in: map[string]string{KeepDaily: "7", KeepLast: "0"}, want: RetentionPolicy{Daily: 7}},
		{name: "keep-last max", in: map[string]string{KeepDaily: "7", KeepLast: "10000"}, want: RetentionPolicy{Daily: 7, KeepLast: 10000}},
		{name: "keep-last float", in: map[string]string{KeepDaily: "7", KeepLast: "2.5"}, invalid: true},
		{name: "keep-last negative", in: map[string]string{KeepDaily: "7", KeepLast: "-1"}, invalid: true},
		{name: "keep-last above max", in: map[string]string{KeepDaily: "7", KeepLast: "10001"}, invalid: true},
		{name: "keep-last typo", in: map[string]string{KeepDaily: "7", AnnotationPrefix + "keep-lst": "2"}, invalid: true},

		// Typos in the key are the dangerous case: ignoring the key would silently drop a level.
		{name: "misspelt level", in: map[string]string{AnnotationPrefix + "keep-dail": "7"}, invalid: true},
		{name: "missing keep- prefix", in: map[string]string{AnnotationPrefix + "daily": "7"}, invalid: true},
		{name: "typo in the domain is not ours, so no policy", in: map[string]string{"retention.velero.ioo/keep-daily": "7"}, none: true},
		{name: "unknown level", in: map[string]string{AnnotationPrefix + "keep-decadely": "1"}, invalid: true},
		{name: "typo alongside a valid level", in: map[string]string{KeepDaily: "7", AnnotationPrefix + "keep-weekl": "3"}, invalid: true},
		{name: "our prefix, not a level", in: map[string]string{AnnotationPrefix + "enabled": "true"}, invalid: true},
	} {
		got, err := parsePolicy(tc.in)
		switch {
		case tc.none:
			if !stderrors.Is(err, errNoRetentionPolicy) {
				t.Errorf("%s: err=%v, want errNoRetentionPolicy", tc.name, err)
			}
		case tc.invalid:
			if err == nil || stderrors.Is(err, errNoRetentionPolicy) {
				t.Errorf("%s: err=%v, want a configuration error", tc.name, err)
			}
		default:
			if err != nil {
				t.Errorf("%s: unexpected error %v", tc.name, err)
			}
			if got != tc.want {
				t.Errorf("%s: policy %+v, want %+v", tc.name, got, tc.want)
			}
		}
	}
}

// A malformed annotation must name every problem, since the operator reads this from the log with
// no other feedback channel.
func TestParsePolicyErrorMessages(t *testing.T) {
	_, err := parsePolicy(map[string]string{
		KeepDaily:                       "1.153e-6",
		AnnotationPrefix + "keep-weekl": "3",
	})
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, want := range []string{"keep-daily", "1.153e-6", "keep-weekl", "unknown annotation"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err.Error(), want)
		}
	}
	if !strings.Contains(err.Error(), KeepWeekly) {
		t.Errorf("error should list valid keys including %s: %q", KeepWeekly, err.Error())
	}
}

// An annotation from another tool that merely shares a suffix must not disable retention.
func TestForeignAnnotationsDoNotBlockRetention(t *testing.T) {
	_, err := parsePolicy(map[string]string{
		KeepDaily:                            "7",
		"kubectl.kubernetes.io/last-applied": "x",
		"argocd.argoproj.io/sync-wave":       "2",
		"velero.io/keep-daily":               "legacy value",
	})
	if err != nil {
		t.Errorf("foreign annotations must be ignored, got %v", err)
	}
}

// A retention annotation under a misspelt domain is not ours to enforce, so it must not fail the
// policy — but it must be reported, otherwise the operator believes retention is running.
func TestNearMissAnnotations(t *testing.T) {
	got := nearMissAnnotations(map[string]string{
		"retention.velero.ioo/keep-daily":  "7",
		"velero.io/keep-weekly":            "4",
		KeepDaily:                          "7",
		"argocd.argoproj.io/sync-wave":     "2",
		"kubectl.kubernetes.io/last":       "x",
		"some.tool/keep-daily":             "1",
		"retention.velero.io/keep-daily-x": "1",
	})
	if len(got) != 3 {
		t.Fatalf("expected 3 suspects, got %v", got)
	}
	for _, want := range []string{"retention.velero.ioo/keep-daily", "velero.io/keep-weekly", "some.tool/keep-daily"} {
		found := false
		for _, s := range got {
			if strings.HasPrefix(s, want+" ") {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s not flagged in %v", want, got)
		}
	}
}

// The whole point of failing closed: a typo in a quota must never widen the deletion set.
func TestInvalidAnnotationDeletesNothing(t *testing.T) {
	r, c := newReconciler(mkSchedule("pg", map[string]string{AnnotationPrefix + "keep-dail": "1", Enforce: "true"}),
		mkBackup("pg-old", testBase.AddDate(0, 0, -30), velerov1.BackupPhaseCompleted, "pg"),
		mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"))
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
	})
	if err != nil {
		t.Fatalf("a bad annotation must not return an error, got %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Error("invalid policy should be retried, not dropped")
	}
	if got := deleteRequestsFor(t, c); len(got) != 0 {
		t.Errorf("invalid policy must delete nothing, got %v", got)
	}
}

// Each level in isolation must keep exactly N buckets.
func TestGetBackupsToKeepPerLevel(t *testing.T) {
	hourly := make([]*velerov1.Backup, 0, 100)
	for i := 0; i < 100; i++ {
		hourly = append(hourly, mkBackup(fmt.Sprintf("h%03d", i), testBase.Add(-time.Duration(i)*time.Hour), velerov1.BackupPhaseCompleted, "sch"))
	}

	cases := []struct {
		name    string
		backups []*velerov1.Backup
		policy  RetentionPolicy
		want    int
	}{
		{"daily=1 over 400 days", dailyBackups(400, "sch"), RetentionPolicy{Daily: 1}, 1},
		{"daily=7 over 400 days", dailyBackups(400, "sch"), RetentionPolicy{Daily: 7}, 7},
		{"weekly=2 over 400 days", dailyBackups(400, "sch"), RetentionPolicy{Weekly: 2}, 2},
		{"monthly=3 over 400 days", dailyBackups(400, "sch"), RetentionPolicy{Monthly: 3}, 3},
		{"yearly=2 over 400 days", dailyBackups(400, "sch"), RetentionPolicy{Yearly: 2}, 2},
		{"hourly=24 over 100 hours", hourly, RetentionPolicy{Hourly: 24}, 24},
		{"empty policy keeps nothing", dailyBackups(400, "sch"), RetentionPolicy{}, 0},
	}
	for _, tc := range cases {
		if got := len(getBackupsToKeep(tc.backups, tc.policy)); got != tc.want {
			t.Errorf("%s: kept %d, want %d", tc.name, got, tc.want)
		}
	}
}

// GFS semantics: an old backup kept for a coarse level also occupies a day/hour bucket, so the
// union spans more days than keep-daily alone. The binding limits are per level (covered by
// TestGetBackupsToKeepPerLevel); here we check the union stays within sane bounds.
func TestGetBackupsToKeepCombinedGFS(t *testing.T) {
	backups := dailyBackups(730, "sch")
	policy := RetentionPolicy{Hourly: 2, Daily: 3, Weekly: 2, Monthly: 3, Yearly: 2}

	kept := getBackupsToKeep(backups, policy)
	if !kept[types.NamespacedName{Namespace: ns, Name: "sch-000"}] {
		t.Error("newest backup must always be kept")
	}

	// No backup may be kept more times than it exists, and the union cannot exceed the sum
	// of the per-level quotas.
	sum := policy.Hourly + policy.Daily + policy.Weekly + policy.Monthly + policy.Yearly
	if len(kept) > sum {
		t.Errorf("kept %d backups, exceeds sum of per-level quotas %d", len(kept), sum)
	}
	if len(kept) < policy.Yearly {
		t.Errorf("kept %d backups, fewer than keep-yearly %d", len(kept), policy.Yearly)
	}

	distinct := map[string]map[string]bool{"monthly": {}, "yearly": {}, "weekly": {}}
	for _, b := range backups {
		if !kept[client.ObjectKeyFromObject(b)] {
			continue
		}
		ts := b.CreationTimestamp.Time
		y, w := ts.ISOWeek()
		distinct["weekly"][fmt.Sprintf("%d-%d", y, w)] = true
		distinct["monthly"][ts.Format("2006-01")] = true
		distinct["yearly"][ts.Format("2006")] = true
	}
	// Coarse levels must actually reach back in time. A backup kept by one level also lands in
	// every other level's buckets, so the union can exceed a level's quota but never fall short
	// of it when enough history exists (730 daily backups span 3 years and ~25 months).
	if len(distinct["yearly"]) < policy.Yearly {
		t.Errorf("yearly: kept %d distinct years, want at least %d", len(distinct["yearly"]), policy.Yearly)
	}
	if len(distinct["monthly"]) < policy.Monthly {
		t.Errorf("monthly: kept %d distinct months, want at least %d", len(distinct["monthly"]), policy.Monthly)
	}
	if len(distinct["weekly"]) < policy.Weekly {
		t.Errorf("weekly: kept %d distinct weeks, want at least %d", len(distinct["weekly"]), policy.Weekly)
	}
}

// Retention must be independent of the input ordering: the same set of backups listed in a
// different order must produce the same keep set.
func TestGetBackupsToKeepIsOrderIndependent(t *testing.T) {
	backups := dailyBackups(60, "sch")
	policy := RetentionPolicy{Daily: 5, Weekly: 2, Monthly: 2}

	want := getBackupsToKeep(backups, policy)

	shuffled := make([]*velerov1.Backup, len(backups))
	copy(shuffled, backups)
	for i, j := 0, len(shuffled)-1; i < j; i, j = i+1, j-1 {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}
	got := getBackupsToKeep(shuffled, policy)

	if len(got) != len(want) {
		t.Fatalf("unsorted input kept %d backups, sorted input kept %d", len(got), len(want))
	}
	for uid := range want {
		if !got[uid] {
			t.Errorf("unsorted input changed the keep set: missing %s", uid)
		}
	}
}

func TestReconcileCreatesDeleteBackupRequests(t *testing.T) {
	objs := toObjects(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "3"})))
	for _, b := range dailyBackups(10, "pg") {
		objs = append(objs, b)
	}
	// Owned by a different schedule: must never be touched.
	objs = append(objs, mkBackup("other-000", testBase.AddDate(0, 0, -500), velerov1.BackupPhaseCompleted, "other"))
	// Failed backups are not retention candidates.
	objs = append(objs, mkBackup("pg-failed", testBase.AddDate(0, 0, -200), velerov1.BackupPhaseFailed, "pg"))

	r, c := newReconciler(objs...)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns}}); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	var backups velerov1.BackupList
	if err := c.List(context.Background(), &backups, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	// Retention must not remove Backup objects itself: purging object storage is Velero's job.
	if len(backups.Items) != 12 {
		t.Errorf("expected all 12 Backup CRs intact, got %d", len(backups.Items))
	}

	var requests velerov1.DeleteBackupRequestList
	if err := c.List(context.Background(), &requests, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	requested := map[string]bool{}
	for _, d := range requests.Items {
		requested[d.Spec.BackupName] = true
	}
	if len(requests.Items) != 7 {
		t.Errorf("expected 7 delete requests (10 completed - keep-daily 3), got %d: %v", len(requests.Items), requested)
	}
	for _, mustKeep := range []string{"pg-failed", "other-000", "pg-000", "pg-001", "pg-002"} {
		if requested[mustKeep] {
			t.Errorf("%s must not be scheduled for deletion", mustKeep)
		}
	}
	for _, mustGo := range []string{"pg-003", "pg-009"} {
		if !requested[mustGo] {
			t.Errorf("%s is outside keep-daily=3 and must be scheduled for deletion", mustGo)
		}
	}
}

// Re-running must not pile up duplicate requests for the same backup.
func TestReconcileIsIdempotent(t *testing.T) {
	objs := toObjects(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "3"})))
	for _, b := range dailyBackups(10, "pg") {
		objs = append(objs, b)
	}

	r, c := newReconciler(objs...)
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns}}
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatalf("reconcile %d error: %v", i, err)
		}
	}

	var requests velerov1.DeleteBackupRequestList
	if err := c.List(context.Background(), &requests, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	if len(requests.Items) != 7 {
		t.Errorf("reconcile created duplicates: got %d delete requests, want 7", len(requests.Items))
	}
}

// Backups whose deletion is still in flight must not consume the retention budget, otherwise
// they push genuinely wanted backups out of the keep set.
func TestReconcileIgnoresBackupsPendingDeletion(t *testing.T) {
	objs := toObjects(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "3"})))
	for i := 0; i < 3; i++ {
		objs = append(objs, mkBackup(fmt.Sprintf("keep-%d", i), testBase.AddDate(0, 0, -i), velerov1.BackupPhaseCompleted, "pg"))
	}
	for i := 0; i < 3; i++ {
		b := mkBackup(fmt.Sprintf("dying-%d", i), testBase.AddDate(0, 0, -(20+i)), velerov1.BackupPhaseCompleted, "pg")
		dt := metav1.NewTime(testBase.Add(-time.Hour))
		b.DeletionTimestamp = &dt
		b.Finalizers = []string{"test/hold-open"}
		objs = append(objs, b)
	}

	r, c := newReconciler(objs...)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns}}); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	var requests velerov1.DeleteBackupRequestList
	if err := c.List(context.Background(), &requests, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	if len(requests.Items) != 0 {
		names := map[string]bool{}
		for _, d := range requests.Items {
			names[d.Spec.BackupName] = true
		}
		t.Errorf("terminating backups consumed the retention budget; unexpected delete requests: %v", names)
	}
}

func TestReconcileScheduleWithoutPolicyIsNoop(t *testing.T) {
	objs := toObjects(mkSchedule("pg", nil))
	for _, b := range dailyBackups(10, "pg") {
		objs = append(objs, b)
	}

	r, c := newReconciler(objs...)
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns}})
	if err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Error("expected the schedule to be requeued even without a policy")
	}
	var requests velerov1.DeleteBackupRequestList
	if err := c.List(context.Background(), &requests, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	if len(requests.Items) != 0 {
		t.Errorf("no policy must mean no deletions, got %d", len(requests.Items))
	}
}

// Dry-run must not create anything, while still selecting the same backups to delete.
func TestDryRunCreatesNothing(t *testing.T) {
	r, c := newReconciler(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "1"})),
		mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"),
		mkBackup("pg-old", testBase.AddDate(0, 0, -30), velerov1.BackupPhaseCompleted, "pg"))
	r.DryRun = true
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
	}); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if got := deleteRequestsFor(t, c); len(got) != 0 {
		t.Errorf("dry run must create no DeleteBackupRequest, got %v", got)
	}
}

// Write mode requires both switches: DryRun=false on the reconciler and Enforce: "true" on the
// schedule.
func TestWriteModeCreates(t *testing.T) {
	r, c := newReconciler(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "1"})),
		mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"),
		mkBackup("pg-old", testBase.AddDate(0, 0, -30), velerov1.BackupPhaseCompleted, "pg"))
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
	}); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if got := deleteRequestsFor(t, c); !got["pg-old"] || got["pg-recent"] {
		t.Errorf("write mode should delete only pg-old, got %v", got)
	}
}

// Without the enforce annotation a schedule stays in dry run even when the controller runs in
// write mode — the opt-in is per schedule, never implied by having quotas at all.
func TestScheduleWithoutEnforceStaysDryRun(t *testing.T) {
	r, c := newReconciler(mkSchedule("pg", map[string]string{KeepDaily: "1"}),
		mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"),
		mkBackup("pg-old", testBase.AddDate(0, 0, -30), velerov1.BackupPhaseCompleted, "pg"))
	r.DryRun = false
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
	}); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if got := deleteRequestsFor(t, c); len(got) != 0 {
		t.Errorf("schedule without %s must never create requests, got %v", Enforce, got)
	}
}

// Each global-flag x enforce combination, asserted explicitly so neither switch can be silently
// dropped from the write gate.
func TestEnforceCombinationTable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		globalDry bool
		enforce   string // "", "true", "false", or a malformed value
		wantWrite bool
	}{
		{"both off", true, "", false},
		{"global dry, enforce true", true, "true", false},
		{"write mode, no annotation", false, "", false},
		{"write mode, enforce false", false, "false", false},
		{"write mode, enforce true", false, "true", true},
		{"write mode, enforce typo", false, "ture", false},
	} {
		annotations := map[string]string{KeepDaily: "1"}
		if tc.enforce != "" {
			annotations[Enforce] = tc.enforce
		}
		r, c := newReconciler(mkSchedule("pg", annotations),
			mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"),
			mkBackup("pg-old", testBase.AddDate(0, 0, -30), velerov1.BackupPhaseCompleted, "pg"))
		r.DryRun = tc.globalDry
		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
		}); err != nil {
			t.Fatalf("%s: reconcile error: %v", tc.name, err)
		}
		got := deleteRequestsFor(t, c)
		if tc.wantWrite && !got["pg-old"] {
			t.Errorf("%s: expected a delete request, got none", tc.name)
		}
		if !tc.wantWrite && len(got) != 0 {
			t.Errorf("%s: expected no delete requests, got %v", tc.name, got)
		}
	}
}

// A typo in the deletion switch itself must be loud, not silently treated as false.
func TestMalformedEnforceRefusesAndReports(t *testing.T) {
	for _, raw := range []string{"ture", "True", "TRUE", "1", "yes", " true", ""} {
		_, err := parseEnforce(map[string]string{Enforce: raw})
		if raw == "" {
			// An empty value is ambiguous intent — it must error like any other non-boolean.
			if err == nil {
				t.Errorf("empty %s must be an error", Enforce)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s=%q must be an error", Enforce, raw)
		}
	}
	enforce, err := parseEnforce(map[string]string{})
	if err != nil || enforce {
		t.Errorf("missing %s must mean dry run, got (%v, %v)", Enforce, enforce, err)
	}
	for _, raw := range []string{"true", "false"} {
		if _, err := parseEnforce(map[string]string{Enforce: raw}); err != nil {
			t.Errorf("%s=%q must parse, got %v", Enforce, raw, err)
		}
	}
}

// Enforce is ours, so it must not trip the unknown-key rejection alongside quotas.
func TestEnforceIsKnownAnnotation(t *testing.T) {
	if _, err := parsePolicy(enforcing(map[string]string{KeepDaily: "7"})); err != nil {
		t.Errorf("quota + enforce must parse together, got %v", err)
	}
	// Enforce alone is not a quota policy: it must not enable retention by itself.
	if _, err := parsePolicy(map[string]string{Enforce: "true"}); !stderrors.Is(err, errNoRetentionPolicy) {
		t.Errorf("enforce without quotas must be a no-op, got %v", err)
	}
}

// Retention is GFS on completion time, not TTL: an Expiration far in the future (the operator
// deliberately sets TTL to ~100 years so Velero's own GC never fires) must not protect an old
// backup, and a long-past Expiration must not delete a recent one.
func TestExpirationDoesNotAffectRetention(t *testing.T) {
	old := mkBackup("pg-old", testBase.AddDate(0, 0, -30), velerov1.BackupPhaseCompleted, "pg")
	exp := metav1.NewTime(testBase.AddDate(100, 0, 0))
	old.Status.Expiration = &exp

	r, c := newReconciler(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "1"})),
		old,
		mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"))
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
	}); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if got := deleteRequestsFor(t, c); !got["pg-old"] {
		t.Errorf("a 100-year Expiration/TTL must not exempt a backup from GFS retention, got %v", got)
	}
}

// A backup whose only timestamp is CreationTimestamp is treated as having no time at all.
func TestCreationTimestampOnlyIsTreatedAsUndated(t *testing.T) {
	onlyCreation := mkBackup("pg-creation-only", testBase.AddDate(0, 0, -500), velerov1.BackupPhaseCompleted, "pg")
	onlyCreation.Status.CompletionTimestamp = nil

	r, c := newReconciler(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "1"})),
		onlyCreation,
		mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"))
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
	}); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if got := deleteRequestsFor(t, c); got["pg-creation-only"] {
		t.Errorf("CreationTimestamp must never make a backup deletable, got %v", got)
	}
	if _, ok := backupTimestamp(onlyCreation); ok {
		t.Error("a backup with only CreationTimestamp must report no usable time")
	}
}

// keep-last is a floor applied on top of GFS: the newest N are protected even when GFS buckets
// would not keep them, and it never protects fewer backups than GFS alone.
func TestKeepLastFloorExtendsKeepSet(t *testing.T) {
	backups := dailyBackups(10, "pg") // pg-000 newest .. pg-009 oldest

	// daily=1 keeps only pg-000; keep-last=3 adds pg-001, pg-002.
	got := getBackupsToKeep(backups, RetentionPolicy{Daily: 1, KeepLast: 3})
	for _, name := range []string{"pg-000", "pg-001", "pg-002"} {
		if !got[types.NamespacedName{Namespace: ns, Name: name}] {
			t.Errorf("%s must be kept with daily=1 keep-last=3", name)
		}
	}
	if got[types.NamespacedName{Namespace: ns, Name: "pg-003"}] {
		t.Error("pg-003 is outside both the quota and the floor, must not be kept")
	}

	// A floor larger than the set protects everything.
	if got := getBackupsToKeep(backups, RetentionPolicy{Daily: 1, KeepLast: 50}); len(got) != 10 {
		t.Errorf("keep-last=50 of 10 backups must keep all 10, got %d", len(got))
	}

	// keep-last=0 changes nothing versus no floor at all.
	withZero := getBackupsToKeep(backups, RetentionPolicy{Daily: 1, KeepLast: 0})
	without := getBackupsToKeep(backups, RetentionPolicy{Daily: 1})
	if len(withZero) != len(without) {
		t.Errorf("keep-last=0 must equal no floor: %d vs %d", len(withZero), len(without))
	}

	// The floor is monotone: adding it can only ever grow the keep set.
	base := getBackupsToKeep(backups, RetentionPolicy{Daily: 3, Weekly: 2})
	floored := getBackupsToKeep(backups, RetentionPolicy{Daily: 3, Weekly: 2, KeepLast: 4})
	for k := range base {
		if !floored[k] {
			t.Errorf("floor removed %v from the keep set — it must only add", k)
		}
	}
}

// Backups without a usable timestamp are already never deleted; the floor must not start
// counting them either (it operates on the same dated, newest-first ordering).
func TestKeepLastFloorIgnoresUndatedBackups(t *testing.T) {
	undated := mkBackup("pg-undated", time.Time{}, velerov1.BackupPhaseCompleted, "pg")
	undated.Status.CompletionTimestamp = nil
	backups := append(dailyBackups(3, "pg"), undated)

	got := getBackupsToKeep(backups, RetentionPolicy{Daily: 1, KeepLast: 1})
	if !got[types.NamespacedName{Namespace: ns, Name: "pg-000"}] {
		t.Error("the single newest dated backup must be kept")
	}
}

func TestEventEmitters(t *testing.T) {
	newSchedule := func(annotations map[string]string) *velerov1.Schedule {
		return mkSchedule("pg", annotations)
	}
	backups := func() []client.Object {
		return []client.Object{
			mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"),
			mkBackup("pg-old", testBase.AddDate(0, 0, -30), velerov1.BackupPhaseCompleted, "pg"),
		}
	}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns}}

	t.Run("policy error warns on every reconcile", func(t *testing.T) {
		objs := append([]client.Object{newSchedule(map[string]string{AnnotationPrefix + "keep-dail": "7"})}, backups()...)
		r, _ := newReconciler(objs...)
		for i := 0; i < 3; i++ {
			if _, err := r.Reconcile(context.Background(), req); err != nil {
				t.Fatalf("reconcile %d: %v", i, err)
			}
		}
		events := collectEvents(r)
		if got := eventsContaining(events, "InvalidRetentionPolicy"); len(got) != 3 {
			t.Errorf("policy errors must repeat every reconcile, got %d of 3: %v", len(got), events)
		}
		if got := eventsContaining(events, "Normal"); len(got) != 0 {
			t.Errorf("a broken policy must not emit Normal events: %v", got)
		}
	})

	t.Run("applied policy announces once per change", func(t *testing.T) {
		r, _ := newReconciler(append([]client.Object{newSchedule(enforcing(map[string]string{KeepDaily: "3"}))}, backups()...)...)
		for i := 0; i < 3; i++ {
			if _, err := r.Reconcile(context.Background(), req); err != nil {
				t.Fatalf("reconcile %d: %v", i, err)
			}
		}
		events := collectEvents(r)
		if got := eventsContaining(events, "RetentionPolicyApplied"); len(got) != 1 {
			t.Errorf("steady-state reconciles must emit the policy once, got %d: %v", len(got), events)
		}

		// A schedule change re-announces with the new content.
		var s velerov1.Schedule
		if err := r.Get(context.Background(), req.NamespacedName, &s); err != nil {
			t.Fatal(err)
		}
		s.Annotations = enforcing(map[string]string{KeepDaily: "5"})
		if err := r.Update(context.Background(), &s); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		events = collectEvents(r)
		applied := eventsContaining(events, "RetentionPolicyApplied")
		if len(applied) != 1 {
			t.Fatalf("annotation change must re-announce, got %d: %v", len(applied), events)
		}
		if !strings.Contains(applied[0], "keep-daily=5") {
			t.Errorf("announcement must carry the new policy, got %q", applied[0])
		}
	})

	t.Run("fixing a policy re-announces even at an old fingerprint", func(t *testing.T) {
		r, _ := newReconciler(append([]client.Object{newSchedule(enforcing(map[string]string{KeepDaily: "3"}))}, backups()...)...)
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		collectEvents(r)

		var s velerov1.Schedule
		if err := r.Get(context.Background(), req.NamespacedName, &s); err != nil {
			t.Fatal(err)
		}
		s.Annotations = enforcing(map[string]string{KeepDaily: "3", AnnotationPrefix + "keep-dail": "9"})
		if err := r.Update(context.Background(), &s); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		collectEvents(r)

		// Back to the exact annotations applied before the breakage: still a new change.
		if err := r.Get(context.Background(), req.NamespacedName, &s); err != nil {
			t.Fatal(err)
		}
		s.Annotations = enforcing(map[string]string{KeepDaily: "3"})
		if err := r.Update(context.Background(), &s); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		events := collectEvents(r)
		if got := eventsContaining(events, "RetentionPolicyApplied"); len(got) != 1 {
			t.Errorf("repaired policy must re-announce despite matching the old fingerprint, got %d: %v", len(got), events)
		}
	})

	t.Run("deletion event is aggregated and dry run emits none", func(t *testing.T) {
		// Write mode: exactly one Normal BackupsQueuedForDeletion naming the count and oldest.
		objs := []client.Object{
			newSchedule(enforcing(map[string]string{KeepDaily: "1"})),
			mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"),
			mkBackup("pg-mid", testBase.AddDate(0, 0, -10), velerov1.BackupPhaseCompleted, "pg"),
			mkBackup("pg-old", testBase.AddDate(0, 0, -30), velerov1.BackupPhaseCompleted, "pg"),
		}
		r, _ := newReconciler(objs...)
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		events := collectEvents(r)
		deletion := eventsContaining(events, "BackupsQueuedForDeletion")
		if len(deletion) != 1 {
			t.Fatalf("want exactly one aggregated deletion event, got %v", events)
		}
		if !strings.Contains(deletion[0], "2 backup(s)") || !strings.Contains(deletion[0], "pg-old") {
			t.Errorf("deletion event must name count and oldest, got %q", deletion[0])
		}

		// Dry run: the same evaluation must produce no deletion event.
		r2, _ := newReconciler(append([]client.Object{newSchedule(map[string]string{KeepDaily: "1"})}, backups()...)...)
		if _, err := r2.Reconcile(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		if got := eventsContaining(collectEvents(r2), "BackupsQueuedForDeletion"); len(got) != 0 {
			t.Errorf("dry run must not announce deletions, got %v", got)
		}
	})

	t.Run("invalid enforce warns", func(t *testing.T) {
		r, _ := newReconciler(append([]client.Object{
			newSchedule(map[string]string{KeepDaily: "1", Enforce: "ture"}),
		}, backups()...)...)
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		if got := eventsContaining(collectEvents(r), "InvalidEnforcement"); len(got) != 1 {
			t.Errorf("malformed enforce must warn, got %v", got)
		}
	})
}

// GFS converges rather than erodes: the keep-set is the latest backup per bucket regardless of
// whether new backups keep arriving, so a paused schedule must still have retention enforced
// (the old backups beyond quota are as deletable paused as unpaused).
func TestPausedScheduleStillEnforcesRetention(t *testing.T) {
	schedule := mkSchedule("pg", enforcing(map[string]string{KeepDaily: "1"}))
	schedule.Spec.Paused = true
	r, c := newReconciler(schedule,
		mkBackup("pg-old", testBase.AddDate(0, 0, -30), velerov1.BackupPhaseCompleted, "pg"),
		mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"))
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
	}); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if got := deleteRequestsFor(t, c); !got["pg-old"] || got["pg-recent"] {
		t.Errorf("paused schedule must still enforce GFS retention, got %v", got)
	}
}

func TestReconcileMissingSchedule(t *testing.T) {
	r, _ := newReconciler(mkSchedule("other", nil))
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "gone", Namespace: ns}})
	if err != nil {
		t.Errorf("missing schedule must not error, got %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Error("missing schedule must not be requeued")
	}
}

func mkRestore(name, backupName string, phase velerov1.RestorePhase) *velerov1.Restore {
	return &velerov1.Restore{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID("uid-" + name)},
		Spec:       velerov1.RestoreSpec{BackupName: backupName},
		Status:     velerov1.RestoreStatus{Phase: phase},
	}
}

func deleteRequestsFor(t *testing.T, c client.Client) map[string]bool {
	t.Helper()
	var requests velerov1.DeleteBackupRequestList
	if err := c.List(context.Background(), &requests, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, d := range requests.Items {
		out[d.Spec.BackupName] = true
	}
	return out
}

func completionOf(ts time.Time) *metav1.Time {
	if ts.IsZero() {
		return nil
	}
	t := metav1.NewTime(ts)
	return &t
}

// be left alone rather than deleted as "not kept".
func TestBackupWithoutCompletionTimestampIsNeverDeleted(t *testing.T) {
	noCompletion := mkBackup("pg-no-completion", testBase.AddDate(0, 0, -100), velerov1.BackupPhaseCompleted, "pg")
	noCompletion.Status.CompletionTimestamp = nil

	r, c := newReconciler(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "1"})),
		noCompletion,
		mkBackup("pg-001", testBase, velerov1.BackupPhaseCompleted, "pg"))
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
	}); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	if got := deleteRequestsFor(t, c); got["pg-no-completion"] {
		t.Errorf("backup with unknown completion time must be left alone, got %v", got)
	}
}

// Backups imported by the backup-sync controller carry the import time as CreationTimestamp; their
// real age lives in Status. Selecting on CreationTimestamp ranks a 400-day-old synced backup as
// newest and deletes genuinely recent ones instead.
func TestSyncedBackupsAgedByCompletionNotCreation(t *testing.T) {
	old1 := mkBackup("pg-old-1", testBase.Add(-5*time.Minute), velerov1.BackupPhaseCompleted, "pg")
	old1.Status.CompletionTimestamp = completionOf(testBase.AddDate(0, 0, -400))
	old2 := mkBackup("pg-old-2", testBase.Add(-5*time.Minute), velerov1.BackupPhaseCompleted, "pg")
	old2.Status.CompletionTimestamp = completionOf(testBase.AddDate(0, 0, -410))

	r, c := newReconciler(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "2"})),
		old1, old2,
		mkBackup("pg-recent-1", testBase, velerov1.BackupPhaseCompleted, "pg"),
		mkBackup("pg-recent-2", testBase.AddDate(0, 0, -1), velerov1.BackupPhaseCompleted, "pg"))
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
	}); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}

	got := deleteRequestsFor(t, c)
	for _, name := range []string{"pg-recent-1", "pg-recent-2"} {
		if got[name] {
			t.Errorf("%s is within the daily quota but was deleted: %v", name, got)
		}
	}
	for _, name := range []string{"pg-old-1", "pg-old-2"} {
		if !got[name] {
			t.Errorf("%s is 400+ days old and must be deleted: %v", name, got)
		}
	}
}

// A future-dated completion would sort ahead of everything and win every bucket; skip it instead.
func TestFutureDatedBackupIsNeverDeleted(t *testing.T) {
	r, c := newReconciler(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "1"})),
		mkBackup("pg-future", testBase.AddDate(5, 0, 0), velerov1.BackupPhaseCompleted, "pg"),
		mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"),
		mkBackup("pg-old", testBase.AddDate(0, 0, -9), velerov1.BackupPhaseCompleted, "pg"))
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
	}); err != nil {
		t.Fatalf("reconcile error: %v", err)
	}
	got := deleteRequestsFor(t, c)
	if got["pg-future"] {
		t.Errorf("future-dated backup must be skipped, got %v", got)
	}
	if got["pg-recent"] {
		t.Errorf("future-dated backup must not consume the daily quota, got %v", got)
	}
	if !got["pg-old"] {
		t.Errorf("the genuinely old backup must be deleted, got %v", got)
	}
}

// Velero's backup-deletion controller cascades a backup deletion to every Restore referencing it,
// so a backup an unfinished restore depends on must be spared.
func TestBackupWithUnfinishedRestoreIsProtected(t *testing.T) {
	for _, phase := range []velerov1.RestorePhase{
		velerov1.RestorePhaseNew,
		velerov1.RestorePhaseInProgress,
		velerov1.RestorePhaseWaitingForPluginOperations,
		velerov1.RestorePhaseWaitingForPluginOperationsPartiallyFailed,
		velerov1.RestorePhaseFinalizing,
		velerov1.RestorePhaseFinalizingPartiallyFailed,
		"",
	} {
		r, c := newReconciler(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "1"})),
			mkBackup("pg-old", testBase.AddDate(0, 0, -30), velerov1.BackupPhaseCompleted, "pg"),
			mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"),
			mkRestore("restore-1", "pg-old", phase))
		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
		}); err != nil {
			t.Fatalf("phase %q: reconcile error: %v", phase, err)
		}
		if got := deleteRequestsFor(t, c); got["pg-old"] {
			t.Errorf("restore phase %q: deleting the backup would abort the restore", phase)
		}
	}
}

// Finished restores must not pin a backup forever.
func TestBackupWithFinishedRestoreIsDeletable(t *testing.T) {
	for _, phase := range []velerov1.RestorePhase{
		velerov1.RestorePhaseCompleted,
		velerov1.RestorePhasePartiallyFailed,
		velerov1.RestorePhaseFailed,
		velerov1.RestorePhaseFailedValidation,
	} {
		r, c := newReconciler(mkSchedule("pg", enforcing(map[string]string{KeepDaily: "1"})),
			mkBackup("pg-old", testBase.AddDate(0, 0, -30), velerov1.BackupPhaseCompleted, "pg"),
			mkBackup("pg-recent", testBase, velerov1.BackupPhaseCompleted, "pg"),
			mkRestore("restore-1", "pg-old", phase))
		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: "pg", Namespace: ns},
		}); err != nil {
			t.Fatalf("phase %q: reconcile error: %v", phase, err)
		}
		if got := deleteRequestsFor(t, c); !got["pg-old"] {
			t.Errorf("restore phase %q is terminal; backup should be deletable, got %v", phase, got)
		}
	}
}

// Second-precision timestamps tie for backups synced in the same instant; the survivor must not
// depend on the order the API server happened to return.
func TestTiedTimestampsBreakDeterministically(t *testing.T) {
	build := func(reversed bool) []*velerov1.Backup {
		list := []*velerov1.Backup{
			mkBackup("pg-20261015120000", testBase, velerov1.BackupPhaseCompleted, "pg"),
			mkBackup("pg-20261015120001", testBase, velerov1.BackupPhaseCompleted, "pg"),
			mkBackup("pg-20261015120002", testBase, velerov1.BackupPhaseCompleted, "pg"),
		}
		if reversed {
			return []*velerov1.Backup{list[2], list[1], list[0]}
		}
		return list
	}

	survivors := func(list []*velerov1.Backup) []string {
		kept := getBackupsToKeep(list, RetentionPolicy{Daily: 1})
		out := []string{}
		for _, b := range list {
			if kept[client.ObjectKeyFromObject(b)] {
				out = append(out, b.Name)
			}
		}
		return out
	}

	forward := survivors(build(false))
	reversed := survivors(build(true))
	if len(forward) != 1 || len(reversed) != 1 {
		t.Fatalf("expected exactly one survivor, got %v and %v", forward, reversed)
	}
	if forward[0] != reversed[0] {
		t.Errorf("survivor depends on API list order: %v vs %v", forward, reversed)
	}
	// Velero encodes the trigger time in the name, so the newest name wins the tie.
	if forward[0] != "pg-20261015120002" {
		t.Errorf("tie must resolve to the newest name, got %v", forward[0])
	}
}

func TestResolveNamespace(t *testing.T) {
	cases := []struct {
		flag, pod, want string
		wantErr         bool
	}{
		{"velero", "", "velero", false},
		{"", "from-downward-api", "from-downward-api", false},
		{"explicit", "pod", "explicit", false},
		{"  spaced  ", "", "spaced", false},
		{"", "", "", true},
		{"   ", "  ", "", true},
	}
	for _, tc := range cases {
		got, err := resolveNamespace(tc.flag, tc.pod)
		if tc.wantErr {
			if err == nil {
				t.Errorf("resolveNamespace(%q, %q): expected error, got %q", tc.flag, tc.pod, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("resolveNamespace(%q, %q): unexpected error %v", tc.flag, tc.pod, err)
			continue
		}
		if got != tc.want {
			t.Errorf("resolveNamespace(%q, %q) = %q, want %q", tc.flag, tc.pod, got, tc.want)
		}
	}
}
