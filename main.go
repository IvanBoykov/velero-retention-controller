package main

import (
	"context"
	stderrors "errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	velerov1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerolabel "github.com/vmware-tanzu/velero/pkg/label"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// AnnotationPrefix marks the annotations this controller owns. Anything under the prefix that is
// not one of the documented keys is a configuration error rather than something to ignore:
// a misspelt keep-dail must not silently disable the daily quota and delete every daily backup.
//
// Retention annotations go on a Schedule and set how many completed backups to keep per bucket.
// Each value must be a non-negative decimal integer (0-10000); anything else stops retention for
// that schedule instead of being skipped, so a typo can never widen the deletion set.
const (
	AnnotationPrefix = "retention.velero.io/"
	KeepHourly       = AnnotationPrefix + "keep-hourly"
	KeepDaily        = AnnotationPrefix + "keep-daily"
	KeepWeekly       = AnnotationPrefix + "keep-weekly"
	KeepMonthly      = AnnotationPrefix + "keep-monthly"
	KeepYearly       = AnnotationPrefix + "keep-yearly"

	// KeepLast is a protection floor, not a quota: the N newest completed backups are always kept,
	// unioned into the GFS keep-set regardless of how old they are or which buckets they fill.
	// It can only ever shrink the deletion set, never widen it.
	KeepLast = AnnotationPrefix + "keep-last"

	// Enforce opts a schedule into real deletions. Without it (or with "false") the schedule is
	// evaluated in dry-run even when the controller runs with -dry-run=false, so enabling writes
	// per schedule is a deliberate edit rather than a side effect of the quota annotations.
	// It can only narrow what gets deleted: the global -dry-run flag always wins.
	Enforce = AnnotationPrefix + "enforce"

	// maxKeep bounds a quota. Larger values are treated as a mistake: they are harmless on their
	// own but never intentional, and refusing to act is recoverable while deleting is not.
	maxKeep = 10000
)

// policyLevels maps each accepted quota annotation to its field. KeepLast and Enforce are also
// known keys (see isKnownAnnotation), but neither is a quota: keep-last is a protection floor
// parsed alongside the quotas, enforce is parsed by parseEnforce. Neither alone makes a schedule
// retention-managed — they can only protect or gate, never enable deletion by themselves.
var policyLevels = []struct {
	key    string
	assign func(*RetentionPolicy, int)
}{
	{KeepHourly, func(p *RetentionPolicy, v int) { p.Hourly = v }},
	{KeepDaily, func(p *RetentionPolicy, v int) { p.Daily = v }},
	{KeepWeekly, func(p *RetentionPolicy, v int) { p.Weekly = v }},
	{KeepMonthly, func(p *RetentionPolicy, v int) { p.Monthly = v }},
	{KeepYearly, func(p *RetentionPolicy, v int) { p.Yearly = v }},
}

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(velerov1.AddToScheme(scheme))
}

type RetentionPolicy struct {
	Hourly  int
	Daily   int
	Weekly  int
	Monthly int
	Yearly  int

	// KeepLast is a protection floor applied independently of the GFS buckets: the newest N
	// completed backups are always kept. It only ever shrinks the deletion set.
	KeepLast int
}

type ScheduleReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// DryRun logs the deletions it would request instead of creating DeleteBackupRequests.
	// The -dry-run flag defaults to true; this field's zero value is write mode, so any new
	// construction site must set it deliberately.
	DryRun bool

	// Recorder publishes schedule-level events for operators (kubectl describe schedule).
	// Nil-safe: emitters check before use.
	Recorder record.EventRecorder

	// appliedPolicies remembers the last retention-annotation fingerprint per schedule so the
	// "policy applied" event fires once per annotation change instead of once per reconcile.
	// Bounded by the schedule count: the NotFound path in Reconcile drops entries of deleted
	// schedules.
	appliedPolicies sync.Map // types.NamespacedName -> string

	// now defaults to time.Now; overridable in tests.
	now func() time.Time
}

func (r *ScheduleReconciler) currentTime() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

// retentionAnnotationString renders the annotations we own as a stable, human-readable summary.
// It doubles as the change-detection fingerprint: sorting makes it independent of map iteration
// order, and it is redaction-free by design because it becomes event text operators can read with
// kubectl describe. Do not add arbitrary or secret-bearing annotations to it.
func retentionAnnotationString(annotations map[string]string) string {
	owned := make([]string, 0, len(annotations))
	for key, value := range annotations {
		if strings.HasPrefix(key, AnnotationPrefix) {
			owned = append(owned, key+"="+value)
		}
	}
	sort.Strings(owned)
	return strings.Join(owned, " ")
}

// emitPolicyError warns on every reconcile while the policy stays broken. Errors repeat by intent:
// a schedule with an invalid policy is silently un-retained, which is exactly the state an
// operator must not miss. The event aggregate deduplicates repeats server-side.
func (r *ScheduleReconciler) emitPolicyError(schedule *velerov1.Schedule, err error) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(schedule, corev1.EventTypeWarning, "InvalidRetentionPolicy",
		"retention disabled, no backups will be deleted: %v", err)
}

// emitPolicyApplied announces the accepted policy once per annotation change (including the
// first sight of a schedule), so an operator can verify what the controller actually parsed.
// Steady-state reconciles emit nothing. The map entry is removed on parse failure, so a
// fixed-after-broken policy is announced as a new change even if it lands on the same fingerprint
// as the last accepted one.
func (r *ScheduleReconciler) emitPolicyApplied(schedule *velerov1.Schedule) {
	if r.Recorder == nil {
		return
	}
	key := client.ObjectKeyFromObject(schedule)
	fingerprint := retentionAnnotationString(schedule.Annotations)
	if previous, seen := r.appliedPolicies.Load(key); seen && previous == fingerprint {
		return
	}
	r.appliedPolicies.Store(key, fingerprint)
	r.Recorder.Eventf(schedule, corev1.EventTypeNormal, "RetentionPolicyApplied",
		"retention policy accepted: %s", fingerprint)
}

// +kubebuilder:rbac:groups=velero.io,resources=schedules,verbs=get;list;watch
// +kubebuilder:rbac:groups=velero.io,resources=backups,verbs=get;list;watch
// +kubebuilder:rbac:groups=velero.io,resources=restores,verbs=get;list;watch
// +kubebuilder:rbac:groups=velero.io,resources=deletebackuprequests,verbs=get;list;watch;create
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// activeRestorePhases are the Restore phases that have not finished reading from their backup.
var activeRestorePhases = map[velerov1.RestorePhase]bool{
	velerov1.RestorePhaseNew:                                       true,
	velerov1.RestorePhaseInProgress:                                true,
	velerov1.RestorePhaseWaitingForPluginOperations:                true,
	velerov1.RestorePhaseWaitingForPluginOperationsPartiallyFailed: true,
	velerov1.RestorePhaseFinalizing:                                true,
	velerov1.RestorePhaseFinalizingPartiallyFailed:                 true,
}

// backupsWithActiveRestores returns the names of backups in ns that an unfinished restore still
// depends on. Velero's backup-deletion controller cascades a backup deletion to every Restore
// that references it, so deleting such a backup would abort a restore in progress.
func backupsWithActiveRestores(ctx context.Context, c client.Client, ns string) (map[string]bool, error) {
	var restores velerov1.RestoreList
	if err := c.List(ctx, &restores, client.InNamespace(ns)); err != nil {
		return nil, err
	}
	blocked := make(map[string]bool)
	for i := range restores.Items {
		restore := &restores.Items[i]
		if restore.Spec.BackupName == "" {
			continue
		}
		// Phase is empty while a restore is queued; treat unknown phases as active so an
		// unrecognised in-progress state can never unlock a deletion.
		if restore.Status.Phase == "" || activeRestorePhases[restore.Status.Phase] {
			blocked[restore.Spec.BackupName] = true
		}
	}
	return blocked, nil
}

// requestBackupDeletion creates a DeleteBackupRequest, which is the only deletion path that
// also purges the backup from object storage and its snapshots. Deleting the Backup CR directly
// bypasses the backup-deletion controller and leaks the data, because Backups carry no finalizer.
// In dry-run mode it reports the decision without touching the API.
func (r *ScheduleReconciler) requestBackupDeletion(ctx context.Context, backup *velerov1.Backup, dryRun bool) error {
	dbr := &velerov1.DeleteBackupRequest{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: backup.Name + "-",
			Namespace:    backup.Namespace,
			Labels: map[string]string{
				velerov1.BackupNameLabel: velerolabel.GetValidName(backup.Name),
				velerov1.BackupUIDLabel:  string(backup.UID),
			},
		},
		Spec: velerov1.DeleteBackupRequestSpec{BackupName: backup.Name},
	}
	if dryRun {
		log.FromContext(ctx).Info("Dry run: would create delete backup request",
			"namespace", dbr.Namespace, "backup", backup.Name, "backupUID", backup.UID)
		return nil
	}
	return r.Create(ctx, dbr)
}

func (r *ScheduleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	now := r.currentTime()

	var schedule velerov1.Schedule
	if err := r.Get(ctx, req.NamespacedName, &schedule); err != nil {
		if errors.IsNotFound(err) {
			r.appliedPolicies.Delete(req.NamespacedName)
			logger.Info("Schedule not found, probably deleted.")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Schedule")
		return ctrl.Result{}, err
	}

	policy, err := parsePolicy(schedule.Annotations)
	if stderrors.Is(err, errNoRetentionPolicy) {
		r.appliedPolicies.Delete(req.NamespacedName)
		logger.Info("No retention policy annotations found for schedule, skipping.", "schedule", schedule.Name)
		return ctrl.Result{RequeueAfter: 1 * time.Hour}, nil
	}
	if err != nil {
		// Refuse to touch this schedule's backups. Editing the annotation re-triggers this
		// reconcile through the Schedule watch, so an hourly requeue is enough; returning the
		// error would only add backoff noise for a condition that cannot heal by itself.
		logger.Error(err, "Invalid retention policy, refusing to delete any backups for this schedule",
			"schedule", schedule.Name)
		// Forget the last accepted fingerprint so that re-fixing the policy re-announces it even
		// if the final annotations match a previously applied state.
		r.appliedPolicies.Delete(req.NamespacedName)
		r.emitPolicyError(&schedule, err)
		return ctrl.Result{RequeueAfter: 1 * time.Hour}, nil
	}
	r.emitPolicyApplied(&schedule)

	// Retention is evaluated even while a schedule is paused: GFS keeps the latest backup per
	// bucket regardless of the flow of new backups, so pausing does not turn deletion into
	// attrition. Only actual deletions are gated, by the Enforce annotation below.

	logger.Info("Processing schedule with retention policy", "schedule", schedule.Name, "policy", fmt.Sprintf("%+v", policy))
	if suspects := nearMissAnnotations(schedule.Annotations); len(suspects) > 0 {
		logger.Info("Ignoring annotations that look like misspelt retention keys",
			"schedule", schedule.Name, "annotations", strings.Join(suspects, ", "))
	}

	enforce, err := parseEnforce(schedule.Annotations)
	if err != nil {
		logger.Error(err, "Invalid enforcement annotation, refusing to delete any backups for this schedule",
			"schedule", schedule.Name)
		if r.Recorder != nil {
			r.Recorder.Eventf(&schedule, corev1.EventTypeWarning, "InvalidEnforcement",
				"retention will not delete anything: %v", err)
		}
		return ctrl.Result{RequeueAfter: 1 * time.Hour}, nil
	}
	// Two switches must both allow writes: the global -dry-run flag and this schedule's Enforce
	// annotation. Either one being off keeps the schedule in dry run.
	dryRun := r.DryRun || !enforce

	var allBackups velerov1.BackupList
	if err := r.List(ctx, &allBackups, client.InNamespace(req.Namespace), client.MatchingLabels{velerov1.ScheduleNameLabel: schedule.Name}); err != nil {
		logger.Error(err, "Failed to list backups for schedule", "schedule", schedule.Name)
		return ctrl.Result{}, err
	}

	completedBackups := make([]*velerov1.Backup, 0)
	// Names with a deletion already in flight, either marked on the Backup itself or covered by
	// an existing DeleteBackupRequest.
	queuedForDeletion := make(map[string]bool)
	undated := 0
	for i := range allBackups.Items {
		backup := &allBackups.Items[i]
		// Already being removed: don't count it against the retention budget, otherwise the
		// pending backups crowd out the ones we intend to keep.
		if backup.DeletionTimestamp != nil {
			queuedForDeletion[backup.Name] = true
			continue
		}
		if backup.Status.Phase != velerov1.BackupPhaseCompleted {
			continue
		}
		// A backup whose age cannot be established must never be deleted: it cannot be placed in
		// any bucket, so treating it as "not kept" would delete the least-understood object in
		// the set. The same reasoning covers a timestamp in the future.
		ts, ok := backupTimestamp(backup)
		if !ok || ts.After(now) {
			undated++
			continue
		}
		completedBackups = append(completedBackups, backup)
	}
	if undated > 0 {
		logger.Info("Skipping backups with no usable timestamp", "schedule", schedule.Name, "count", undated)
	}

	// Backups already queued for deletion keep the Backup CR (and its Completed phase) until
	// Velero's backup-deletion controller finishes. Skip names that already have a request,
	// otherwise every reconcile creates a duplicate.
	var existingRequests velerov1.DeleteBackupRequestList
	if err := r.List(ctx, &existingRequests, client.InNamespace(req.Namespace)); err != nil {
		logger.Error(err, "Failed to list delete backup requests", "schedule", schedule.Name)
		return ctrl.Result{}, err
	}
	for i := range existingRequests.Items {
		if name := existingRequests.Items[i].Spec.BackupName; name != "" {
			queuedForDeletion[name] = true
		}
	}

	backupsToKeep := getBackupsToKeep(completedBackups, policy)
	logger.Info("Backup retention analysis complete", "schedule", schedule.Name, "total", len(completedBackups), "to_keep", len(backupsToKeep))

	protected, err := backupsWithActiveRestores(ctx, r.Client, req.Namespace)
	if err != nil {
		logger.Error(err, "Failed to list restores", "schedule", schedule.Name)
		return ctrl.Result{}, err
	}

	deletedCount := 0
	var oldestDeleted string
	var oldestTs time.Time
	for _, backup := range completedBackups {
		if _, shouldKeep := backupsToKeep[client.ObjectKeyFromObject(backup)]; shouldKeep {
			continue
		}
		if queuedForDeletion[backup.Name] {
			continue
		}
		if protected[backup.Name] {
			logger.Info("Backup is referenced by an unfinished restore, keeping it", "backup", backup.Name)
			continue
		}
		logger.Info("Requesting deletion of old backup", "backup", backup.Name)
		if err := r.requestBackupDeletion(ctx, backup, dryRun); err != nil {
			logger.Error(err, "Failed to create delete backup request", "backup", backup.Name)
			return ctrl.Result{}, err
		}
		if ts, ok := backupTimestamp(backup); ok && (oldestDeleted == "" || ts.Before(oldestTs)) {
			oldestDeleted, oldestTs = backup.Name, ts
		}
		deletedCount++
	}
	if deletedCount > 0 {
		if dryRun {
			logger.Info("Finished evaluating retention in dry run: no DeleteBackupRequests created",
				"wouldDelete", deletedCount, "enforce", enforce)
		} else {
			logger.Info("Finished requesting backup deletions", "count", deletedCount)
			// One aggregated event per reconcile, never per backup: a first enforcement pass can
			// remove a backlog of hundreds, and per-backup events would bury every other event on
			// the object. The per-backup detail stays in the log lines above.
			if r.Recorder != nil {
				r.Recorder.Eventf(&schedule, corev1.EventTypeNormal, "BackupsQueuedForDeletion",
					"queued %d backup(s) for deletion, oldest %s", deletedCount, oldestDeleted)
			}
		}
	}

	return ctrl.Result{RequeueAfter: 1 * time.Hour}, nil
}

// errNoRetentionPolicy reports that no retention level is enabled, which is different from a
// malformed policy: there is nothing to enforce, so the schedule is simply left alone.
var errNoRetentionPolicy = stderrors.New("no retention level enabled")

// parsePolicy reads the retention annotations, failing on anything it cannot interpret exactly.
//
// Malformed input is an error rather than a skipped key because a skipped level keeps nothing, so
// a typo would silently widen the deletion set: "keep-dail: 7" disables the daily quota and
// deletes every daily backup. Refusing to act is recoverable; deleting is not.
func parsePolicy(annotations map[string]string) (RetentionPolicy, error) {
	policy := RetentionPolicy{}
	var problems []string
	enabled := 0

	for _, level := range policyLevels {
		raw, ok := annotations[level.key]
		if !ok {
			continue
		}
		value, err := parseQuota(raw)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s=%q: %v", level.key, raw, err))
			continue
		}
		level.assign(&policy, value)
		if value > 0 {
			enabled++
		}
	}

	if raw, ok := annotations[KeepLast]; ok {
		value, err := parseQuota(raw)
		if err != nil {
			problems = append(problems, fmt.Sprintf("%s=%q: %v", KeepLast, raw, err))
		} else {
			policy.KeepLast = value
		}
		// A floor alone is not a retention policy: enforce nothing until some quota is enabled.
	}

	// A key under our prefix that is not a known level is a typo, not another tool's annotation.
	var unknown []string
	for key := range annotations {
		if strings.HasPrefix(key, AnnotationPrefix) && !isKnownAnnotation(key) {
			unknown = append(unknown, key)
		}
	}
	sort.Strings(unknown)
	if len(unknown) > 0 {
		problems = append(problems, fmt.Sprintf("unknown annotation(s) %s; valid keys are %s",
			strings.Join(unknown, ", "), strings.Join(knownAnnotationKeys(), ", ")))
	}

	if len(problems) > 0 {
		return RetentionPolicy{}, fmt.Errorf("invalid retention policy: %s", strings.Join(problems, "; "))
	}
	if enabled == 0 {
		return RetentionPolicy{}, errNoRetentionPolicy
	}
	return policy, nil
}

// quotaPattern is the one accepted value shape: a plain non-negative decimal integer.
var quotaPattern = regexp.MustCompile(`^[0-9]+$`)

// parseQuota accepts only a plain non-negative decimal integer. Floats (1.153e-6, 7.5), signs,
// hex, digit separators and empty strings are rejected rather than coerced, because each is
// ambiguous about how much the operator actually wanted to keep.
func parseQuota(raw string) (int, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, stderrors.New("value is empty")
	}
	if !quotaPattern.MatchString(trimmed) {
		return 0, stderrors.New("value must be a non-negative decimal integer")
	}
	value, err := strconv.Atoi(trimmed)
	if err != nil {
		return 0, fmt.Errorf("value out of range: %w", err)
	}
	if value > maxKeep {
		return 0, fmt.Errorf("value must be at most %d", maxKeep)
	}
	return value, nil
}

// parseEnforce reports whether this schedule opted into real deletions. Only the exact string
// "true" enables writes; missing or "false" means dry run. Anything else is rejected rather than
// defaulted, because "ture" or "1" next to a deletion switch must not resolve silently.
func parseEnforce(annotations map[string]string) (bool, error) {
	raw, ok := annotations[Enforce]
	if !ok || raw == "false" {
		return false, nil
	}
	if raw == "true" {
		return true, nil
	}
	return false, fmt.Errorf("invalid %s=%q: must be exactly %q or %q", Enforce, raw, "true", "false")
}

func isKnownAnnotation(key string) bool {
	if key == Enforce || key == KeepLast {
		return true
	}
	for _, level := range policyLevels {
		if level.key == key {
			return true
		}
	}
	return false
}

// nearMissAnnotations finds keys under a different domain that end in one of our annotation names,
// such as retention.velero.ioo/keep-daily. Those are not ours to enforce, so they cannot be errors,
// but silently doing nothing while the operator believes retention is running wastes storage.
func nearMissAnnotations(annotations map[string]string) []string {
	var suspects []string
	for key := range annotations {
		if isKnownAnnotation(key) {
			continue
		}
		_, name, ok := strings.Cut(key, "/")
		if !ok {
			continue
		}
		for _, want := range knownAnnotationNames() {
			if want == name {
				suspects = append(suspects, fmt.Sprintf("%s (did you mean %s%s?)", key, AnnotationPrefix, name))
				break
			}
		}
	}
	sort.Strings(suspects)
	return suspects
}

// knownAnnotationNames lists the name part (after the "/") of every annotation we own.
func knownAnnotationNames() []string {
	names := make([]string, 0, len(policyLevels)+2)
	for _, level := range policyLevels {
		names = append(names, strings.TrimPrefix(level.key, AnnotationPrefix))
	}
	names = append(names, strings.TrimPrefix(KeepLast, AnnotationPrefix), strings.TrimPrefix(Enforce, AnnotationPrefix))
	return names
}

func knownAnnotationKeys() []string {
	keys := make([]string, 0, len(policyLevels)+2)
	for _, level := range policyLevels {
		keys = append(keys, level.key)
	}
	return append(keys, KeepLast, Enforce)
}

// backupTimestamp returns the moment the backup finished, which is what retention ages on.
//
// CreationTimestamp is unusable here: the API server assigns it when the Backup object is written
// to etcd, so backups imported by the backup-sync controller carry the import time and would rank
// as the freshest ones in the set. A missing CompletionTimestamp means the backup never reported
// finishing, so its age is unknown and it is left alone entirely.
func backupTimestamp(b *velerov1.Backup) (time.Time, bool) {
	if b.Status.CompletionTimestamp == nil || b.Status.CompletionTimestamp.IsZero() {
		return time.Time{}, false
	}
	return b.Status.CompletionTimestamp.Time, true
}

func getBackupsToKeep(backups []*velerov1.Backup, policy RetentionPolicy) map[types.NamespacedName]bool {
	toKeep := make(map[types.NamespacedName]bool)

	// Bucket quotas are only meaningful when walking newest-first, so sort here instead of
	// relying on callers to have done it.
	ordered := make([]*velerov1.Backup, 0, len(backups))
	timestamps := make(map[types.NamespacedName]time.Time, len(backups))
	for _, b := range backups {
		ts, ok := backupTimestamp(b)
		if !ok {
			continue
		}
		ordered = append(ordered, b)
		timestamps[types.NamespacedName{Namespace: b.Namespace, Name: b.Name}] = ts
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		ti := timestamps[types.NamespacedName{Namespace: ordered[i].Namespace, Name: ordered[i].Name}]
		tj := timestamps[types.NamespacedName{Namespace: ordered[j].Namespace, Name: ordered[j].Name}]
		if !ti.Equal(tj) {
			return ti.After(tj)
		}
		// Timestamps are second-precision, so backups synced in the same instant tie. Velero
		// encodes the trigger time in the name, making descending name the newest-first tiebreak.
		return ordered[i].Name > ordered[j].Name
	})

	// The keep-last floor runs first and unconditionally: the newest N dated backups are kept no
	// matter which buckets they occupy. Being purely additive it can only protect backups GFS
	// would have deleted, never the reverse.
	if n := policy.KeepLast; n > 0 {
		for i, b := range ordered {
			if i >= n {
				break
			}
			toKeep[types.NamespacedName{Namespace: b.Namespace, Name: b.Name}] = true
		}
	}

	hourlyKept := make(map[string]bool)
	dailyKept := make(map[string]bool)
	weeklyKept := make(map[string]bool)
	monthlyKept := make(map[string]bool)
	yearlyKept := make(map[string]bool)

	for _, b := range ordered {
		ts := timestamps[types.NamespacedName{Namespace: b.Namespace, Name: b.Name}]
		key := types.NamespacedName{Namespace: b.Namespace, Name: b.Name}

		yearKey := ts.Format("2006")
		if policy.Yearly > 0 && len(yearlyKept) < policy.Yearly && !yearlyKept[yearKey] {
			toKeep[key] = true
			yearlyKept[yearKey] = true
		}

		monthKey := ts.Format("2006-01")
		if policy.Monthly > 0 && len(monthlyKept) < policy.Monthly && !monthlyKept[monthKey] {
			toKeep[key] = true
			monthlyKept[monthKey] = true
		}

		year, week := ts.ISOWeek()
		weekKey := fmt.Sprintf("%d-%d", year, week)
		if policy.Weekly > 0 && len(weeklyKept) < policy.Weekly && !weeklyKept[weekKey] {
			toKeep[key] = true
			weeklyKept[weekKey] = true
		}

		dayKey := ts.Format("2006-01-02")
		if policy.Daily > 0 && len(dailyKept) < policy.Daily && !dailyKept[dayKey] {
			toKeep[key] = true
			dailyKept[dayKey] = true
		}

		hourKey := ts.Format("2006-01-02-15")
		if policy.Hourly > 0 && len(hourlyKept) < policy.Hourly && !hourlyKept[hourKey] {
			toKeep[key] = true
			hourlyKept[hourKey] = true
		}
	}

	return toKeep
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var dryRun bool
	var probeAddr string
	var watchNamespace string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&watchNamespace, "namespace", "",
		"Namespace to watch; defaults to the POD_NAMESPACE env (injected via the downward API). "+
			"The controller is single-namespace by design: Velero only reconciles its own.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	// Defaults to true so an accidental deploy reports decisions instead of deleting backups.
	flag.BoolVar(&dryRun, "dry-run", true,
		"Log the DeleteBackupRequests the controller would create without creating them. "+
			"Set -dry-run=false to allow deletion, which still requires the annotation "+
			Enforce+": true on each Schedule.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	ns, err := resolveNamespace(watchNamespace, os.Getenv("POD_NAMESPACE"))
	if err != nil {
		setupLog.Error(err, "unable to determine namespace")
		os.Exit(1)
	}
	setupLog.Info("watching a single namespace", "namespace", ns)

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		// Scoped like Velero itself (its server caches exactly its own namespace). The
		// Role grants access to this namespace only, so a cluster-wide informer would
		// fail with Forbidden at startup anyway.
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{ns: {}},
		},
		Metrics: server.Options{
			BindAddress: metricsAddr,
		},
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        "a92b15f5.velero.io",
		LeaderElectionNamespace: ns,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err = (&ScheduleReconciler{
		Client:   mgr.GetClient(),
		Scheme:   mgr.GetScheme(),
		DryRun:   dryRun,
		Recorder: mgr.GetEventRecorderFor("retention-controller"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Schedule")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	if dryRun {
		setupLog.Info("DRY RUN: no DeleteBackupRequest will be created; re-run with -dry-run=false to allow enforcement")
	} else {
		setupLog.Error(nil, "WRITE MODE enabled globally: schedules still need "+Enforce+
			": true before any DeleteBackupRequest is created")
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func (r *ScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1.Schedule{}).
		Complete(r)
}

// resolveNamespace implements the precedence for the watched namespace:
// explicit -namespace flag, then POD_NAMESPACE (downward API), error if neither.
// An empty result is rejected here rather than at manager start, where a missing
// namespace would silently mean "watch everything" (which the Role forbids).
func resolveNamespace(flagValue, podNamespace string) (string, error) {
	if ns := strings.TrimSpace(flagValue); ns != "" {
		return ns, nil
	}
	if ns := strings.TrimSpace(podNamespace); ns != "" {
		return ns, nil
	}
	return "", stderrors.New("no namespace: set -namespace or the POD_NAMESPACE env")
}
