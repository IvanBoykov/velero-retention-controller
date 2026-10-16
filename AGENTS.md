# velero-retention-controller

## Build / test

```bash
go build ./... && go vet ./... && go test -race -count=1 ./...
```

No cluster or envtest is set up in this repo: verify reconcile behaviour with the fake client
in `main_test.go` (`sigs.k8s.io/controller-runtime/pkg/client/fake`) rather than trying to run
the binary. Deployment manifests live under `deploy/` (kustomize base + `write-mode` overlay
example); validate changes with `kustomize build deploy/base` and
`kustomize build deploy/overlays/write-mode`.

## Velero deletion semantics (important, non-obvious)

Deleting a `Backup` CR with `client.Delete()` does **not** remove the backup data from object
storage or its volume snapshots, and `Backup` objects carry no finalizer, so the CR disappears
immediately and nothing ever cleans the data. The correct path — what `velero backup delete`
does — is to create a `DeleteBackupRequest` in the backup's namespace with
`spec.backupName` plus the `velero.io/backup-name` / `velero.io/backup-uid` labels. Velero's
backup-deletion controller then purges the store and removes the CR.

Consequence for this controller: a `Completed` backup with an outstanding request (or a
`DeletionTimestamp`) is still visible to `List`. Such backups must be excluded from the
retention budget and must not receive a duplicate request, or every reconcile both starves the
keep-set and piles up requests.

## Annotation contract (Schedule)

Keys, all under `retention.velero.io/`: `keep-hourly`, `keep-daily`, `keep-weekly`,
`keep-monthly`, `keep-yearly`, plus `keep-last` (protection floor) and `enforce` (write opt-in,
see below). Quota values: plain non-negative decimal integers (`^[0-9]+$`), 0–10000, surrounding
whitespace tolerated. `0` explicitly disables one level; a schedule with no level enabled (or no
annotations) has retention skipped entirely — "all zero" is never interpreted as "keep nothing,
delete everything".

`keep-last` is NOT a quota: it protects the N newest completed backups unconditionally, unioned
into the GFS keep-set regardless of age or bucket. It is parsed with the same strict value rules
(`keep-last: 2.5` is an error, not a coercion) but does not count toward "some level enabled" —
`keep-last: 5` alone is not a policy. Being purely additive it can only ever shrink the deletion
set, so it fails safe. A real-world schedule may carry `keep-last: "0"` to state "no extra floor";
that is a valid no-op, not a configuration error.

**Parsing fails closed.** Any annotation under our prefix that is not one of the seven known keys,
or any quota value that is not exactly a decimal integer, is a configuration error: the controller
logs the full problem list at Error level, deletes nothing for that schedule and requeues hourly.
Rationale: loose parsing (`Atoi` + skip) made a typo like `keep-dail: 7` *silently disable the
daily quota*, i.e. every parse mistake widened the deletion set. Refusing to act is recoverable;
deleting is not.

- Rejected values: `""`, `"  "`, `-3`, `abc`, `7x`, `1.153e-6`, `7.5`, `7.0`, `+7`, `0x07`, `1_000`,
  `true`, `"7"` (quoted), `7-10`, non-ASCII digits, > int range, > 10000.
- Keys under a *different* domain that end in a level name (`velero.io/keep-weekly`,
  `retention.velero.ioo/keep-daily`) are not ours to enforce → logged as "did you mean" hints,
  policy still applies. (`velero.io/keep-*` looks plausible historically but never existed; the
  prefix is this project's invention.)

## Paused schedules

Retention runs normally while `spec.paused: true`. GFS converges rather than erodes: the keep-set
is the latest backup per bucket, so with no new backups arriving the older-bucket survivors stay
and nothing further gets deleted. Paused must NOT skip retention — an old backlog of over-quota
backups is just as deletable while paused.

## Per-schedule write opt-in: retention.velero.io/enforce

Deletions require BOTH switches; either one off keeps the schedule in dry run (log only):

1. controller flag `-dry-run=false` (global kill switch, default true)
2. annotation `retention.velero.io/enforce: "true"` on the Schedule

Value must be exactly `true` or `false`; a typo (`ture`, `True`, `1`) is a configuration error: the
schedule refuses to delete and logs why. Missing annotation = dry run. `enforce` without quotas is
a no-op — it never enables retention by itself. Effect on cluster rollout: deploying the controller
with `-dry-run=false` changes nothing until each schedule is individually opted in.

## Status reporting design

`.status.conditions` cannot be written on a Velero `Schedule` (verified against the CRD schema and
Velero source): the status schema is exactly {phase, lastBackup, lastSkipped, validationErrors}
with pruning ON, so unknown fields are silently dropped; there is no `status` subresource, so any
status write is a full-object update that bumps generation and re-triggers our own watch; and the
status belongs to Velero's schedule controller, which re-patches it with `client.MergeFrom`.

Reporting therefore uses Events on the Schedule object. Spam rules:
- `InvalidRetentionPolicy` (Warning): every reconcile while the policy is broken — errors repeat
  by intent, dry-run is normal and never warns. The server-side aggregator dedups repeats.
- `InvalidEnforcement` (Warning): same, for a malformed enforce value.
- `RetentionPolicyApplied` (Normal): once per annotation-change, message = parsed key=value list.
  Deduped by in-memory fingerprint (`sync.Map`, Delete on parse-fail/NotFound so a repaired
  policy always re-announces). Lost on restart → one harmless re-announce per restart.
- `BackupsQueuedForDeletion` (Normal): write-mode only, one aggregate per reconcile
  (count + oldest name), never per backup — first enforcement can remove hundreds.

RBAC: events need create;patch (the aggregator patches count on repeats). Events are best-effort:
a rejected event logs and the reconcile proceeds. An own status CR remains the upgrade path if
conditions are ever wanted (candidates: `retention.velero.io/PolicyValid`,
`retention.velero.io/Enforced`, write-on-flip semantics).

## Verified against Velero source/docs (v1.18.4)

- `DeleteBackupRequest` construction matches `pkg/backup/delete_helpers.go` exactly
  (GenerateName `name-`, `backup-name`+`backup-uid` labels, `spec.backupName`).
- DBR dedup is sound because the deletion controller **expires DBRs after 24h**
  (`deleteBackupRequestMaxAge`) and drops requests whose `backup-uid` label no longer matches the
  Backup's UID; stale DBRs can't block a fresh delete request.
- `velero.io/schedule-name` label is set on backups at creation (`schedule_controller.go:221`) —
  the retention List filter is the same selector Velero's own schedule controller uses.
- Docs (backup-reference, "Deleting Backups"): `kubectl delete backup` removes the CR only and
  leaks storage data — confirms DeleteBackupRequest is the only correct path.
- Velero does not deduplicate DeleteBackupRequests — creating two for the same backup is allowed
  and both get processed. Dedup (skip if a request or a `DeletionTimestamp` already exists) is
  entirely our responsibility.
- Deletion controller cascades to Restores (`backup_deletion_controller.go:394`) and sets Backup
  phase `Deleting`; it has no phase gate, so protection of in-flight work is ours alone.

## Age source: completion time only — never TTL, never CreationTimestamp

Schedules are expected to run with a deliberately huge `spec.ttl` (e.g. `87600h` = 10 years) so
Velero's own TTL GC never fires; retention is entirely GFS in this controller. Rationale: with
short TTL, a week of failing backups silently ages out and leaves *nothing*, whereas GFS always
keeps the older buckets.

Therefore, non-negotiable:

- `Status.Expiration` / `Spec.TTL` must never enter the decision. A 100-year Expiration is not
  protection and a past Expiration is not a delete signal.
- `CreationTimestamp` must never enter the decision. A backup whose only timestamp is
  `CreationTimestamp` is treated as **having no time at all** and is never deleted.
- `Status.CompletionTimestamp` is the sole age source.
- Backups missing `completionTimestamp` are in practice the `FailedValidation`/`Failed` ones, i.e.
  exactly the set the phase filter already excludes.

## Dry run

`-dry-run` defaults to **true**: the controller logs the `DeleteBackupRequest`s it would create and
writes nothing. Pass `-dry-run=false` to enforce. `ScheduleReconciler.DryRun`'s zero value is write
mode, so any new construction site must set it deliberately (the flag, not the struct, carries the
safe default).

## Verifying against a real cluster

- Any kubeconfig placed in the repo for ad-hoc verification is gitignored — never commit or
  print it. With a read-only credential, dry-run is the only possible mode.
- If the verification identity lacks `create leases.coordination.k8s.io`, run with
  `-leader-elect=false`.
- Before ever enabling write mode, cross-check the controller's decisions against an independent
  reimplementation of GFS over the same `kubectl get -o json` dump and compare the deletion sets.

Expected side effect to confirm with the operator: deleting a backup cascades to the `Restore`
objects referencing it, so historical restore records disappear with their source backups.

## Retention algorithm gotchas

- `getBackupsToKeep` must walk backups newest-first; it sorts internally rather than trusting
  the caller's ordering.
- Bucket quotas are per level, so the *union* of kept backups legitimately spans more days than
  `keep-daily`. A backup kept for `keep-yearly` also occupies a daily/hourly bucket. Tests must
  not assert that the union respects a finer level's quota.
- Age is taken from `Status.CompletionTimestamp`, never `CreationTimestamp`. The API server stamps
  `CreationTimestamp` when the object lands in etcd, so backups imported by the backup-sync
  controller carry their *import* time and would outrank genuinely newer backups.
- A `Completed` backup with no `CompletionTimestamp` is skipped entirely rather than deleted: it
  cannot be placed in any bucket, so treating it as "not kept" would delete the object we know
  least about. Same for a future-dated timestamp, which would otherwise win every bucket.
- Timestamps are second-precision (`metav1.Time` serialises as RFC3339), so synced-in backups
  tie. The sort tiebreaks on descending name because Velero embeds the trigger time in the name
  (`<schedule>-20060102150405`); without it the survivor depends on API list order.
- The keep-set is keyed by `NamespacedName`, not `UID`: hand-created or restored CRs can have an
  empty `UID`, and those would otherwise collide into one map entry and get mass-deleted.

## Deleting backups is destructive in ways the CR hides

- `Status.CompletionTimestamp`/`CreationTimestamp` are the only age signals; both can be absent or
  misleading, so an unresolvable age must mean "do nothing".
- Deleting a backup **cascades to every `Restore` that references it**: Velero's
  backup-deletion controller lists restores matching `spec.backupName` and deletes them. A
  retention pass can therefore silently abort a restore in flight. `backupsWithActiveRestores`
  protects backups referenced by a restore in any non-terminal phase, and treats an empty or
  unrecognised phase as active so a future Velero phase cannot unlock a deletion by default.
- RBAC: needs `restores` get/list/watch (read-only) in addition to
  `deletebackuprequests` create.

## Tests

`main_test.go` is mutation-checked: reverting any of the six protections (restore guard,
completion-timestamp ageing, missing-timestamp skip, name tiebreak, deletion-timestamp skip,
`NamespacedName` keying) makes at least one test fail. Prefer adding a failing test before
relaxing any of those.
