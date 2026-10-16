# velero-retention-controller

Grandfather-revision-style (GFS) retention for [Velero](https://velero.io) schedules:
keeps the newest backup per hour/day/week/month/year instead of Velero's single
TTL window, and removes the rest through Velero's own `DeleteBackupRequest` path
(so object storage and snapshots are cleaned too).

## Install

Both commands create the same five objects (ServiceAccount, ClusterRole,
ClusterRoleBinding, metrics Service, Deployment); they differ only in the
`-dry-run` flag, and neither deletes anything until a `Schedule` opts in.

**Default install — dry run (recommended first step).** The controller reports, in logs
and Events, which backups each schedule *would* retire. Nothing is ever deleted:

```bash
kubectl apply -k "https://github.com/IvanBoykov/velero-retention-controller//deploy/base?ref=master"
```

**Full install — delete mode.** Same controller, with the global kill switch turned
off. Deletions still happen only for schedules explicitly annotated
`retention.velero.io/enforce: "true"` (see [Configure](#configure)):

```bash
kubectl apply -k "https://github.com/IvanBoykov/velero-retention-controller//deploy/overlays/write-mode?ref=master"
```

Pin a release instead of `master` for a reproducible install:

```bash
kubectl apply -k "https://github.com/IvanBoykov/velero-retention-controller//deploy/base?ref=v0.1.0"
```

The `?ref` pins the manifests; the *image* is pinned separately — starting with the
first release, CI rewrites the base to reference that explicit version tag (never
`latest`), so `apply -k` without an overlay always pulls a published version.
In the registry, `:latest` is only ever moved by a release tag; the tip of master is
published as `:master` (and `:sha-<commit>`), which nothing in `deploy/` references.

Prerequisites: Velero CRDs (`schedules`, `backups`, `deletebackuprequests`) and the target
namespace (default: `velero`) must already exist — usually both are true wherever Velero
runs. The namespace itself is intentionally **not** part of the manifests, so the
controller's installation never owns a namespace that may contain your Velero install.

Override the namespace, image, or flags in your own
[kustomize overlay](deploy/overlays/write-mode/kustomization.yaml) instead of
forking the base.

### What the defaults do

| Setting | Default | Effect |
| --- | --- | --- |
| `-dry-run` | `true` | The controller only reports what it *would* delete. |
| `retention.velero.io/enforce` | absent | Even with `-dry-run=false`, a schedule is only enforced if it opts in. |
| Retention annotations | absent | Schedules without annotations are never touched. |
| RBAC | read + `create deletebackuprequests` | The controller can never delete `Backup` objects directly, and has no write access to schedules/backups. |
| Namespace object | not included | `kubectl delete -k` cannot cascade-destroy a co-located Velero install. |

Applying the base with these defaults deletes **nothing**: it starts an observer that
logs and emits Events (`InvalidRetentionPolicy`, `RetentionPolicyApplied`) on your
schedules without changing them. Deleting backups requires three
deliberate steps: set `-dry-run=false` (deployment), annotate a schedule with a quota,
and annotate it with `retention.velero.io/enforce: "true"`.

### Uninstall

```bash
kubectl delete -k "https://github.com/IvanBoykov/velero-retention-controller//deploy/base?ref=master"
```

This removes exactly the five objects the install created — the controller's
ServiceAccount, ClusterRole, ClusterRoleBinding, metrics Service and Deployment. It
does not delete the namespace, any Velero resource (backups, schedules, restores), or
anything else in the cluster.

## Configure

Add annotations to a `Schedule` (`retention.velero.io/` is the only prefix this
controller reads; unknown keys under it are treated as typos and disable retention for
that schedule rather than being ignored):

| Annotation | Meaning |
| --- | --- |
| `keep-hourly` / `keep-daily` / `keep-weekly` / `keep-monthly` / `keep-yearly` | How many most-recent backups to keep per bucket (plain integers, 0 disables a level). |
| `keep-last` | Protection floor: always keep the N newest completed backups, whatever buckets they fill. |
| `enforce` | `"true"` opts this schedule into real deletions (requires `-dry-run=false` on the deployment). |

Example: keep 24 hourly, 30 daily, 12 weekly, 12 monthly, 5 yearly, and never fewer
than the 3 newest backups:

```yaml
metadata:
  annotations:
    retention.velero.io/keep-hourly: "24"
    retention.velero.io/keep-daily: "30"
    retention.velero.io/keep-weekly: "12"
    retention.velero.io/keep-monthly: "12"
    retention.velero.io/keep-yearly: "5"
    retention.velero.io/keep-last: "3"
    retention.velero.io/enforce: "true"   # actually delete; omit to stay in dry-run
```

Safety properties, by design:

- A malformed or nonsensical policy (typos, negative or fractional values, all-zero
  quotas) disables deletion for that schedule entirely and emits a warning Event —
  it never widens the deletion set.
- Backups with no reliable completion timestamp, non-`Completed` phase, a future
  timestamp, or an in-flight restore are never deleted.
- Retention for one schedule can only ever consider that schedule's own backups.

## Development

```bash
go build ./... && go vet ./... && go test ./...
kustomize build deploy/base            # or: deploy/overlays/write-mode
```

Images are published by GitHub Actions to
`ghcr.io/ivanboykov/velero-retention-controller` (`linux/amd64`, `linux/arm64`).
