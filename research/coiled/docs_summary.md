## COILED RESEARCH SUMMARY

Verified against: `coiled` 1.135.0 (installed) + docs.coiled.io + the `coiled/feedback`
issue tracker (research/coiled/topic_summary.json).

### What is Coiled

Coiled is a managed cloud-compute platform that provisions VMs in *your own* cloud
account (AWS / GCP / Azure) and runs Dask clusters, serverless functions, or batch jobs
on them. The library is a thin client: you describe the cluster you want, Coiled boots
the VMs, installs your software, and hands back a normal Dask `distributed` cluster.

**Mental model that prevents most confusion:**
- **One Coiled "worker" = one whole VM** running a single Dask worker process. This is
  unlike `LocalCluster`, where workers are process/thread slices of one machine. The
  `LocalCluster` knobs `threads_per_worker`, `processes`, `n_workers` (as slices) do not
  map onto `coiled.Cluster`. (issue #238 "Making a worker use processes rather than cores")
- **Software is replicated from your local environment by default** ("package sync"). Coiled
  scans the active local env and recreates it on the cluster. Therefore a broken local env
  (missing dask, broken SSL, weird package versions) becomes a cluster-side failure.

**Current version observed**: 1.135.0

---

### Three execution models

1. **`coiled.Cluster`** — full Dask cluster; returns a `distributed.Client` via
   `cluster.get_client()`. Use for Dask collections, `client.submit/map`, xarray/dask.
2. **`coiled.function`** — decorator that turns a plain Python function into one that runs
   on a Coiled cluster; `.map()` fans out across VMs, `.submit()` runs one call. Cluster is
   created/scaled implicitly.
3. **`coiled batch`** (CLI) — SLURM-style array jobs; no Dask required. `coiled batch run`.

---

### Python API Map (verified signatures)

#### `coiled.Cluster(...)` — key kwargs (all verified present in 1.135.0)
- `n_workers` — int, or `[min, max]` list for adaptive scaling
- `region` — colocate compute with data ("us-east-2", etc.)
- `spot_policy` — `"spot"`, `"spot_with_fallback"`, `"on-demand"`
- `worker_memory` — e.g. `"16GiB"`; alternative to `worker_vm_types`
- `worker_vm_types` — list, e.g. `["m6i.large"]`
- `worker_options` — dict forwarded to `distributed.Worker` (e.g. `{"nthreads": 1}`).
  Only real `distributed.Worker` kwargs are accepted — `nthreads` yes; `nprocs`/`nworkers`
  no (those are `dask worker` CLI flags).
- `idle_timeout` — auto-shutdown, default "20 minutes"
- `mount_bucket` — mount cloud storage as a filesystem
- `arm` — bool; Graviton instances, and the only way to get true single-vCPU VMs
- `allow_ssh` — bool; opens port 22 for `coiled cluster ssh`
- `package_sync_ignore` — list of local packages to exclude from replication
- `software` — name of a prebuilt Coiled software environment (skip package sync)
- `container` — registry image ref (skip package sync)
- `name` — reconnect to an existing cluster by name

**NOT accepted** (raise `TypeError: unexpected keyword argument`): `threads_per_worker`,
`processes`. These are `LocalCluster` knobs and do not exist on `coiled.Cluster`.

`use_magic=True` is the **old** name for package sync; it is the default behavior now and
is no longer a parameter — do not pass it.

#### Cluster methods
- `cluster.get_client()` → `distributed.Client` (preferred over `Client(cluster)`)
- `cluster.get_logs()` → logs **only from live instances** (cannot fetch from dead VMs)

#### `coiled.function(...)` — decorator, key kwargs (verified)
- `region`, `memory`, `spot_policy`, plus most `Cluster` kwargs
- decorated object exposes `.map(iterable)` and `.submit(*args)`

---

### CLI Map (verified against `coiled --help` in 1.135.0)

Top-level commands: `batch`, `cluster`, `login`, `notebook`, `logs`, `env`,
`package-sync`, `diagnostics`, `setup`, `run`, …

- `coiled login` — configure account credentials
- `coiled batch run --vm-type … --region … --memory … <cmd>` — submit array/batch job
- `coiled cluster ssh [<name>] [--worker <name|any>] [--dask]` — SSH to scheduler host;
  `--worker` targets a worker; `--dask` attaches *inside* the Dask container vs the host.
  Requires `allow_ssh=True` at cluster creation.
- `coiled cluster logs --cluster <id> [--filter <str>] [--since <str>]` — Dask logs from the
  centralized store (works even when the VM is dead).
- `coiled cluster better-logs-cli [<cluster>]` — full system + cloud-init logs. **NOTE:** the
  command is `better-logs-cli`, **not** `better-logs` (which is not a valid subcommand).
  Related: `logs-via-aws-cli`, `azure-logs`.
- `coiled notebook start --sync` — Jupyter session with file syncing (needs ssh/ssh-add).

---

### Software / Package Sync (default)

- Coiled scans the *local* env and recreates it on cluster VMs. The local env must contain
  `dask` + `distributed` or workers fail (sometimes silently).
- Failures show up in cloud-init system logs, not always in Dask logs — use
  `coiled cluster better-logs-cli`.
- Alternatives to sync: `software="prebuilt-env"` or `container="registry/image:tag"`.

---

### Key issues mined from `coiled/feedback` (with topic + score)

| # | Title (topic) | Lesson for the skill |
|---|---------------|----------------------|
| 238 | Making a worker use processes rather than cores | one worker = one VM; use more/smaller VMs or `arm=True`, not ProcessPoolExecutor |
| 266 | "cannot find X~=… on pypi.org" | usually transient server-side, pip-only; retry |
| 177, 223 | package silently omitted / wrong version | unparseable local version dropped; cluster never phones home; check `pip list` + system logs |
| 263 | `coiled login` SSLCertVerificationError | broken local SSL trust store; `SSL_CERT_FILE=…` or rebuild env with conda |
| 106 | `coiled login` TypeError | ancient coiled vs new click; upgrade client |
| 267 | notebook `--sync` Permission denied (publickey) | needs ssh/ssh-add + key; `allow_ssh=True` |
| 296, 291 | many clients on one cluster (workshops) | give each user their own cluster |
| 185 | worker memory_limit lower than VM RAM | limit comes from cgroup, ~5% overhead expected |
| 171, 172 | cluster stalls / dies | dashboard first, then centralized logs, then SSH |

---

### Known Limitations

- `cluster.get_logs()` / `client.get_worker_logs()` cannot retrieve logs from **dead**
  instances — use `coiled cluster logs` / the web app.
- `worker_options` only accepts real `distributed.Worker` kwargs.
- Package sync faithfully replicates the local env, including its problems.

---

### Relationship to dask_distributed

A Coiled cluster *is* a `distributed` cluster. Anything about scheduler/Client behavior —
killed workers, memory thresholds/spilling, connection timeouts, serialization/version
mismatches, the nested-`compute()` deadlock — lives in the **dask_distributed** skill. The
coiled skill covers only the provisioning/software/CLI layer and cross-links to it.

---

**End of Research Summary**
