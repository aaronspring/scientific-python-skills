---
name: coiled
description: Setting up and debugging Coiled clusters for Dask/serverless cloud compute on AWS/GCP/Azure. Use when creating coiled.Cluster, running coiled.function or coiled batch jobs, diagnosing package sync errors, login/SSL failures, worker memory issues, or clusters that stall/die.
user-invocable: false
---

# Coiled

> A Coiled "worker" is a whole VM running ONE Dask worker process — not a thread/process slice like `LocalCluster`. Most setup confusion comes from this. Software is replicated from your *local* environment by default (package sync), so client-side env problems become cluster-side failures.

> Coiled provisions the VMs; the cluster you get is a normal Dask `distributed` cluster. For the underlying scheduler/Client behavior — killed workers, memory thresholds/spilling, connection timeouts, serialization/version mismatches, and the nested-`compute()` deadlock — see the **dask_distributed** skill.

## CRITICAL: One worker = one VM (sizing & parallelism)

```python
# WRONG - LocalCluster knobs don't exist on coiled.Cluster
cluster = coiled.Cluster(processes=True, threads_per_worker=1, n_workers=8)
# TypeError: __init__() got an unexpected keyword argument 'threads_per_worker'

# RIGHT - each worker is its own VM. For more parallelism use more / smaller VMs.
cluster = coiled.Cluster(n_workers=8, worker_vm_types=["m6i.large"])  # 8 separate VMs
# thread count per worker goes through worker_options (forwarded to distributed.Worker):
cluster = coiled.Cluster(n_workers=8, worker_options={"nthreads": 1})
```

For **pure-Python / GIL-bound** code: prefer many small VMs over one big VM. `arm=True` unlocks
single-vCPU instances and is usually a better lever than building a `ProcessPoolExecutor` on workers
(#238). Don't swap the worker threadpool for `concurrent.futures.ProcessPoolExecutor` — it pickles
poorly; if you truly need it, `loky` is the drop-in replacement.

## Setup (Copy-Paste Ready)

```python
import coiled

cluster = coiled.Cluster(
    n_workers=15,                       # or [min, max] for adaptive, e.g. [2, 50]
    region="us-east-2",                 # put compute next to your data
    spot_policy="spot_with_fallback",   # cheap, falls back to on-demand if spot unavailable
    worker_memory="16GiB",              # or worker_vm_types=["m6i.xlarge"]
    idle_timeout="20 minutes",          # auto-shutdown; default is 20 min
    mount_bucket="s3://my-bucket",      # mount cloud storage as a filesystem
)
client = cluster.get_client()           # preferred over Client(cluster)
print(client.dashboard_link)            # live Dask dashboard URL
```

```python
# Serverless function - parallelize a plain function across many VMs
@coiled.function(region="us-east-2", memory="8GiB", spot_policy="spot_with_fallback")
def process(filename):
    ...
results = list(process.map(filenames))  # .map() fans out across the cluster
```

```bash
# Batch jobs (SLURM-style array) from the CLI - no Dask needed
coiled batch run --vm-type m6i.large --region us-east-2 --memory 64GB python myscript.py
```

## Software / Package Sync (default behavior)

By default Coiled scans your **local** env and recreates it on the cluster. Your local env therefore
matters even when running batch/serverless jobs.

```python
# Local env MUST contain dask + distributed or workers fail (sometimes silently). Verify:
#   uv run python -c "import dask, distributed"

# Exclude noisy/unneeded local packages from being replicated:
cluster = coiled.Cluster(package_sync_ignore=["torch", "tensorflow"])
# Or pin to an explicit prebuilt env / container instead of sync:
cluster = coiled.Cluster(software="my-prebuilt-env")
cluster = coiled.Cluster(container="my-registry/my-image:latest")
```

`use_magic=True` is the **old** name for package sync — it is the default now; don't pass it.

## Gotchas & Common Mistakes

### "cannot find X~=1.3.0 on pypi.org" warnings (#266)
Usually a **transient Coiled server-side issue**, and only affects pip (not conda) packages. Retry;
if it persists for a real package, it's genuinely missing from the index for linux-64.

### Packages silently "omitted" / wrong version installed (#177, #223)
A locally-installed package with an unparseable/odd version (e.g. `nwis==0.0.*`) gets dropped, and
the *cluster never phones home* — failure shows up only in cloud-init system logs. Check what's
actually in your env with `pip list`, remove/fix the offending package, and inspect full logs
(`coiled cluster better-logs`), not just Dask logs.

### `coiled login` fails with SSLCertVerificationError (#263)
Not Coiled-specific — your local Python's SSL trust store is broken (`aiohttp` HTTPS fails too).
```bash
export SSL_CERT_FILE=/path/to/ca-certificates.crt   # often fixes it
```
Or recreate the env with conda/mamba/miniconda. Confirm with:
```python
import ssl; print(ssl.get_default_verify_paths().openssl_cafile)
```

### `coiled login` TypeError on old versions (#106)
Ancient `coiled` against newer `click`. Fix by upgrading: the client must be current.

### `coiled notebook start --sync` / SSH "Permission denied (publickey)" (#267)
The SSH/Mutagen sync wrapper needs `ssh` and `ssh-add` runnable from your shell, with a key loaded
(`ssh-add`). Create the cluster with `allow_ssh=True` to open port 22.

### Multiple clients on one cluster during demos/workshops (#296, #291)
Sharing one token/cluster means many clients connect to the same scheduler (logs showed 8 clients),
causing instability. Give each user their **own** cluster. A client-side network blip disconnects you
but doesn't kill the cluster — reconnect with `coiled.Cluster(name="...")`.

## Worker memory

Worker `memory_limit` is set from the container's **cgroup** limit (not raw VM RAM), so ~5% system
overhead is expected and accounted for (#185). Watch worker logs for the standard distributed signals:

```
Worker is at 90% memory usage. Pausing worker.   # -> bump worker_memory or use more workers
Event loop was unresponsive ... 3.83s            # GIL-holding code or large data moves
```
If a worker hits the terminate threshold (~95%) it's killed and tasks retry. Raise `worker_memory`
or split work into smaller chunks rather than fighting the limit.

## Debugging a cluster that stalls or dies (#171, #172)

1. **Dashboard first**: `client.dashboard_link` — look for memory pressure, unresponsive event loop, task pileup.
2. **Centralized logs** (reliable even if a VM is dead — `get_logs()` cannot fetch from dead instances):
   ```bash
   coiled cluster logs --cluster <id> --filter "starting"   # Dask logs, supports --since
   coiled cluster better-logs                                # full system + cloud-init logs
   ```
3. **Logging from a script** (workflow/orchestration runs):
   ```python
   import logging
   logging.basicConfig(level=logging.INFO)
   logging.getLogger("coiled").setLevel(logging.INFO)
   ```
4. **SSH in** to inspect a live host/container:
   ```bash
   coiled cluster ssh                 # most recent cluster's scheduler (host machine)
   coiled cluster ssh <name> --worker any
   coiled cluster ssh --dask          # attach inside the Dask container, not the host
   ```
   Requires `allow_ssh=True` at creation. Scheduler-dying-after-a-while often means a memory leak or
   a GIL-soft-lock from log spam / huge graphs — check scheduler CPU and worker memory in the dashboard.

## Performance & cost Tips

- **FAST: colocate compute and data** — set `region` to match your bucket; cross-region transfer is slow and costly.
- **FAST: `spot_policy="spot_with_fallback"`** — large savings, auto on-demand fallback when spot is scarce.
- **FAST: `arm=True`** — cheaper Graviton instances, and the only way to get true single-vCPU VMs.
- **Always set `idle_timeout`** — clusters auto-shut-down (default 20 min) so forgotten clusters stop billing.
- **Adaptive scaling**: `n_workers=[min, max]` scales workers to the workload.

## Known Limitations

- `cluster.get_logs()` / `client.get_worker_logs()` **cannot retrieve logs from dead instances** — use `coiled cluster logs` / the web app instead.
- `worker_options` only accepts real `distributed.Worker` kwargs (`nthreads` yes; `nprocs`/`nworkers` no — those are `dask worker` CLI flags).
- Package sync replicates the local env: an unhealthy local env (broken SSL, weird package versions, missing dask) breaks the cluster.

Docs: https://docs.coiled.io/user_guide/index.html · [API](https://docs.coiled.io/user_guide/api.html) · [Logging](https://docs.coiled.io/user_guide/logging.html) · [SSH](https://docs.coiled.io/user_guide/ssh.html)

Underlying Dask cluster behavior (memory, killed workers, timeouts): see the **dask_distributed** skill.
