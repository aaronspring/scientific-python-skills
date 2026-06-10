---
name: dask_distributed
description: Setting up and debugging Dask distributed clusters (the `distributed` scheduler/Client). Use when creating a Client/LocalCluster, running client.submit/map or dask collections on a cluster, or diagnosing killed workers, memory blow-ups, spilling, connection timeouts, serialization failures, dashboard access, or compute() called inside a task.
user-invocable: false
---

# Dask Distributed

> Most "my cluster is broken" problems are one of three things: (1) the client/scheduler/worker environments don't match, (2) workers run out of memory because of *unmanaged* memory or oversized partitions, or (3) a task internally calls `.compute()`/creates a new `Client` and deadlocks. For cloud clusters, see the **coiled** skill — this skill is the underlying `distributed` layer.

## CRITICAL: Never call `.compute()` or create a `Client` inside a task

Calling `.compute()`, `client.gather()`, or `Client()` from *within* a task starves
the worker's thread pool and deadlocks once you have >1 worker (#3791, #2336, #4612).

```python
# WRONG - blocks a worker thread waiting on the scheduler -> deadlock at scale
def process(id):
    ddf = get_client().get_dataset("foo")
    return ddf[ddf.id == id].compute()      # nested compute, holds the thread
futures = client.map(process, ids)

# RIGHT - use worker_client() + secede() so the long wait doesn't occupy a slot
from distributed import worker_client, secede, rejoin
def process(id):
    secede()                                 # give my thread back to the pool while I wait
    with worker_client() as client:          # reuse the worker's own client, never Client()
        ddf = client.get_dataset("foo")
        result = ddf[ddf.id == id].compute()
    rejoin()                                  # reclaim a thread slot before returning
    return result
```

`client.publish_dataset` futures belong to the publishing client; a *second* `Client`
to the same scheduler gets `"Inputs contain futures created by another client"` (#2336).
Inside tasks always reuse `worker_client()`/`get_client()`, never construct a new `Client`.

## Setup (Copy-Paste Ready)

```python
from dask.distributed import Client, LocalCluster

# Local cluster: pick the layout to match your workload, don't accept defaults blindly
cluster = LocalCluster(
    n_workers=4,
    threads_per_worker=1,        # pure-Python / GIL-bound work -> processes, 1 thread each
    memory_limit="4GiB",         # per worker; "0" disables the limit (rarely a good idea)
    dashboard_address=":8787",
)
client = Client(cluster)
print(client.dashboard_link)     # always open this first when debugging

# NumPy / pandas / array work releases the GIL -> fewer workers, more threads is faster:
cluster = LocalCluster(n_workers=1, threads_per_worker=8)
```

```python
# Connect to an existing scheduler (dask-scheduler / dask-worker, HPC, k8s)
client = Client("tcp://scheduler-host:8786")
client.get_versions(check=True)  # FIRST thing on a remote cluster: catch env mismatches
```

`pip install dask` alone does NOT install the scheduler — you need `dask[distributed]`
(or `dask[complete]`), otherwise distributed examples raise ImportError (#962).

## Version & environment matching (the #1 silent failure)

Client, scheduler, and every worker must run **matching** versions of `dask`,
`distributed`, and any library whose objects you serialize (pyarrow, numpy, pandas, cloudpickle).
Mismatches surface as cryptic errors, not clear messages:

```python
client.get_versions(check=True)  # raises ValueError listing mismatched packages
```

| Symptom | Real cause |
|---------|-----------|
| `CRITICAL - Failed to Serialize` / `Failed to deserialize` (#2597, #3532) | library version differs across env, or object isn't picklable |
| `pickle_loads ... IndexError: tuple index out of range` (#4645) | `distributed` version mismatch client vs worker |
| `unpack requires a bytes object of length 8` (#1059) | corrupted/mismatched comm protocol — check versions |
| Worker dies with a traceback right after connecting (killed.html) | unrecoverable import/version error — read the worker log |

Pin the same env everywhere (conda env file, container image, or coiled package sync).

## Worker memory: thresholds, spilling, and unmanaged memory

Workers manage memory in four stages (fractions of the process memory limit):

| Threshold | Default | Behavior |
|-----------|---------|----------|
| target  | 0.60 | spill least-recently-used data to disk |
| spill   | 0.70 | spill based on *measured* process memory |
| pause   | 0.80 | stop accepting new tasks |
| terminate | 0.95 | nanny kills & restarts the worker; tasks reschedule |

```python
# Configure programmatically BEFORE creating the cluster
import dask
dask.config.set({"distributed.worker.memory.target": 0.6,
                 "distributed.worker.memory.spill": 0.7,
                 "distributed.worker.memory.pause": 0.85,
                 "distributed.worker.memory.terminate": 0.95})
```

**Unmanaged memory** (allocator fragmentation, not data Dask tracks) is the usual culprit
behind "memory full but nothing to spill" hangs and slow leaks in long jobs (#2757, #5960, #6110).
The dashboard Workers tab shows "unmanaged old" growing. Fixes, in order:

```bash
# 1. On Linux, aggressively return freed memory to the OS (set before launching workers)
MALLOC_TRIM_THRESHOLD_=0 dask worker tcp://scheduler:8786
```
```python
# 2. Manually trim once (Linux) for diagnosis or between iterations
import ctypes
def trim(): return ctypes.CDLL("libc.so.6").malloc_trim(0)
client.run(trim)
```
- **3. Smaller partitions** — the real fix for "workers stuck on a big CSV/parquet" (#1467, #1674).
  One partition must fit comfortably in one worker (aim well under `memory_limit`).
- **4. `client.restart()`** clears accumulated state as a blunt last resort (#2757).
- Don't run with `--no-nanny` to dodge restarts: you lose memory monitoring and auto-recovery.

## Connection timeouts

`Timed out trying to connect ... connect() didn't finish in time` (#4080, #2880) means the
scheduler/worker comms are saturated or the network is slow — often the event loop is blocked
by a huge graph, large `scatter`, or GIL-holding code, not a true network fault.

```python
import dask
dask.config.set({"distributed.comm.timeouts.connect": "60s",
                 "distributed.comm.timeouts.tcp": "60s"})
```
First reduce graph/transfer size (smaller partitions, avoid broadcasting huge objects) before
just raising timeouts. `scatter(large_obj, broadcast=True)` to many workers is a classic trigger.

## Dashboard not reachable

`client.dashboard_link` is the primary debugging tool. If `/status` times out while the root
page loads (#1027, #227), it's almost always missing/stale bokeh, not a firewall — confirm
with `curl localhost:8787/status`. Ensure `bokeh` is installed and matches across the env; on
remote clusters forward the port (`ssh -L 8787:localhost:8787 host`).

## Debugging workflow

```python
# 1. Dashboard first — memory pressure, task pileup, "event loop unresponsive" warnings
print(client.dashboard_link)

# 2. Pull worker logs (works while workers are alive)
client.get_worker_logs()            # dict {worker: log}; scheduler: client.get_scheduler_logs()

# 3. Reproduce a failing task locally with the real inputs
future = client.submit(fn, *args)
client.recreate_error_locally(future)   # raises the exception in your process for pdb

# 4. Capture a shareable performance trace of a slow region
from distributed import performance_report
with performance_report(filename="report.html"):
    result = my_computation.compute()
```

When a worker just disappears with no traceback, it was killed externally (OOM-killer, k8s
eviction, spot reclaim) — check the *deployment's* logs (k8s/HPC/cloud), not just Dask's.

## Gotchas & Common Mistakes

### `Worker failed to start` / nanny start timeout (#1825)
Slow process spawn (Windows, slow network FS, old tornado) exceeds the 30s start timeout.
Upgrade `distributed`/`tornado`; on Windows try `dask.config.set({"distributed.worker.multiprocessing-method": "spawn"})`.

### `distributed CLI: cannot import name '_unicodefun' from click` (#6013)
A `click` release removed a private API used by old `distributed`. Upgrade `distributed`
(or pin `click<8.1`).

### Creating clusters without `if __name__ == "__main__":`
On Windows/spawn, building a `LocalCluster` at module top level forks recursively. Guard it.

## Performance Tips

- **FAST: chunk so one partition fits in one worker** — most stalls and OOMs are oversized partitions.
- **FAST: GIL-releasing work (numpy/pandas) → threads; pure Python → processes** (`threads_per_worker=1`).
- **FAST: `client.persist()`** to keep an intermediate in distributed memory; **`scatter` once**, don't re-send big args per task.
- **SLOW: many tiny tasks** — graph/scheduler overhead dominates; coarsen partitions.
- **Adaptive scaling:** `cluster.adapt(minimum=2, maximum=50)` scales workers to the workload.

## Reference
- Setup, Client, Managing Memory, "Why did my worker die?": https://distributed.dask.org/en/stable/
- Worker memory thresholds & trimming: https://distributed.dask.org/en/stable/worker-memory.html
- Cloud clusters (AWS/GCP/Azure): see the **coiled** skill.
