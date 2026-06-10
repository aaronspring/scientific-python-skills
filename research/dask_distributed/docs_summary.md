## DASK DISTRIBUTED RESEARCH SUMMARY

Verified against: `distributed` 2026.3.0 / `dask` 2026.3.0 (installed) + source clone at
`repos/dask-tmp/distributed/` + the `dask/distributed` issue tracker
(research/dask_distributed/topic_summary.json).

### What is `distributed`

`distributed` is Dask's centrally-managed, distributed scheduler. A cluster is a
**scheduler** plus one or more **workers**; a **Client** connects your session to the
scheduler and submits work (dask collections, or `client.submit`/`client.map` futures).
`LocalCluster` runs the whole thing on one machine; remote clusters use
`dask-scheduler`/`dask-worker`, HPC, k8s, or Coiled.

**`pip install dask` does NOT install the scheduler** — distributed examples raise
`ImportError` until you install `dask[distributed]` or `dask[complete]` (issue #962).

**The three failure families** (covering the vast majority of "my cluster is broken"):
1. Client / scheduler / worker **environments don't match** (versions, libraries).
2. Workers run out of memory — usually **unmanaged memory** or **oversized partitions**.
3. A task internally calls `.compute()` / `client.gather()` / constructs a `Client` and
   **deadlocks** once there is >1 worker.

---

### Python API Map (verified against source/instance, 2026.3.0)

#### Top-level imports (all verified importable)
`Client`, `LocalCluster`, `worker_client`, `secede`, `rejoin`, `get_client`,
`performance_report`.

#### `LocalCluster(...)` — key kwargs (verified in signature / `__init__`)
- `n_workers`, `threads_per_worker`, `processes`, `dashboard_address`,
  `worker_dashboard_address`, `protocol`, `interface`, `security`, `host`,
  `scheduler_port`, `silence_logs`, `worker_class`, `scheduler_kwargs`, `worker_kwargs`.
- `memory_limit` — accepted (handled in `__init__`, forwarded to workers); per-worker;
  `"0"` disables the limit.

Layout guidance:
- GIL-releasing work (numpy/pandas/array) → fewer workers, more threads.
- Pure-Python / GIL-bound work → more processes, `threads_per_worker=1`.

#### `Client` methods (verified `hasattr` on instance)
`get_versions`, `recreate_error_locally`, `get_worker_logs`, `get_scheduler_logs`,
`run`, `persist`, `scatter`, `submit`, `map`, `restart`, `gather`, `close`.
Property: `dashboard_link`.

- `client.get_versions(check=True)` → raises `ValueError` listing mismatched packages
  across client/scheduler/workers. **First thing to run on a remote cluster.**
- `client.recreate_error_locally(future)` — re-raises a failed task's exception in the
  local process for pdb. (Defined on the `ReplayTaskClient` mixin in
  `distributed/recreate_tasks.py:148`; attached at instance level — `hasattr(Client, …)`
  on the class is `False`, but it works on an instance.)
- `client.get_worker_logs()` → `dict {worker_addr: log}` (verified at runtime).

#### Coordination primitives for nested work
- `secede()` — release the calling task's thread back to the worker pool while it waits.
- `rejoin()` — reclaim a thread slot before returning.
- `worker_client()` — context manager that reuses the worker's own client; **never**
  construct a new `Client()` inside a task.

---

### CRITICAL: nested compute / Client-in-task deadlock

Calling `.compute()`, `client.gather()`, or `Client()` from inside a task occupies a worker
thread waiting on the scheduler and deadlocks once >1 worker exists (#3791, #2336, #4612).
The correct pattern (verified to run — returns `[2,4,6,8]`):

```python
from distributed import worker_client, secede, rejoin
def process(id):
    secede()
    with worker_client() as client:
        result = client.submit(fn, id).result()
    rejoin()
    return result
```

A *second* `Client` to the same scheduler also yields
`"Inputs contain futures created by another client"` for published-dataset futures (#2336).

---

### Worker memory: thresholds (verified defaults)

| Threshold | Config key | Default (verified) | Behavior |
|-----------|-----------|--------------------|----------|
| target | `distributed.worker.memory.target` | 0.6 | spill LRU data to disk |
| spill | `distributed.worker.memory.spill` | 0.7 | spill based on measured process memory |
| pause | `distributed.worker.memory.pause` | 0.8 | stop accepting new tasks |
| terminate | `distributed.worker.memory.terminate` | 0.95 | nanny kills + restarts worker |

**Unmanaged memory** (allocator fragmentation Dask doesn't track) is the usual cause of
"memory full but nothing to spill" hangs and slow leaks (#2757, #5960, #6110). The
dashboard Workers tab shows "unmanaged old" growing. Fixes, in order:
1. `MALLOC_TRIM_THRESHOLD_=0` env var before launching workers (Linux).
2. Manual `ctypes.CDLL("libc.so.6").malloc_trim(0)` via `client.run(...)` (Linux, diagnostic).
3. **Smaller partitions** — the real fix; one partition must fit comfortably under
   `memory_limit` (#1467, #1674).
4. `client.restart()` as a blunt last resort.
Don't use `--no-nanny` to dodge restarts — you lose memory monitoring + auto-recovery.

---

### Connection timeouts (verified config keys + defaults)

`distributed.comm.timeouts.connect` and `distributed.comm.timeouts.tcp` both default to
`"30s"`. `Timed out trying to connect …` (#4080, #2880) usually means the event loop is
blocked (huge graph, large `scatter`, GIL-holding code), not a true network fault. Reduce
graph/transfer size before raising timeouts. `scatter(large, broadcast=True)` is a classic
trigger.

---

### Environment / version mismatch (the #1 silent failure)

Client, scheduler, and every worker must run matching `dask`, `distributed`, and any library
whose objects you serialize (pyarrow, numpy, pandas, cloudpickle). Symptoms map to causes:

| Symptom | Real cause |
|---------|-----------|
| `Failed to Serialize` / `Failed to deserialize` (#2597, #3532) | library version differs across env, or object not picklable |
| `pickle_loads … IndexError: tuple index out of range` (#4645) | distributed version mismatch client vs worker |
| `unpack requires a bytes object of length 8` (#1059) | corrupted/mismatched comm protocol — check versions |
| worker dies right after connecting | unrecoverable import/version error — read worker log |

---

### Other verified gotchas

- **Nanny failed to start** when a `LocalCluster` with processes is built at module top level
  without `if __name__ == "__main__":` (spawn re-imports the module) — observed directly when
  running via stdin/heredoc. `distributed.worker.multiprocessing-method` defaults to `"spawn"`
  (verified). Guard cluster creation in `__main__`.
- Nanny start timeout (#1825): slow process spawn (Windows, slow FS, old tornado) exceeds 30s.
- `cannot import name '_unicodefun' from click` (#6013): a click release removed a private API
  used by old distributed; upgrade distributed or pin `click<8.1`.
- Dashboard `/status` times out while root page loads (#1027, #227): almost always
  missing/stale `bokeh`, not a firewall; `curl localhost:8787/status` to confirm.

---

### Debugging workflow (verified APIs)

1. `print(client.dashboard_link)` — memory pressure, task pileup, "event loop unresponsive".
2. `client.get_worker_logs()` / `client.get_scheduler_logs()` (works while alive).
3. `client.recreate_error_locally(future)` — reproduce the failing task for pdb.
4. `with performance_report(filename="report.html"):` — shareable trace (verified writes file).

When a worker just disappears with no traceback, it was killed externally (OOM-killer, k8s
eviction, spot reclaim) — check the deployment's logs, not just Dask's.

---

### Performance Tips

- Chunk so one partition fits in one worker — most stalls/OOMs are oversized partitions.
- GIL-releasing work → threads; pure Python → processes (`threads_per_worker=1`).
- `client.persist()` to keep an intermediate in cluster memory; `scatter` big args once.
- Avoid many tiny tasks (scheduler overhead) — coarsen partitions.
- `cluster.adapt(minimum=2, maximum=50)` for adaptive scaling (`adapt` verified present).

---

### Relationship to coiled

A Coiled cluster is a `distributed` cluster; the **coiled** skill covers cloud provisioning,
package sync, and the Coiled CLI, then defers all scheduler/Client behavior to this skill.

---

**End of Research Summary**
