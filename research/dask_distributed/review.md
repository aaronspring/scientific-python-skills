# Dask Distributed Skill File Review

Reviewed: `skills/dask_distributed/SKILL.md` (191 lines, 9.3KB)
Source verified against: `distributed` 2026.3.0 / `dask` 2026.3.0 (installed) + source clone
at `repos/dask-tmp/distributed/`. APIs checked via import, `inspect.signature`,
`dask.config.get`, and a real `LocalCluster` run. Issue numbers cross-checked against
`research/dask_distributed/topic_summary.json`.

---

## ACCURACY

### Verified correct (no changes needed)

**Top-level imports** — `Client`, `LocalCluster`, `worker_client`, `secede`, `rejoin`,
`get_client`, `performance_report` all import cleanly.

**`LocalCluster` kwargs** — `n_workers`, `threads_per_worker`, `processes`,
`dashboard_address` confirmed in signature. `memory_limit` is not in the explicit signature
but **is** handled in `LocalCluster.__init__` (forwarded to workers) — confirmed by reading
the source; the skill's usage `memory_limit="4GiB"` is correct.

**Memory threshold defaults** — verified exactly against `dask.config.get`:
`target=0.6`, `spill=0.7`, `pause=0.8`, `terminate=0.95`. The skill's table matches.

**Config keys** — `distributed.worker.memory.{target,spill,pause,terminate}`,
`distributed.comm.timeouts.{connect,tcp}` (both default `"30s"`),
`distributed.worker.multiprocessing-method` (default `"spawn"`) all resolve via
`dask.config.get` — no typos.

**Client methods** — `get_versions`, `get_worker_logs`, `get_scheduler_logs`, `run`,
`persist`, `scatter`, `submit`, `map`, `restart` all present on the class.
`recreate_error_locally` returns `False` for `hasattr(Client, …)` at the **class** level
(it's defined on the `ReplayTaskClient` mixin, `distributed/recreate_tasks.py:148`, attached
at instance construction) but is present and callable on a live `Client` instance — verified
with a real `LocalCluster`. The skill's `client.recreate_error_locally(future)` usage is
correct.

**`cluster.adapt`** — present (`hasattr` True); `cluster.adapt(minimum=2, maximum=50)` valid.

### Runtime test (the CRITICAL example)

Ran the `secede()` / `worker_client()` / `rejoin()` nested-task pattern from the CRITICAL
section as a real script against a 2-worker `LocalCluster`:
- `client.map(process, [1,2,3,4])` → `[2, 4, 6, 8]` ✓
- `performance_report(filename=…)` wrote the HTML file ✓
- `client.get_worker_logs()` returned a dict keyed by worker ✓

Note: the first attempt via heredoc hit `RuntimeError: Nanny failed to start` — this is
exactly the `if __name__ == "__main__":` / spawn issue the skill's own gotcha warns about
(stdin has no importable module for the spawned process). Re-running as a real `.py` file
with the `__main__` guard worked. This incidentally confirms that gotcha is real.

**Issue numbers** — #962, #3791, #2336, #4612, #2597, #3532, #4645, #1059, #2757, #5960,
#6110, #1467, #1674, #4080, #2880, #1027, #227, #1825, #6013 all map to real items in
`topic_summary.json`.

---

## COMPLETENESS / SCOPE

- The three-failure-families framing in the intro (env mismatch / memory / nested compute)
  is accurate and matches the dominant issue clusters in `topic_summary.json`.
- Correctly positions itself as the underlying layer beneath the **coiled** skill and
  cross-links it. No duplication of cloud-provisioning content.

## STRUCTURE

Follows the template: CRITICAL pitfall first → Setup → version matching → memory → timeouts →
dashboard → debugging workflow → gotchas → performance → reference. Under the 30KB cap
(9.3KB) and 500-line target (191 lines).

---

## VERDICT

No bugs found — every signature, config key, default value, Client method, and the CRITICAL
runtime example verified accurate against distributed 2026.3.0. The skill is unusually
testable (LocalCluster runs locally) and passed end-to-end execution.
