# Coiled Skill File Review

Reviewed: `skills/coiled/SKILL.md` (164 lines, 8.7KB)
Source verified against: `coiled` 1.135.0 (installed via `uv add coiled`) — signatures via
`inspect.signature`, CLI via `coiled … --help`. Issue numbers cross-checked against
`research/coiled/topic_summary.json` (the `coiled/feedback` tracker).

---

## ACCURACY

### Bug found & fixed

**1. `coiled cluster better-logs` is not a valid subcommand — FIXED**

The skill referenced `coiled cluster better-logs` in two places (the "packages silently
omitted" gotcha and the debugging-logs section). `coiled cluster --help` in 1.135.0 lists:
`azure-logs`, `better-logs-cli`, `logs`, `logs-via-aws-cli`, `ssh` — there is no
`better-logs`. Running `coiled cluster better-logs --help` errors with "Try 'coiled cluster
-h' for help." Both occurrences were corrected to `coiled cluster better-logs-cli`.

### Verified correct (no change needed)

**`coiled.Cluster` kwargs** — all of the following are present in the 1.135.0 signature:
`n_workers`, `region`, `spot_policy`, `worker_memory`, `worker_vm_types`, `worker_options`,
`idle_timeout`, `mount_bucket`, `arm`, `allow_ssh`, `package_sync_ignore`, `software`,
`container`, `name`.

**`threads_per_worker` / `processes` correctly absent** — confirmed NOT in the signature, so
the skill's claim that passing them raises `TypeError: unexpected keyword argument` is
accurate.

**`use_magic` correctly described as removed** — not in the 1.135.0 signature; the skill
correctly says it is the default now and should not be passed.

**`cluster.get_client()` and `cluster.get_logs()`** — both present (`hasattr` True).

**`coiled.function`** — `region`, `memory`, `spot_policy` all in the signature; the decorated
object exposes both `.map()` and `.submit()` (verified at runtime).

**CLI subcommands** — `coiled batch run`, `coiled cluster ssh` (with `--worker` and `--dask`
flags), `coiled cluster logs` (with `--cluster`, `--filter`, `--since`), `coiled notebook
start`, `coiled login` all verified present with the documented flags.

**Issue numbers** — #238, #266, #177, #223, #263, #106, #267, #296, #291, #185, #171, #172
all map to real items in `topic_summary.json` with the cited topics.

---

## COMPLETENESS / SCOPE

- Correctly factors all scheduler/Client behavior (killed workers, memory thresholds,
  timeouts, serialization, nested compute) out to the **dask_distributed** skill and
  cross-links both directions. Good separation; no duplication.
- The "one worker = one VM" framing up top is the single most valuable insight and is placed
  first. Verified against issue #238 discussion.

## STRUCTURE

Follows the project template: lead with the mental model → Setup → Software/Package Sync →
Gotchas → Worker memory → Debugging → Performance → Known Limitations. Under the 30KB cap
(8.7KB) and 500-line target (164 lines).

---

## VERDICT

One real bug (`better-logs` → `better-logs-cli`), now fixed. All other code examples,
signatures, CLI commands, and issue references verified accurate against coiled 1.135.0.
Note: coiled requires cloud auth, so cluster creation cannot be exercised end-to-end in CI;
all verification was via signature introspection and `--help`, which is sufficient for the
API claims the skill makes.
