"""
Data Siege defense — Đỗ Quang Vinh (2A202602989).

Design (one metered call per event, never more):
  1. Read the payload (free) for anything decidable without a tool.
  2. Make exactly ONE tool call for the event type.
  3. Judge each signal against a threshold placed BETWEEN the largest clean value
     and the smallest faulty value — not at the bare 3-sigma baseline, which is
     either too tight (false positives on natural variance) or too loose
     (misses subtle faults).
  4. For data batches, also score a multi-signal composite distance, so a batch
     that is mildly off on several axes at once is caught even when every single
     axis is still inside its own bound.

Budget: practice/public = 220 cr, private = 320 cr. A full scan of private costs
~300 cr, so calling the tool on every event fits; on public the ~9% overage costs
~1.8 points, far less than the TPR lost by skipping events (each missed fault is
worth more than any single tool call). No per-event answers are hard-coded —
every decision is a function of the measured signals and the published baseline.

Debugging: never print() (stdout is the RPC channel). Stderr is piped but not
drained while the run is live, so large stderr output deadlocks the run on
Windows — keep it out of normal runs.
"""
import math

from api import Verdict

# ---------------------------------------------------------------------------
# Thresholds, set from a sensitivity sweep on practice + public only (never on
# private): each sits inside the measured gap between the largest clean value
# and the smallest faulty one, leaning toward the clean side where the gap is
# wide (a missed fault costs ~5x a false alarm in this score), and kept at the
# midpoint where the gap is tight. Lineage runtime stays at the published
# baseline: 0.9x of it already alarms on clean runs.
# ---------------------------------------------------------------------------
FEATURE_SHIFT_SIGMA = 0.60          # clean peaks ~0.45-0.50 (practice+public); faults from ~0.95
EMBED_CENTROID_SHIFT = 0.0395       # clean peaks 0.038-0.039; drift from 0.039-0.0405 — tight gap
CORPUS_AGE_DAYS = 43.0              # clean peaks 40-41 days; stale corpora from ~47.5 days
COMPOSITE_NORM2 = 1.30              # clean batches reach 1.1-1.2 on public; lower => false alarms
REQUIRED_UPSTREAM = {"dbt:stg_orders": {"raw.orders", "raw.customers"}}


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def _num(x, default=None):
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _alert(pillar, reasons):
    return Verdict(alert=True, pillar=pillar, reason="; ".join(reasons)[:300], confidence=1.0)


def _quiet(pillar, note=""):
    return Verdict(alert=False, pillar=pillar, reason=note[:300])


def _bump(ctx, key):
    stats = ctx.state.setdefault("counts", {})
    stats[key] = stats.get(key, 0) + 1


# ---------------------------------------------------------------------------
# 1. data_batch — pillar: checks
# ---------------------------------------------------------------------------
def check_data_batch(payload, ctx):
    _bump(ctx, "data_batch")
    prof = ctx.tools.batch_profile(payload.get("batch_id"))
    if not isinstance(prof, dict) or "error" in prof:
        return _quiet("checks", "profile unavailable")
    b = ctx.baseline
    rows = _num(prof.get("row_count"))
    mean = _num(prof.get("mean_amount"))
    null = _num((prof.get("null_rate") or {}).get("customer_id"), 0.0)
    stale = _num(prof.get("staleness_min"), 0.0)

    reasons = []
    if rows is not None and not (b["row_count_min"] <= rows <= b["row_count_max"]):
        reasons.append(f"row_count {rows:.0f} outside [{b['row_count_min']:.0f}, {b['row_count_max']:.0f}]")
    if null > b["null_rate_max"]:
        reasons.append(f"customer_id null_rate {null:.4f} > {b['null_rate_max']}")
    if mean is not None and not (b["mean_amount_min"] <= mean <= b["mean_amount_max"]):
        reasons.append(f"mean_amount {mean:.2f} outside [{b['mean_amount_min']:.2f}, {b['mean_amount_max']:.2f}]")
    if stale > b["staleness_min_max"]:
        reasons.append(f"staleness {stale:.1f} min > {b['staleness_min_max']}")
    if reasons:
        return _alert("checks", reasons)

    # Multi-signal composite: each axis normalised by its baseline spread
    # (baseline bounds are mean ± 3σ, so σ = (max - min) / 6).
    mu_rows = (b["row_count_min"] + b["row_count_max"]) / 2
    sd_rows = (b["row_count_max"] - b["row_count_min"]) / 6
    mu_mean = (b["mean_amount_min"] + b["mean_amount_max"]) / 2
    sd_mean = (b["mean_amount_max"] - b["mean_amount_min"]) / 6
    parts = [
        ((rows - mu_rows) / sd_rows) / 3 if rows is not None else 0.0,
        ((mean - mu_mean) / sd_mean) / 3 if mean is not None else 0.0,
        null / b["null_rate_max"],
        stale / b["staleness_min_max"],
    ]
    norm2 = math.sqrt(sum(p * p for p in parts))
    if norm2 > COMPOSITE_NORM2:
        return _alert("checks", [f"composite distance {norm2:.2f} > {COMPOSITE_NORM2} (several signals drifting together)"])
    return _quiet("checks", f"norm2={norm2:.2f}")


# ---------------------------------------------------------------------------
# 2. contract_checkpoint — pillar: contracts
# ---------------------------------------------------------------------------
def check_contract_checkpoint(payload, ctx):
    _bump(ctx, "contract_checkpoint")
    diff = ctx.tools.contract_diff(payload.get("contract_id"), payload.get("checkpoint_batch_id"))
    if not isinstance(diff, dict) or "error" in diff:
        return _quiet("contracts", "contract diff unavailable")
    reasons = []
    violations = diff.get("violations") or []
    if violations:
        reasons.append("violations: " + ", ".join(map(str, violations)))
    delay = _num(diff.get("freshness_delay_min"), 0.0)
    declared = _num((payload.get("declared_sla") or {}).get("freshness_min"))
    limit = ctx.baseline["freshness_delay_max_min"]
    if declared is not None:
        limit = min(limit, declared)
    if delay > limit:
        reasons.append(f"freshness delay {delay:.1f} min > SLA/baseline {limit:.1f}")
    return _alert("contracts", reasons) if reasons else _quiet("contracts")


# ---------------------------------------------------------------------------
# 3. lineage_run — pillar: lineage
# ---------------------------------------------------------------------------
def check_lineage_run(payload, ctx):
    _bump(ctx, "lineage_run")
    sl = ctx.tools.lineage_graph_slice(payload.get("run_id"))
    if not isinstance(sl, dict) or "error" in sl:
        return _quiet("lineage", "lineage slice unavailable")
    reasons = []
    downstream = sl.get("actual_downstream_count")
    if downstream is not None and _num(downstream, 1) == 0:
        reasons.append("orphan output: no downstream consumer")
    actual = {u if isinstance(u, str) else (u or {}).get("name") for u in (sl.get("actual_upstream") or [])}
    declared = {i.get("name") for i in (payload.get("inputs") or []) if isinstance(i, dict)}
    required = (declared | REQUIRED_UPSTREAM.get(payload.get("job"), set())) - {None}
    missing = sorted(required - actual)
    if missing:
        reasons.append("missing upstream: " + ", ".join(missing))
    duration = _num(sl.get("duration_ms"))
    if duration is not None and duration > ctx.baseline["lineage_duration_ms_max"]:
        reasons.append(f"runtime {duration:.0f} ms > {ctx.baseline['lineage_duration_ms_max']:.0f}")
    return _alert("lineage", reasons) if reasons else _quiet("lineage")


# ---------------------------------------------------------------------------
# 4. feature_materialization — pillar: ai_infra
# ---------------------------------------------------------------------------
def check_feature_materialization(payload, ctx):
    _bump(ctx, "feature_materialization")
    fd = ctx.tools.feature_drift(payload.get("feature_view"), payload.get("batch_id"))
    if not isinstance(fd, dict) or "error" in fd:
        return _quiet("ai_infra", "feature drift unavailable")
    sigma = _num(fd.get("mean_shift_sigma"))
    if sigma is None:
        serve, train, std = _num(fd.get("serve_mean")), _num(fd.get("train_mean")), _num(fd.get("train_std"))
        sigma = abs(serve - train) / std if None not in (serve, train, std) and std else 0.0
    if sigma > FEATURE_SHIFT_SIGMA:
        return _alert("ai_infra", [f"training-serving skew: mean shift {sigma:.3f}σ > {FEATURE_SHIFT_SIGMA}σ"])
    return _quiet("ai_infra", f"shift={sigma:.3f}")


# ---------------------------------------------------------------------------
# 5. embedding_batch — pillar: ai_infra
# ---------------------------------------------------------------------------
def check_embedding_batch(payload, ctx):
    _bump(ctx, "embedding_batch")
    ed = ctx.tools.embedding_drift(payload.get("corpus"), payload.get("chunk_batch_id"))
    if not isinstance(ed, dict) or "error" in ed:
        return _quiet("ai_infra", "embedding drift unavailable")
    reasons = []
    shift = _num(ed.get("centroid_shift"), 0.0)
    age = _num(ed.get("avg_doc_age_days"), 0.0)
    if shift > EMBED_CENTROID_SHIFT:
        reasons.append(f"centroid shift {shift:.4f} > {EMBED_CENTROID_SHIFT}")
    if age > CORPUS_AGE_DAYS:
        reasons.append(f"corpus avg doc age {age:.1f} d > {CORPUS_AGE_DAYS}")
    return _alert("ai_infra", reasons) if reasons else _quiet("ai_infra")
