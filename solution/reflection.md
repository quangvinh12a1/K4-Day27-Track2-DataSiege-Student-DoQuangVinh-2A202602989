# Reflection — Đỗ Quang Vinh (2A202602989)

| Phase | Score | TPR | FPR | Cost |
|---|---|---|---|---|
| Practice | **50.00** | 1.000 (33/33) | 0.000 | 0% over (220 cr) |
| Public | **48.18** | 1.000 (39/39) | 0.000 | 240/220 cr, overage 0.0909 |
| Private | **32.41** | 0.648 | 0.000 | no overage (320 cr) |

**Which fault types were hardest to catch, and why?**
The subtle-magnitude faults that sit inside the natural spread of clean data. Practice
and public were dominated by obvious faults, so every detector reached 100% recall there,
yet private recall dropped to 0.648 with zero false alarms. That pattern means the
misses are faults whose single signals stayed below my thresholds, not detectors that
were wrong. The tightest gap I measured was the **embedding centroid shift**: clean
batches peak at 0.038–0.039 and drift starts at 0.039–0.0405, so a static threshold can
barely separate them. **Data batches** were the other hard case. Clean public batches
already reach a composite distance of 1.1–1.2, so the composite threshold could not drop
below 1.30 without false alarms, and a subtle volume, null or freshness fault that moves
one axis by under 3σ stays invisible. Contract and lineage faults are structural
(violations list, missing `raw.customers`, zero downstream), so they are binary and easy;
runtime anomalies are the exception, because clean runs already reach 0.9× the baseline.

**How I balanced cost against TPR.** One metered call per event, never more, and no free
"skip" heuristics. The score makes each caught fault worth about 1 point and each credit
of overage about 0.06 points (private budget 320), so on public I accepted the 9% overage
(−1.82) rather than skip events, and on private a full scan (~300 cr) fits the budget. I
set thresholds with a sensitivity sweep on practice + public only — never on private —
placing each one inside the measured clean/faulty gap. Where the gap was wide (feature
skew 0.47 → 0.95, corpus age 41 → 47.5 days) I leaned toward the clean side, because a
missed fault costs ~5× a false alarm in this formula. Where it was tight (centroid
shift), I kept the midpoint.

**What I would change with another pass (and how I'd use `ctx.state`).**
1. **Stream-adaptive baselines.** Keep a running mean/variance per signal over events I
   judged clean (Welford in `ctx.state`), and alert on a z-score against *this stream's*
   own clean distribution. That catches a shift that is small in absolute terms but
   unusual for the stream.
2. **Sequential (CUSUM-style) detectors.** Accumulate small deviations across consecutive
   batches of the same table or feature view. Subtle faults that persist are only visible
   as a sum, not as one event.
3. **A per-pillar composite for ai_infra**, combining centroid shift and doc age the way
   the data-batch composite combines rows, mean, nulls and staleness.
4. **Spend the slack.** Private finished under budget, so roughly 20 credits were
   available; I'd use them on a second look only for events whose first signal lands in
   the ambiguous band just below a threshold.

The lesson: practice/public at 100% recall did not imply private robustness. My
thresholds were calibrated on a stream that rarely produced subtle faults, which is
exactly what the anti-overfit rule warns about.

*AI use:* Claude Code (Claude Opus 5.5) helped write `defense.py`, run the
practice/public sweeps and draft this reflection; I reviewed every threshold and its
justification.
