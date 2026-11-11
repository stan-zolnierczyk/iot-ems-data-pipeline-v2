# Silver Layer – Data Cleaning Rules

This document specifies the validation, cleaning and derivation rules implemented in
[`src/silver_layer.py`](../src/silver_layer.py). The Silver layer transforms raw Bronze
telemetry into a validated, gap-free time series on a regular 1-minute grid. Downstream
layers (Gold, Grafana) treat it as the single source of truth.

---

## 1. Scope

### 1.1 Input: Bronze layer

Raw telemetry is pushed every minute by the Grenton edge gateway into the InfluxDB 3 database `EMS-2`.
Each series is identified by the tag pair **(`device`, `measurement_type`)**, and the measured value
is stored in the field `value`.

| Bronze table | Series (`device` / `measurement_type`) | Unit | Description |
|---|---|---|---|
| `power` | `SUN2000` / `activePower` | W | PV production (inverter AC output + LUNA2000 charge/discharge power) |
| `power` | `SUN2000` / `peakPower` | W | Daily peak power of the inverter (running daily maximum) |
| `power` | `LUNA2000` / `chargeDischargePower` | W | Battery power: `> 0` charging, `< 0` discharging |
| `power` | `DTSU666` / `powerBalance` | W | Grid exchange at the point of connection: `> 0` export, `< 0` import |
| `power` | `DTSU666` / `powerConsumption` | W | *Ignored*: recalculated in Silver |
| `energy` | `SUN2000` / `production` | Wh | Daily inverter AC energy yield (includes battery discharge) |
| `energy` | `DTSU666` / `import` | Wh | Daily energy imported from the grid |
| `energy` | `DTSU666` / `export` | Wh | Daily energy exported to the grid |
| `energy` | `LUNA2000` / `currentDayCharge` | Wh | Daily energy charged into the battery |
| `energy` | `LUNA2000` / `currentDayDischarge` | Wh | Daily energy discharged from the battery |
| `energy` | `DTSU666` / `consumption` | Wh | *Ignored*: recalculated in Silver |
| `battery_soc` | `LUNA2000` / `stateOfCharge` | % | Battery state of charge |

All energy series are **daily counters**. Each one should not decrease during the day and resets
to 0 around local midnight. The inverter (`production`) and the Grenton controller (`import`, `export`)
use independent clocks, so their resets may be a few minutes apart.

### 1.2 Output: Silver layer

| Silver table | Content |
|---|---|
| `power_clean` | Cleaned power series + derived `EMS_Calculated` / `powerConsumption` |
| `energy_clean` | Cleaned daily counters + derived `EMS_Calculated` / `consumption` |
| `battery_soc_clean` | Cleaned battery state of charge |

The output schema is the same as the input schema: tags `device` and `measurement_type`, field `value`.
Output timestamps are aligned to full minutes (UTC).

---

## 2. Processing model

### 2.1 Incremental loading

Each Silver table is processed **independently**, in the order `power` → `energy` → `battery_soc`:

1. **Watermark:** `last_ts = MAX(time)` of the Silver table, searched within the last 7 days.
2. **Processing window:** starts at `start = last_ts − LOOKBACK` (`LOOKBACK` = 15 min).
   On the first run (no Silver data yet), the window covers the last hour.
3. **Bronze fetch:** all rows with `start ≤ time ≤ now()`. Rows timestamped in the future,
   for example because of a wrong device clock, are excluded.
4. **Seed:** for each series, the last cleaned value **before** `start` is fetched from the
   Silver table (searched within the last 24 h; derived `EMS_Calculated` series are excluded)
   and prepended to the Bronze rows.
5. **Validation and derivation**, as described in sections 3–5.
6. **Write:** all rows with `time ≥ start` are written to the Silver table.

### 2.2 Rationale

- **Independent watermarks.** A delay in one Bronze table cannot cause minutes to be skipped
  in another Silver table.
- **Reprocessing the lookback window.** Each run recomputes and overwrites the last 15 minutes
  of its previous output. Writes are idempotent (same series, same timestamp), so values that were
  filled or computed with incomplete context are corrected once later data arrives.
- **Seed.** Filters start from a validated value instead of the first, possibly corrupted, raw sample.
  Validation state, such as the running maximum of a counter or the last valid SoC, therefore
  carries over between runs. Seed rows are context only and are never rewritten.

### 2.3 Error handling

- If a Silver table does not exist yet (the query error contains `not found`), the run is treated
  as a first run.
- Any other query error, for example an authentication or network failure, aborts the run.
  The run is not silently treated as a first run.

---

## 3. Common transformations

### 3.1 Resampling to a 1-minute grid

Rows are pivoted to one column per (`device`, `measurement_type`) series. Timestamps are floored to
the minute. If a minute contains several samples of the same series, the **last** sample is kept.
The index is then extended to a continuous 1-minute grid, and minutes with no sample become
missing values, to be filled by the series-specific rules below.

### 3.2 Domain range filter

Values outside the physically plausible range of a series are rejected (set to missing).
This rule also removes invalid-data markers reported by Huawei devices,
e.g. `0x7FFFFFFF`-based values.

### 3.3 Order of operations

For every series: **range filter → spike / step filter → monotonicity (counters only) → gap filling →
derivation of calculated series.** Rejected samples are treated in the same way as missing samples.

---

## 4. Rules per table

### 4.1 Power (`power` → `power_clean`)

| Series | Valid range | Additional rule | Gap filling |
|---|---|---|---|
| `SUN2000` / `activePower` | 0 … 12 000 W | — | Linear interpolation |
| `SUN2000` / `peakPower` | 0 … 12 000 W | Daily monotonicity (§ 6) | Previous valid value |
| `LUNA2000` / `chargeDischargePower` | −5 000 … 5 000 W | — | Linear interpolation |
| `DTSU666` / `powerBalance` | −40 000 … 12 000 W | — | Linear interpolation |

- The lower limit of `powerBalance` follows from the 40 kW grid connection capacity.
  The upper limit (maximum export) follows from the inverter's AC output.
- `peakPower` is a running daily maximum, so it is filled with the previous valid value
  instead of being interpolated. Interpolating across the midnight reset would create
  artificial decreasing values before midnight.
- Linear interpolation also extends the last valid value to any minutes at the end of the window
  that have no sample yet. Such values are recomputed by the next run (§ 2.2).

**Derived series: `EMS_Calculated` / `powerConsumption`**

```
powerConsumption = (activePower − chargeDischargePower) − powerBalance,   clipped to ≥ 0
```

`activePower − chargeDischargePower` is the inverter's AC output (PV production minus battery
charging plus battery discharging). Subtracting the grid balance gives the instantaneous
consumption of the building. The derived value is computed from cleaned components only.
The value reported by the edge gateway is not used.

### 4.2 Energy (`energy` → `energy_clean`)

| Series | Valid range |
|---|---|
| `SUN2000` / `production` | 0 … 120 000 Wh |
| `DTSU666` / `import` | 0 … 120 000 Wh |
| `DTSU666` / `export` | 0 … 120 000 Wh |
| `LUNA2000` / `currentDayCharge` | 0 … 25 000 Wh |
| `LUNA2000` / `currentDayDischarge` | 0 … 25 000 Wh |

Each counter is processed as follows:

1. **Range filter** (§ 3.2).
2. **Counter cycle detection** (§ 6.1): the series is split into daily cycles at accepted resets.
3. **Upward spike filter:** a value is rejected if it exceeds the centered rolling median
   (window of 5 samples, computed **within the same cycle**) by more than 1 000 Wh.
   The median is computed per cycle so that values from before and after the midnight reset
   are never mixed. Downward deviations are handled by the next step.
4. **Daily monotonicity** (§ 6.2): within a cycle, any value below the highest previously accepted
   value is rejected.
5. **Gap filling:** rejected and missing samples take the **previous valid value**
   (forward fill). Counters are never filled with zeros.

**Derived series: `EMS_Calculated` / `consumption`**

```
consumption = production + import − export
```

The `production` register reports the inverter's AC energy output, which already includes
battery discharge and excludes energy used for battery charging. The formula therefore gives
the building's daily energy consumption without explicit battery terms. It is computed from
cleaned and filled counters. The value reported by the edge gateway is not used.

### 4.3 Battery state of charge (`battery_soc` → `battery_soc_clean`)

1. **Range filter:** 0 … 100 %.
2. **Step filter:** a value is rejected if it differs from the previous valid value by more than
   `2 % × minutes elapsed` since that value. With a 5 kW maximum battery power and 10 kWh capacity,
   the physical limit is about 0.8 %/min.
3. **Gap filling:** previous valid value (forward fill).

The state of charge is written to a dedicated table, so that Gold does not treat it as an
energy counter.

---

## 5. Derived series: general principles

- Derived series are computed **after** their components have been cleaned and filled.
- Derived series are tagged `device = EMS_Calculated`.
- Values reported by the edge gateway for the same quantities (`DTSU666` / `powerConsumption`,
  `DTSU666` / `consumption`) are excluded at query time.

---

## 6. Daily counter semantics

These rules apply to the energy counters and to `peakPower`.

### 6.1 Reset detection (counter cycles)

A decrease between two consecutive valid samples is accepted as a **midnight reset** if either:

- the local calendar date (Europe/Warsaw) has changed since the previous valid sample, **or**
- the sample lies within the reset window **23:50–00:15 local time** and its value is close to 0
  (≤ 1 000 Wh for counters, ≤ 1 000 W for `peakPower`).

The reset window allows for clock differences between the inverter and the controller. The date-change
condition covers data gaps spanning midnight, where the first sample of the new day may already be
well above 0. Every accepted reset starts a new **counter cycle**. The same cycle definition is used by
both the spike filter and the monotonicity rule.

### 6.2 Monotonicity

Within a cycle, a valid sample must be greater than or equal to the highest previously accepted value
of that cycle. Equal values are valid, because a counter does not change when there is no energy flow.
Any lower value is rejected. Because of the seed (§ 2.1), the running maximum continues across
consecutive runs.

---

## 7. Parameters

| Parameter | Value | Purpose |
|---|---|---|
| `LOOKBACK` | 15 min | Reprocessed and rewritten window preceding the watermark |
| `SEED_MAX_AGE` | 1 day | Maximum age of the last cleaned value used as a seed |
| `SPIKE_THRESHOLD_WH` | 1 000 Wh | Upward spike threshold relative to the rolling median (counters) |
| `RESET_MAX_VALUE` | 1 000 (Wh / W) | Maximum value accepted as "close to 0" after a reset |
| `SOC_MAX_STEP_PER_MIN` | 2 % | Maximum state-of-charge change per minute |
| `LOCAL_TZ` | Europe/Warsaw | Time zone of the daily counter resets |
| Reset window | 23:50–00:15 local | Window in which a near-zero drop is accepted as a reset |
| Rolling median window | 5 samples, centered | Spike filter |
| First-run window | 1 h | Bronze history processed when no Silver data exists |
| Watermark search range | 7 days | Range searched for the last processed timestamp |

---

## 8. Known limitations

The following limitations are known and accepted at this stage. They are candidates for future
improvements in the Silver or Gold layer.

1. **Daily consumption around midnight.** `consumption` is computed from counter levels. Between the
   controller's reset of `import`/`export` and the inverter's reset of `production` (observed offset:
   about 4 minutes), the derived value mixes counters from two different days, so it is not a reliable
   basis for hourly increments around midnight (observed error: approx. 2.8 kWh/day).
   **Resolved in Gold:** hourly consumption is not derived from this series. Gold computes it from
   per-minute, reset-aware increments of the individual counters
   (`Δproduction + Δimport − Δexport`). See [`src/gold_aggregator.py`](../src/gold_aggregator.py).
2. **In-range corruption of power values.** Power series have range rules only. Corrupted readings that
   fall within the valid range, such as values from adjacent Modbus registers during communication
   disturbances, pass validation and also affect the derived `powerConsumption`.
3. **Small upward outliers in counters.** An upward deviation below the spike threshold (< 1 000 Wh)
   is accepted. Monotonicity then holds the counter at that value until the true counter catches up
   or the day ends.
4. **Near-zero glitches in the reset window.** A spurious near-zero reading between 23:50 and 00:15 local
   time is accepted as a reset. The counter appears to restart and then returns to its true value.
5. **Missing seed.** If a series has no cleaned value within the last 24 h and its first sample in the
   window is missing or rejected, the leading minutes of that series stay empty until the first valid
   sample. Power interpolation and forward fill do not fill backwards.
