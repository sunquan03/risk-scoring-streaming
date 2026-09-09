# risk-scoring-streaming

A real-time credit risk feature pipeline. Kafka events go in, precomputed risk
features come out over HTTP.

```
Kafka topics ──▶ risk-feature-consumer (Python) ──▶ TiDB (events + kv_cache)
                                                          │
                                        risk-feature-api (Go) ──▶ HTTP /v1
```

## Services

| Service | What it does |
|---|---|
| [risk-feature-producer/](risk-feature-producer/) | Producer made for providing synthetic data for all 4 topics (randomized). |
| [risk-feature-consumer/](risk-feature-consumer/) | Consumes four Kafka topics, writes events to TiDB, computes aggregate features into the `kv_cache` table. Reads its topic and feature-group config from Postgres. |
| [risk-feature-api/](risk-feature-api/) | Serves features from `kv_cache`, computing them live on a cache miss. Listens on `:8080`. |

Topics: `loan-applications`, `loan-payments`, `loan-operations`, `client-money` -
one consumer process per topic.

## API

| Method | Path |
|---|---|
| `GET` | `/v1/health` |
| `GET` | `/v1/risk-profile/:client_id` |
| `GET` | `/v1/risk-profile/:client_id/:group_id` |
| `DELETE` | `/v1/risk-profile/:client_id/cache` |

## Features

81 features across four groups, one group per Kafka topic. Both services compute
them from the same SQL: the consumer reads
[risk-feature-consumer/aggregations/](risk-feature-consumer/aggregations/), the API
embeds byte-identical copies in
[risk-feature-api/internal/features/sql/](risk-feature-api/internal/features/sql/).
Keep the two directories in sync when adding a feature.

Every group also carries `client_id` (the IIN, echoed so a cached payload is
self-describing) and `computed_at` (millisecond UTC aggregation timestamp).

### `loan_app_features` - Loan application history

`client:{client_id}:loan_apps` · TTL 1800s · `loan-applications` → `loan_applications`

Demand-side behaviour: how often the client asks for credit, how much, through
which channel, and what prior underwriters decided. Shortest TTL of the four,
because application velocity is the fastest-moving signal here.

| Feature | What it measures and why it matters |
|---|---|
| `total_applications` | Lifetime count of credit applications. Denominator for every rate below. |
| `apps_last_30d` | Credit hunger. Applications in the trailing month - the strongest short-horizon distress and fraud signal in this group. Shopping several lenders in weeks usually means being declined elsewhere. |
| `apps_last_90d` | Trailing quarter. Smooths a single-month spike and separates a one-off need from sustained shopping. |
| `apps_last_12m` | Trailing year. The structural application rate, and the baseline against which a 30-day spike is judged abnormal. |
| `max_requested_amount` | Largest sum ever requested (KZT). Ceiling of stated need; a far outlier flags a large legitimate purchase or an inflated ask. |
| `min_requested_amount` | Smallest sum requested. Very low values indicate micro-lending behaviour, which carries a distinct risk profile. |
| `avg_requested_amount` | Mean request size. Normalises an incoming application against the client's own history rather than the portfolio's. |
| `total_approved_amount` | Cumulative credit ever approved. Proxy for total lifetime exposure to this client. |
| `max_approved_amount` | Largest single approval. An implicit prior credit decision - the highest limit any underwriter has extended. |
| `approved_count` | Number of approvals. Accumulated positive underwriting history. |
| `rejected_count` | Number of rejections. Adverse decision history; meaningful only alongside `total_applications`. |
| `approval_rate` | Core underwriting-quality score. Approvals ÷ applications. A low rate means repeatedly judged uncreditworthy by decisions that already priced in full information. |
| `last_application_at` | Most recent application timestamp. Drives how stale the rest of the profile should be considered. |
| `last_status` | Outcome of the most recent application (`APPROVED`, `REJECTED`, `PENDING`, `CANCELLED`). Current standing, which a lifetime rate can mask entirely. |
| `last_rejection_reason` | Causal attribution for the latest decline. `FRAUD_RISK` and `EXISTING_OVERDUE` carry categorically different weight from `AGE_LIMIT` - the reason matters more than the count. |
| `distinct_channels` | Channels used across `MOBILE_APP`, `WEB`, `BRANCH`, `KASPI_PAY`, `CALL_CENTER`. Channel-hopping often indicates an attempt to route around a decline. |
| `mobile_app_count` | Applications from the mobile app. Digital-engagement measure and a soft device-trust anchor. |
| `avg_score_at_decision` | Mean credit score at decision time. Typical creditworthiness across the client's history. |
| `min_score_at_decision` | Floor of credit quality. The worst score ever recorded - the low point a mean smooths away, usually the more predictive figure. |
| `distinct_products` | Distinct product types applied for. Separates a diversified borrower from one concentrated in a single risky product. |
| `has_mortgage` | Ever applied for a mortgage (0/1). Secured-lending engagement, generally a stability and life-stage marker. |
| `has_micro_loan` | Ever applied for a micro loan (0/1). A recognised subprime and short-term-distress marker in this market. |

### `payment_features` - Payment behaviour

`client:{client_id}:payments` · TTL 3600s · `loan-payments` → `loan_payment_events`

Realised repayment discipline - the highest-signal group, because it records what
the client actually did rather than what they asked for. Delinquency is in days
past due (DPD) against the standard 30 / 60 / 90 buckets.

| Feature | What it measures and why it matters |
|---|---|
| `total_payment_events` | All payment-related events on the client's loans. Volume denominator for every ratio below. |
| `on_time_payments` | The primary positive repayment signal. Payments settled at zero days overdue; sustained volume here outweighs most adverse markers elsewhere. |
| `late_payments` | Payments made, but past the due date. Soft delinquency - the debt is serviced, just not on schedule. |
| `missed_payments` | Scheduled payments not made at all. Hard delinquency count over the client's lifetime. |
| `missed_last_90d` | Recent deterioration. Misses in the trailing quarter - materially more predictive of imminent default than a lifetime count that never decays. |
| `total_paid_amount` | Cumulative amount actually paid (KZT). Demonstrated cash-flow servicing capacity. |
| `total_penalties` | Accumulated penalty charges. Monetary cost of the client's own indiscipline; correlates tightly with chronic lateness. |
| `avg_payment_amount` | Mean payment size. The typical instalment being serviced, used to size affordability against income. |
| `max_single_payment` | Largest single payment. Indicates lump-sum or early-settlement capacity beyond the regular schedule. |
| `max_days_overdue` | The classic severity marker. Worst DPD ever reached - the figure most underwriting policies key a decline on. |
| `avg_days_overdue` | Mean DPD across late events only. Separates chronic from acute: a low mean beside a high max is one bad month; a high mean is habitual. |
| `ever_30dpd` | Ever 30+ days past due (0/1). First standard bureau delinquency threshold. |
| `ever_60dpd` | Ever 60+ days past due (0/1). Serious delinquency; typically triggers collections escalation. |
| `ever_90dpd` | The regulatory default definition under most frameworks, and in practice a hard decline on its own. |
| `total_principal_paid` | Principal repaid. Real debt reduction, as distinct from merely servicing interest. |
| `total_interest_paid` | Interest repaid. Revenue contribution - but a high interest-to-principal ratio signals slow amortisation and a client treading water. |
| `last_payment_at` | Timestamp of the most recent payment or partial payment. Recency of active servicing. |
| `days_since_last_payment` | Silence is the signal. A widening gap warns of default before any `MISSED` event is emitted, because it needs no event at all. |
| `late_fee_count` | Late-fee events. Frequency of lateness severe enough to trigger a charge. |
| `penalty_count` | Penalty events. Escalated enforcement beyond a routine late fee. |
| `restructure_count` | Forbearance history. Restructuring resets the delinquency clock and suppresses every DPD feature above it - repeated restructures are adverse precisely because they hide one. |

### `operation_features` - Loan operation & device signals

`client:{client_id}:operations` · TTL 3600s · `loan-operations` → `loan_operation_events`

Loan lifecycle events paired with the device and network context they came from.
This group carries fraud signal rather than credit signal; device fingerprints
arrive SHA-256 hashed and truncated, so they identify without disclosing.

| Feature | What it measures and why it matters |
|---|---|
| `total_operations` | All lifecycle operations on the client's loans. Activity baseline for the group. |
| `disbursement_count` | Loans actually funded. Distinguishes approvals that converted from those that did not. |
| `topup_count` | Revolving dependence. Repeated top-ups mean borrowing further against an existing facility rather than paying it down - a debt spiral in its early form. |
| `early_close_count` | Loans closed ahead of term. A strong credit-quality positive, though it reduces expected interest revenue. |
| `refinance_count` | Refinancing events. Debt restructuring outside formal forbearance, often used to move stress off the books before it registers as delinquency. |
| `write_off_count` | Realised loss. The terminal adverse outcome; any non-zero value is normally decisive on its own. |
| `total_disbursed` | Total principal advanced (KZT). Lifetime exposure actually put at risk. |
| `avg_disbursement` | Mean disbursement size. The client's typical facility. |
| `max_disbursement` | Largest single disbursement. Peak exposure carried at any one point. |
| `distinct_devices` | Device velocity. Distinct hashed fingerprints seen for one client - a high count is a classic account-takeover and synthetic-identity marker. |
| `distinct_device_types` | Variety across `IOS`, `ANDROID`, `WEB`, `POS`. A coarser view of the same signal; a newly appearing type warrants a soft flag. |
| `suspicious_op_count` | Operations the upstream fraud rules flagged. Direct count of rule hits over the client's history. |
| `suspicious_last_30d` | Active fraud pressure, as opposed to historical noise. The trailing-30-day window is what a review queue should key on. |
| `distinct_countries` | Distinct IP countries observed. Geographic dispersion of account access. |
| `foreign_op_count` | Operations originating outside Kazakhstan. For a domestic KZT retail book this is a meaningful anomaly, not routine travel noise. |
| `first_operation_at` | Earliest operation on record. Start of the lending relationship - tenure is itself protective. |
| `last_operation_at` | Most recent operation. Account liveness and profile freshness. |
| `ops_last_30d` | Operations in the trailing month. Current activity intensity; a sudden burst pairs with the suspicious counters to justify review. |

### `money_features` - Client money profile

`client:{client_id}:money` · TTL 7200s · `client-money` → `client_money_events`

Affordability rather than willingness to pay: balances, verified income, and
cash-flow direction. Longest TTL of the four, because balance and salary facts
move on a monthly cycle rather than event by event.

| Feature | What it measures and why it matters |
|---|---|
| `latest_month_end_balance` | Current liquidity position. The most recent month-end balance - the headline affordability figure. |
| `avg_monthly_balance` | Mean month-end balance. Sustained liquidity, immune to a single window-dressed month. |
| `min_monthly_balance` | Liquidity floor. How close to zero the client actually runs - usually more informative than the average, because the trough is what causes a missed instalment. |
| `max_monthly_balance` | Best month-end balance observed. Peak liquidity; brackets the range alongside the floor. |
| `balance_months_tracked` | Confidence weight. How many month-end observations exist - two months of history must not be read like twenty. |
| `total_salary_credits` | Cumulative salary inflow (KZT). Verified income received through the account. |
| `salary_credit_count` | Income regularity. The count matters as much as the sum - steady monthly arrivals evidence stable employment in a way one large sum does not. |
| `avg_salary_credit` | Mean salary credit - the working estimate of monthly income, and the denominator of any debt-to-income calculation. |
| `max_salary_credit` | Largest salary credit. Detects bonuses and annual supplements, and flags a one-off that may be inflating the mean. |
| `last_salary_at` | Employment liveness. A stale last-salary date is one of the earliest job-loss signals, well ahead of any payment miss. |
| `total_transfer_in` | Cumulative incoming transfers. Non-salary inflow - informal income, family support, or undeclared earnings. |
| `total_transfer_out` | Cumulative outgoing transfers. Standing outflow commitments, possibly obligations not disclosed on the application. |
| `large_debit_count` | Large debit events. Frequency of significant outflow shocks against the balance. |
| `large_credit_count` | Large credit events. Unexplained large inflows are a source-of-funds and AML consideration rather than a positive. |
| `max_single_debit` | Largest single outflow. The worst spending shock the account has absorbed. |
| `max_single_credit` | Largest single inflow. Warrants source-of-funds attention when far out of line with the salary profile. |
| `net_flow_last_30d` | Current cash-flow direction. Inflows minus outflows over the trailing month; sustained negative net flow leads a missed payment - it turns before delinquency does. |
| `events_last_30d` | Money events in the trailing month. Near-zero suggests a dormant or secondary account, which invalidates the affordability reading above it. |
| `distinct_source_systems` | Distinct originating banks (`KASPI`, `HALYK`, `FREEDOM`, …). Broad multi-banking means any single lender sees a smaller share of the true picture. |
| `distinct_accounts` | Variety across `CURRENT`, `SAVINGS`, `DEPOSIT`. Depth of the banking relationship; a deposit product is a wealth and stability marker. |

## Stack

Kafka, TiDB, Postgres, Python, Go.
