# Airflow — Production Thinking (Idempotency, Task Granularity, Logging/Monitoring) — Day 10

## Project Overview

This project is part of my Data Engineering learning journey. Day 10 was a deliberately code-free, concept-only day — the goal was to build the *thinking* a Data Engineer applies to a pipeline before writing or reviewing any code: "will this break if it runs twice? where would it fail? how would I know?"

Unlike other days, there's no script or DAG file here. The output of today is a mental framework, tested against real scenarios below.

## Core Question of the Day

> "Pipeline works" and "pipeline is production-ready" are not the same thing.

A pipeline that runs successfully once can still be fragile — it might duplicate data on retry, fail silently, or be impossible to debug at 3am when something breaks.

## Concepts Covered

### 1. Idempotency
Running the same operation once or five times produces the same final result.

- `SET balance = 100` → idempotent (always ends at 100)
- `balance = balance + 100` → **not** idempotent (accumulates on every run)
- Uncontrolled `INSERT` → **not** idempotent (duplicates the row each run)
- `INSERT ... ON CONFLICT (id) DO UPDATE` → idempotent **only if** the conflict key is a genuine, stable identity for the record, and the UPDATE itself assigns a value rather than accumulating one

**The harder problem:** what if a source system generates a *different* ID for the same real-world event on retry (e.g. after a network failure)? Two answers:
- **Idempotency keys**: the calling system (not the server) generates a stable key and resends the same one on every retry — this is the real production fix, used by some payment APIs as an example, but it depends on the source system being designed this way.
- **Heuristic/fuzzy matching**: if the source doesn't provide this, comparing other fields (customer, amount, timestamp window) can flag *likely* duplicates — but this is probabilistic, not certain, and shouldn't silently delete records without review.

The underlying question a Data Engineer has to keep asking: **"What is the true identity of this record?"** — not "do all the columns match."

### 2. Task Granularity
How many tasks should a pipeline be split into, and how much should each one do?

- Too coarse (one giant task): a failure tells you nothing about *where* it failed.
- Too fine (a task per line of code): overhead and noise, harder to manage.
- The right level: **a task is a meaningful, independent, observable unit of work.**

Test questions for where to draw the boundary: does this step have its own clear purpose? would I want to see it fail separately from other steps? does retrying it in isolation make sense? can it run independently of other steps?

### 3. Logging vs. Monitoring vs. Alerting
Three related but distinct concepts, often confused:

| Concept | Answers | Example |
|---|---|---|
| **Logging** | What happened, in detail? | `Rows read: 1,000,000`, `Rows rejected: 12,431`, `ERROR: connection timed out` |
| **Monitoring** | What's the current state? | `extract 🟢 clean 🟢 load 🔴` |
| **Alerting** | Something's wrong — notify someone | 🚨 Slack/email: "daily_orders_pipeline failed" |

Good logging is selective — log what helps diagnose a problem (row counts, rejected records, duration, source file), not noisy step-by-step variable dumps.

## Closing Test — Mapping a Real Scenario

Given a nightly pipeline (`extract → clean → deduplicate → load → dbt`) with five requirements, matching each to the concept(s) that address it:

| Requirement | Concept(s) |
|---|---|
| Network drops sometimes; failed tasks should retry | Retry + Idempotency |
| Retries shouldn't cause duplicate order writes | Idempotency |
| An engineer should be able to tell where and why it failed | Logging + Monitoring (+ Alerting) |
| If `clean` fails, `load` must not run | Task dependency (not alerting — the pipeline stops itself via the `>>` dependency graph, independent of whether anyone is notified) |
| Don't reprocess all 10M orders every night, only new ones | Incremental processing/loading (a new term for later — watermarks, timestamps, CDC) |

The one worth remembering: **a dependency graph (`clean >> load`) is what stops downstream work from running on a failed upstream step — alerting is a separate concern that only notifies someone, it doesn't prevent anything from executing.**

## What I Learned

- Idempotency, retries, task granularity, and logging/monitoring/alerting are four separate concepts that work together — a pipeline can have great logging and still be dangerous to retry if it isn't idempotent.
- The real identity of a record for deduplication purposes is a business decision, not something a Data Engineer can assume from column values alone — it should come from the source system's actual guarantees (like an idempotency key), with heuristic matching as a fallback, not a first choice.
- Task dependency (Airflow's `>>`) and alerting solve different problems: dependency controls *what runs*, alerting controls *who finds out*. Confusing the two is an easy mistake to make.
- Not every production concept needs code to be understood first — some (like idempotency and incremental loading) are worth understanding conceptually before ever seeing the implementation, since the concept explains *why* the code will eventually look the way it does.
