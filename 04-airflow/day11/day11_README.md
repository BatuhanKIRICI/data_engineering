# Airflow + dbt Integration — Concepts — Day 11

## Project Overview

This project is part of my Data Engineering learning journey. Day 11 covered how Airflow and dbt work together in a single pipeline — and, more importantly, the production concepts (incremental processing, watermarks, idempotency) that connect a "pipeline that runs" to a "pipeline that's safe to run repeatedly."

**Status: concepts covered, hands-on implementation intentionally paused here.** After several days of dense theory (Day 10 and Day 11 both concept-heavy), the plan is to consolidate everything learned so far (Python, SQL, dbt, Airflow, incremental loading, watermarks, idempotency) into one small working pipeline next session, rather than adding more new concepts on top of ones not yet applied hands-on.

## Concepts Covered

### Airflow vs. dbt — division of responsibility
- **Airflow**: decides *when* and *in what order* things run, and what happens on failure (scheduling, dependencies, retries)
- **dbt**: decides *how* data is transformed (the SQL transformation logic itself)
- A typical combined flow: `extract → load → dbt run → dbt test`, each as its own Airflow task
- Critically: `dbt run` succeeding doesn't mean the pipeline succeeded — `dbt test` can still fail on the data `dbt run` produced (e.g. a uniqueness constraint violated), and that failure needs to be visible to Airflow separately from the run itself

### Incremental Processing
Only process new/changed data instead of reprocessing everything on every run (e.g. reprocessing 10 million existing orders every night instead of just the new ones).

### Watermark
A marker of how far a pipeline has successfully progressed (e.g. `last_processed_at`), used to filter incoming data (`WHERE created_at > watermark`).

- **Critical rule**: a watermark should represent "successfully processed," not "seen." Advancing it before confirming success risks silently skipping data that failed to load — that data would never be picked up again by the incremental filter.
- The watermark should only be updated *after* the relevant step succeeds, and only up to the boundary that actually succeeded (a partial failure — e.g. row 103 succeeded, 104 failed — means the watermark advances to 103, not further).
- What a watermark represents has to be defined precisely (e.g. "source successfully loaded into the target") rather than something vague like "pipeline succeeded" — a later step (like dbt) failing doesn't necessarily mean the load itself failed, and the two can be tracked as separate concerns.
- Considered where to store it: not a Python variable (lost when the process ends) or a plain file (fragile, doesn't handle multiple workers) — a small `pipeline_state` table in PostgreSQL is the plan for the consolidation project.

### Idempotency (revisited, now connected to incremental + watermark)
Running the same operation once or multiple times produces the same result. This matters because incremental filters and watermarks are not foolproof on their own — if a boundary condition is off by one (e.g. `>=` instead of `>`), or a retry resends already-processed data, idempotency is what prevents that from corrupting the data instead of just doing harmless extra work.

### A methodology note worth keeping
Not every new topic should be approached the same way. Three different levels call for different teaching/learning approaches:
- **Building blocks already seen before** (JOIN, GROUP BY, CTE, window functions): think it through first, then write it, then review.
- **A new but simple concept**: brief explanation first, then apply it.
- **Genuinely unseen syntax/API** (e.g. `INSERT ... ON CONFLICT ... DO UPDATE`, not seen before this point): show it first, explain what it does, run it, then reuse it independently later — expecting it to be guessed from first principles isn't realistic.

## What I Learned

- Airflow orchestrates *when*, dbt transforms *how* — a combined pipeline chains them as separate tasks (`dbt run` and `dbt test` as distinct steps, not one combined step), since a run succeeding and a test failing are different, separately meaningful outcomes.
- A watermark's precision (row-level ID vs. a coarser date) determines how partially a pipeline can advance after a mixed success/failure run.
- Idempotency and incremental processing solve different problems and depend on each other: incremental processing decides *what* gets reprocessed, idempotency decides *what happens* if something gets reprocessed anyway (by mistake, by retry, or by a boundary error).
- Watermark semantics need to be explicit: "processed" is ambiguous — "source successfully loaded to target" and "fully transformed and tested" are two different boundaries that can be tracked separately.
- Learning something with genuinely new, unseen syntax works better shown first and reasoned about second — trying to derive syntax you've never encountered from pure logic isn't a good use of that exercise.
