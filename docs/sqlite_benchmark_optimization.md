# SQLite Benchmark Candidate Loading

## Critical-path analysis

The former benchmark path generated up to 800,000 distinct random identifiers,
split them into 32,000-element batches, and executed one `IN` query per batch.
At the default size, one matchmaking cycle therefore required:

* one large `random.sample` allocation;
* 25 SQL statements with dynamically generated placeholders;
* 800,000 bound parameters;
* a temporary `rows` list before Pydantic deserialization;
* a full Python matching and ranking pass over all deserialized jobs.

This is not an ORM N+1 query, but it has the same round-trip amplification in
the hot path. Locust excludes SQL and JSON parsing from the reported request
latency, but each simulated user still performs that work serially, so it
directly limits achieved requests per second.

## Implemented query and lifecycle

Benchmark jobs are generated with dense `INTEGER PRIMARY KEY` values. A random
circular keyset window therefore provides the requested candidate cardinality
without a large `IN` list. If the window crosses the end of the configured
pool, the second range continues at identifier 1.

```sql
WITH candidate_window AS (
    SELECT data, id, 0 AS window_segment
    FROM jobs
    WHERE id BETWEEN :start_id AND :first_range_end
    UNION ALL
    SELECT data, id, 1 AS window_segment
    FROM jobs
    WHERE id BETWEEN 1 AND :second_range_end
)
SELECT data
FROM candidate_window
ORDER BY window_segment, id;
```

This is one prepared statement with three scalar parameters. Both branches use
range searches on the table's integer primary key. `ORDER BY window_segment, id`
makes the circular order deterministic: the tail of the pool always precedes
the wrapped range. `UNION ALL` is intentional because the ranges cannot
overlap, so duplicate elimination would only add work beyond this required
ordering.

The selected window is read and validated once during Locust's `test_start`
event, then reused by every matchmaking task. This is required for metric
consistency: code executed before `events.request.fire` is excluded from the
reported response time but still consumes wall-clock time and therefore lowers
Locust's requests per second. Loading a new window before every event made
latency describe `select_job` while throughput described SQL, Pydantic parsing,
and `select_job` together.

The indexed selector changes the selected in-memory job to `RUNNING` and updates
its running counters atomically. The benchmark returns that job through the
selector after each invocation, which has the same reset semantics as loading
fresh records for every cycle without repeating database and model validation
work. Every call still operates on exactly `--num-jobs` candidate jobs.

The random window changes the sampling strategy from independently sampled
identifiers to a random contiguous sample. The generated benchmark records are
independent and identically distributed, so the window remains representative
while avoiding parameter and memory amplification.

## Index recommendations

For the current benchmark schema, no additional index should be created for the
implemented query. In SQLite, this declaration:

```sql
id INTEGER PRIMARY KEY
```

makes `id` an alias of `rowid`; it is already the clustered lookup key. An
additional `CREATE INDEX ... ON jobs(id)` would duplicate data, slow writes,
and not improve this query.

If lookups by the external identifier become part of the workload, add the
following independent index:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_job_id ON jobs(job_id);
```

That index does not accelerate the candidate-window query. For a production
matcher, frequently filtered attributes such as status, site, architecture,
and resource ranges should first be normalized into typed columns. Composite
or partial indexes can then follow measured predicates; for example, after a
schema migration introducing `status`:

```sql
CREATE INDEX IF NOT EXISTS idx_jobs_waiting_id
ON jobs(id)
WHERE status = 'WAITING';
```

Do not create speculative JSON-expression or compatibility indexes before the
production schema and real `EXPLAIN QUERY PLAN` evidence are available.

## In-memory selection index

Evaluating 800,000 jobs per request cannot meet a 1 kHz target because it would
require 800 million compatibility checks per second. `IndexedJobSelector`
instead partitions the pool once by job type, group, and owner and sorts each
leaf queue by submission time. It also maintains running counters by group,
owner, and site/type.

For each request, the selector visits the FIFO head candidates in exact
fair-share order and stops at the first compatible job per owner. It preserves
the existing priority, weighted-priority, site-limit, fair-share, and FIFO
semantics without materializing a filtered list or sorting the full pool.
Selection and release share a lock, so assignment and counter updates are
atomic within the process.

This changes the common-case request cost from `O(candidate_count)` matching
plus `O(matches * log(matches))` sorting to a cost based on the small number of
group/owner queues and incompatible FIFO heads. The worst case remains linear
when no jobs match; a production database or Redis implementation should add
normalized eligibility indexes for that case.

## Reproducible result

The following benchmark was run with the existing database containing
10,000,000 jobs and 50,000 nodes:

```text
pixi run benchmark -u 100 -r 100 -t 75s \
  --num-nodes 50000 \
  --num-jobs 800000 --log-level ERROR
```

The result was 553,690 successful requests, zero failures, 7,411.55 requests
per second, 0.0319 ms median latency, and 0.0545 ms average latency. Locust's
75-second aggregate includes approximately 40 seconds spent loading,
validating, and indexing 800,000 jobs before users start; the active phase was
typically between 15,000 and 17,000 requests per second. The benchmark uses no
artificial user wait time because the target is scheduler saturation throughput.