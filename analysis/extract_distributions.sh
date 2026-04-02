#!/usr/bin/env bash
# Extract TaskQueue distribution data from production MySQL using mycli.
#
# Usage:
#   ./extract_distributions.sh <mycli_connection_args>
#
# Examples:
#   ./extract_distributions.sh -h dbhost -u user -p password -D TaskQueueDB
#   ./extract_distributions.sh --login-path=production -D TaskQueueDB
#
# Output: CSV files in ./data/ directory

set -euo pipefail

if [ $# -eq 0 ]; then
    echo "Usage: $0 <mycli connection args>"
    echo "Example: $0 -h dbhost -u user -p password -D TaskQueueDB"
    exit 1
fi

MYCLI_ARGS="$@"
OUTDIR="$(dirname "$0")/data"
mkdir -p "$OUTDIR"

run_query() {
    local name="$1"
    local sql="$2"
    echo "Extracting: $name ..."
    mycli $MYCLI_ARGS --csv -e "$sql" > "$OUTDIR/${name}.csv" 2>/dev/null
    echo "  -> $OUTDIR/${name}.csv ($(wc -l < "$OUTDIR/${name}.csv") rows)"
}

# 1. Overall scale numbers
run_query "scale_overview" "
SELECT
  (SELECT COUNT(*) FROM tq_TaskQueues WHERE Enabled = 1) as active_tqs,
  (SELECT COUNT(*) FROM tq_TaskQueues) as total_tqs,
  (SELECT COUNT(*) FROM tq_Jobs) as waiting_jobs,
  (SELECT COUNT(DISTINCT Value) FROM tq_TQToSites) as distinct_sites,
  (SELECT COUNT(DISTINCT Value) FROM tq_TQToTags) as distinct_tags,
  (SELECT COUNT(DISTINCT Value) FROM tq_TQToPlatforms) as distinct_platforms,
  (SELECT COUNT(DISTINCT Value) FROM tq_TQToJobTypes) as distinct_job_types;
"

# 2. Jobs per TaskQueue
run_query "jobs_per_tq" "
SELECT tq.TQId, tq.CPUTime, tq.OwnerGroup, tq.Priority, tq.Enabled,
       COUNT(j.JobId) as n_jobs
FROM tq_TaskQueues tq
JOIN tq_Jobs j ON tq.TQId = j.TQId
GROUP BY tq.TQId, tq.CPUTime, tq.OwnerGroup, tq.Priority, tq.Enabled
ORDER BY n_jobs DESC;
"

# 3. Site distribution
run_query "site_distribution" "
SELECT s.Value as site, COUNT(DISTINCT s.TQId) as n_tqs,
       COALESCE(SUM(jc.n_jobs), 0) as total_jobs
FROM tq_TQToSites s
LEFT JOIN (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
  ON s.TQId = jc.TQId
GROUP BY s.Value
ORDER BY total_jobs DESC;
"

run_query "site_any" "
SELECT COUNT(*) as tqs_any_site,
       COALESCE(SUM(jc.n_jobs), 0) as jobs_any_site
FROM tq_TaskQueues tq
LEFT JOIN tq_TQToSites s ON tq.TQId = s.TQId
LEFT JOIN (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
  ON tq.TQId = jc.TQId
WHERE s.TQId IS NULL;
"

# 4. Banned sites
run_query "banned_sites" "
SELECT bs.Value as banned_site, COUNT(DISTINCT bs.TQId) as n_tqs,
       COALESCE(SUM(jc.n_jobs), 0) as total_jobs
FROM tq_TQToBannedSites bs
LEFT JOIN (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
  ON bs.TQId = jc.TQId
GROUP BY bs.Value
ORDER BY total_jobs DESC;
"

# 5. Platform distribution
run_query "platform_distribution" "
SELECT p.Value as platform, COUNT(DISTINCT p.TQId) as n_tqs,
       COALESCE(SUM(jc.n_jobs), 0) as total_jobs
FROM tq_TQToPlatforms p
LEFT JOIN (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
  ON p.TQId = jc.TQId
GROUP BY p.Value
ORDER BY total_jobs DESC;
"

# 6. Tag distribution
run_query "tag_distribution" "
SELECT t.Value as tag, COUNT(DISTINCT t.TQId) as n_tqs,
       COALESCE(SUM(jc.n_jobs), 0) as total_jobs
FROM tq_TQToTags t
LEFT JOIN (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
  ON t.TQId = jc.TQId
GROUP BY t.Value
ORDER BY total_jobs DESC;
"

# 7. CPU time segments
run_query "cputime_distribution" "
SELECT tq.CPUTime as cpu_segment_seconds,
       COUNT(DISTINCT tq.TQId) as n_tqs,
       COALESCE(SUM(jc.n_jobs), 0) as total_jobs
FROM tq_TaskQueues tq
LEFT JOIN (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
  ON tq.TQId = jc.TQId
GROUP BY tq.CPUTime
ORDER BY tq.CPUTime;
"

# 8. RAM requirements
run_query "ram_distribution" "
SELECT r.MinRAM, r.MaxRAM, COUNT(DISTINCT r.TQId) as n_tqs,
       COALESCE(SUM(jc.n_jobs), 0) as total_jobs
FROM tq_RAM_requirements r
LEFT JOIN (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
  ON r.TQId = jc.TQId
GROUP BY r.MinRAM, r.MaxRAM
ORDER BY total_jobs DESC;
"

run_query "ram_none" "
SELECT COUNT(*) as tqs_no_ram,
       COALESCE(SUM(jc.n_jobs), 0) as jobs_no_ram
FROM tq_TaskQueues tq
LEFT JOIN tq_RAM_requirements r ON tq.TQId = r.TQId
LEFT JOIN (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
  ON tq.TQId = jc.TQId
WHERE r.TQId IS NULL;
"

# 9. JobType distribution
run_query "jobtype_distribution" "
SELECT jt.Value as job_type, COUNT(DISTINCT jt.TQId) as n_tqs,
       COALESCE(SUM(jc.n_jobs), 0) as total_jobs
FROM tq_TQToJobTypes jt
LEFT JOIN (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
  ON jt.TQId = jc.TQId
GROUP BY jt.Value
ORDER BY total_jobs DESC;
"

# 10. GridCE distribution
run_query "gridce_distribution" "
SELECT g.Value as grid_ce, COUNT(DISTINCT g.TQId) as n_tqs,
       COALESCE(SUM(jc.n_jobs), 0) as total_jobs
FROM tq_TQToGridCEs g
LEFT JOIN (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
  ON g.TQId = jc.TQId
GROUP BY g.Value
ORDER BY total_jobs DESC;
"

# 11. TQ size distribution (histogram of jobs-per-TQ)
run_query "tq_size_histogram" "
SELECT
  CASE
    WHEN n_jobs = 1 THEN '1'
    WHEN n_jobs <= 10 THEN '2-10'
    WHEN n_jobs <= 100 THEN '11-100'
    WHEN n_jobs <= 1000 THEN '101-1000'
    WHEN n_jobs <= 5000 THEN '1001-5000'
    ELSE '5000+'
  END as size_bucket,
  COUNT(*) as n_tqs,
  SUM(n_jobs) as total_jobs
FROM (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
GROUP BY size_bucket
ORDER BY MIN(n_jobs);
"

# 12. Owner distribution (top 20)
run_query "owner_distribution" "
SELECT tq.Owner, tq.OwnerGroup,
       COUNT(DISTINCT tq.TQId) as n_tqs,
       COALESCE(SUM(jc.n_jobs), 0) as total_jobs
FROM tq_TaskQueues tq
LEFT JOIN (SELECT TQId, COUNT(*) as n_jobs FROM tq_Jobs GROUP BY TQId) jc
  ON tq.TQId = jc.TQId
GROUP BY tq.Owner, tq.OwnerGroup
ORDER BY total_jobs DESC
LIMIT 30;
"

# 13. Running jobs distribution (from JobDB if accessible)
run_query "running_by_site" "
SELECT Site, COUNT(*) as n_running
FROM Jobs
WHERE Status = 'Running'
GROUP BY Site
ORDER BY n_running DESC;
" 2>/dev/null || echo "  (skipped - Jobs table not accessible)"

echo ""
echo "Done. CSV files written to $OUTDIR/"
echo "Run: python3 plot_distributions.py to generate charts."
