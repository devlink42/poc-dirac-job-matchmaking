# Matching Examples

## Production Data Summary

| Dimension | Key insight |
|-----------|------------|
| **Job types** | 3 types dominate: MCSimulation (~60%), MCFastSimulation (~21%), WGProduction (~16%). Long tail of 8+ small types. |
| **Owners** | Extremely skewed: `lbprods/lhcb_mc` owns ~80% of jobs, `lbprods/lhcb_data` ~18%, rest < 2%. |
| **Sites** | Relatively flat: top 20 sites each have ~650K-750K eligible jobs. 48 sites cover 80% (Pareto). ~60 total sites. |
| **CPU time** | Peak at 3 days (~330K jobs), secondary peaks at 4d, 12.5d, and 6h. |
| **Platform** | Mostly x86_64. Top platforms (v4-any, broadwell, skylake, cascadelake, haswell) all ~750K jobs each. |
| **RAM** | Most jobs in production currently have no RAM requirement, but this will change -- all jobs should specify RAM in the future. |
| **TaskQueues** | ~530 TQs total. Most jobs are in large TQs (1001-5000 jobs). |

## Job Examples

| File | Type | Owner/Group | CPU time | Cores | GPU | RAM | Sites | Tags | Notes |
|------|------|-------------|----------|-------|-----|-----|-------|------|-------|
| `job_01_*` | MCSimulation | lbprods/lhcb_mc | 3d | 1 | No | Yes (1.5 GB overhead) | **Any** | Simple | The most common case -- no site restriction |
| `job_02_*` | MCSimulation | lbprods/lhcb_mc | 3d | 1 | No | Yes (1.5 GB + 256/core) | 5 specific sites | OR tags | Multi-site eligibility (optimizer-assigned) |
| `job_03_*` | MCFastSimulation | lbprods/lhcb_mc | 6h | 1 | No | No | 3 sites | Simple | Short jobs, low arch requirement (min v1) |
| `job_04_*` | WGProduction | lbprods/lhcb_data | 12.5d | 4-8 | No | Yes (2 GB + 512/core) | 2 sites | AND tags | Multi-core with per-core RAM scaling |
| `job_05_*` | User | sharmar/lhcb_user | 1d | 1 | No | No | 3 sites | NOT tag | Banned site via tag negation |
| `job_06_*` | MCSimulation | lbprods/lhcb_mc | 12h | 1-4 | **Yes** | Yes (4 GB + 1 GB/core) | 1 (CERN) | Complex | Future GPU workload |
| `job_07_*` | Sprucing | lbprods/lhcb_data | 2d | 1-2 | No | No | 1 (CERN) | AND tags | Niche long-tail type, with IO scratch |

## Node Examples

| File | Site | Arch level | Cores | RAM | GPU | Wall time | Notes |
|------|------|------------|-------|-----|-----|-----------|-------|
| `pilot_01_*` | LCG.CERN.cern | v4 | 16 | 24 GB | No | 3d | Should match most jobs |
| `pilot_02_*` | LCG.NCBJ.pl | v2 | 8 | 16 GB | No | 1d | Older node, matches fewer jobs |
| `pilot_03_*` | LCG.CERN.cern | v4 | 8 | 32 GB | A100 | 2d | GPU node, matches GPU + CPU jobs |

## Expected Match Matrix

This matrix shows which (job, node) pairs should match. Use this to validate your implementation.

| | pilot_01 (CERN v4, 3d) | pilot_02 (NCBJ v2, 1d) | pilot_03 (CERN GPU, 2d) |
|---|:---:|:---:|:---:|
| **job_01** (MCSim, any site) | **YES** | NO (wall-time 1d < 3d, arch v2 < v4) | NO (wall-time 2d < 3d) |
| **job_02** (MCSim, 5 sites) | **YES** | NO (site not in list) | NO (wall-time 2d < 3d) |
| **job_03** (MCFast, 3 sites) | **YES** | **YES** | **YES** |
| **job_04** (WGProd, RAM) | **YES** | NO (site not in list) | NO (wall-time 2d < 12.5d) |
| **job_05** (User, banned) | **YES** | NO (site not in list) | **YES** |
| **job_06** (GPU) | NO (no GPU) | NO (site not in list, no GPU) | **YES** |
| **job_07** (Sprucing) | **YES** | NO (site not in list) | **YES** |

**Summary:** pilot_01 matches 5/7 jobs, pilot_02 matches 1/7, pilot_03 matches 5/7.

### Key things to verify
- **No site = any site**: job_01 has no `site` field, so site filtering is skipped -- only other criteria matter
- **Site filtering**: pilot_02 only matches jobs that have a spec for LCG.NCBJ.pl (or no site restriction)
- **Architecture filtering**: v2 pilot cannot run v4-requiring jobs (job_01 requires min v4, fails on pilot_02)
- **Wall-time**: pilot must offer >= job's wall-time (job_01 and job_02 need 3d, pilot_03 only offers 2d)
- **RAM computation**: job_04 at 4 cores needs 2048 + 512x4 = 4096 MB request, pilot_01 has 24 GB -> OK
- **GPU matching**: only pilot_03 can serve job_06; pilot_01 and pilot_02 have gpu.count: 0
- **Tag negation**: job_05 bans LCG.NIPNE-07.ro via `~diracx:banned:LCG.NIPNE-07.ro`
- **Boundary**: job_07 needs exactly 2d wall-time, pilot_03 offers exactly 2d -> match (>=)

## Invalid Examples (validation tests)

These files should all be **rejected** by the data models during loading or validation.

| File | Location | What's wrong | Expected error |
|------|----------|-------------|----------------|
| `invalid_01_*` | jobs/ | `cpu.num-cores`: min (8) > max (2) | Range validation |
| `invalid_02_*` | jobs/ | `wall-time`: negative value (-3600) | Positive number required |
| `invalid_03_*` | jobs/ | Missing `system` field in matching spec | Required field missing |
| `invalid_04_*` | jobs/ | Unknown CPU architecture (`riscv64`) | Unsupported architecture |
| `invalid_05_*` | jobs/ | Empty `matching_specs` list | At least one spec required |
| `invalid_06_*` | jobs/ | Negative GPU count (min: -1) | Positive number required |
| `invalid_07_*` | nodes/ | Pilot with negative core count (-4) | Positive number required |
| `invalid_08_*` | jobs/ | `wall-time` as string ("three days") | Wrong type (expected integer) |
