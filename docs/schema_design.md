# DiracX Match-making Specification

## Version History

### v0.1

Initial Proposal

## Introduction

This document describes:

* The structure of metadata _**jobs**_ provide for determining resource allocation.
* The structure of metadata _**pilots**_ provide for determining resource allocation.
* How the metadata from _**jobs**_ and _**pilots**_ is compared to find suitable matches.
* How multiple _**jobs**_ are prioritised to then find a single match.

This document does **not** describe:

* How metadata is extracted from jobs or pilots.
* How to efficiently implement this system. Some consideration to this has been given to this when designing this specification, however prototypes should be created before this design is finalised.
* Any consideration to multi-vo. As jobs are per-vo and pilots are per-vo the matching machinery effectively operations as if it's one matcher per VO.

Metadata is generally JSON like however the examples are given as YAML for readability.

## Scale

We expect the DiracX matching infrastructure should be able to support:

* 10,000,000 waiting jobs
* 1KHz of sustained job matching requests
* 1,000 distinct sites
* Several hundred custom tags
* The distribution of job metadata and pilot metadata will be extremely non uniform, i.e. the majority of jobs MAY be similar and the majority of jobs MAY run on a small fraction of the distinct resources.
* A long tail of resources and jobs which are niche.

## Job Matching Criteria

Each job provides one or more matching specifications. The following is an example of such description.

```yaml
site: "SiteA"
system:
    # Different systems may have different keys
    name: Linux
    glibc: 2.17
    user-namespaces: true
wall-time: 86400
cpu-work: 1000000  # Units of DB12 seconds
cpu:
    num-cores: {"min": 1, "max": 128}
    ram-mb:
        request:
            overhead: 512
            per-core: 256
        limit:
            overhead: 768
            per-core: 256
    # Other architectures will have different keys
    # e.g. aarch64 might have arm_version and features (neon, sve, sve2)
    architecture:
        # ==Christophe== we should check whether there is a WLCG convention
        name: x86_64
        microarchitecture-level: {"min": 2, "max": None}
gpu:
    count: {"min": 1, "max": 4}
    ram-mb: 10240  # 10GB per GPU
    # Other vendors might have different keys
    vendor: nvidia
    # ==Chris== will check how to include cuda-version within compute-capability (Nvidia documentation about that)
    compute-capability: {"min": "8.0", "max": None}
io:
    scratch-mb: 12040
    lan-mbitps: 10
    # Could be extended to provide more options such as scratch_iops or wlan_mbitps
# Logical combination of booleans
tags: "cvmfs:lhcb & cvmfs:lhcbdev & (os:el9 | os:ubuntu26) & ~diracx:site:SiteZ"
```

### Examples:

* **Multiple sites:** If a job could run at multiple sites there will be several identical matching specifications which all correspond to the same job.

* **Several supported architectures:** If there are several
* **Banned sites:** Rather than having a top level banned key, the `diracx:site` tag can be used to add an exclusion.

### Considerations:

* Some of the above criteria are optional and may not be enforced depending on the installation configuration.
* If there are multiple nodes the `gpu` requirement is applied on each node (i.e. assuming homogeneous resources)
* The `wall-time` is provided in addition to `cpu-work` to account for jobs which are limited resources other than CPU compute, e.g. GPU or network.

* Definitions of "operating system" are poorly defined and should come from installation specific tags if required.

### Unsupported:

There are several situations which are explicitly not supported. These limitations could be somewhat relaxed on an per-installation basis using the technique in [Edge Cases](#edge-cases).

* Unknown CPU architectures or GPU vendors.
* Heterogeneous CPUs (e.g. performance/efficiency CPU cores).
* Other types of hardware (e.g. FPGAs).
* Distinction between physical and logical processors (e.g. Hyper-threading).

## Pilot Matching Criteria

The majority of pilot matching metadata will come from vanilla DiracX, with some configuration options being available to opt augment it. For further customisation installations can add custom tags (via Python code in the CS?).

```yaml
site: "SiteA"
system:
    # Different systems may have different keys
    name: Linux
    glibc: 2.17
    user_namespaces: true

wall-time: 86400
cpu-work: 1000000  # Units of DB12 seconds
# other numerical value: scratch space...

cpu:
    num-nodes: 1
    num-cores: 16
    ram-mb: 24576
    architecture:
        name: x86_64
        # Other architectures will have different keys
        # e.g. aarch64 might have arm_version and features (neon, sve, sve2)
        microarchitecture-level: 4
gpu:
    count: 0
tags:
    - cvmfs:lhcb
    - cvmfs:lhcbdev
    - os:el9
```

- [name=Alexandre] should we have a mechanism to prevent unhealthy pilots acting as black hole (fetching a large number of jobs and failing them)? Would provide a "healthy score" depending on the `number of failed jobs / number of jobs fetched`.
    - [name=Federico] should be done server side (using a similar mechanism as the JobCommands?)
    - [name=Alexandre] we can open an issue and see how to solve that later

## Comparison of Job and Pilot Metadata

Fundamentally the job matching metadata is a document containing known keys which have the following comparision operations:

* Exact match (e.g. `site == "SiteA"`)
* Range (e.g. `cpu.num-cores.min < 4 < cpu.num-cores.max`)
* Lower limit (e.g.  `cputime > 1e7`)

The use of set operations (e.g. `pilot.site IN job.sites`) is intentionally avoided to avoid difficulties in the implementation.

There are two additional special cases:
* The CPU memory requirements are a function of the number of CPU cores.
* Tags can be combined as an arbitary logical expression of booleans

### Edge Cases

There will always be situations where an installation is trying novel or niche resources that are not handled by this specification. To acccomodate this, installations can provide a custom `tag` to indentify such resources and then provide this tag in their job matching metadata instead of the corrosponding criteria. Examples:

* **New CPU architecture:** To support running jobs on a `riscv` CPU jobs could be submitted without a CPU architecture requirement and the installations pilot is configured to add a `cpu:riscv_cpu_test` tag.
* **Heterogeneous multi-node jobs:** If multi-node jobs are needed a tag could be added like `node:config1` which corresponds to a class of multi-node pilots.

In all cases, if this is a long term need the DiracX specification should be extended accordingly.

## Job Prioritisation

Once the set of eligible jobs is known they must then be prioritised. The available critera are:


* **Job Owner:** Simple round-robin style sharing with no concept of historical tracking for "fair share".
* **Job Group:** Simple round-robin style sharing with no concept of historical tracking for "fair share".
* **Job Type:**
* **Limits:** In some cases we will want to limit the maximum number of running jobs for some criteria (e.g. jobs of a given type at a specific site)

**QUESTION:** Is it good enough to do this by keeping track of something like:

```sql
select Site, JobType, JobGroup, Owner count(*) as n_running
from Jobs
where Status = "Running"
```

In the DiracX installation configuration we then have something like:

```yaml
# ==Chris== need to check it was written this way
job-priorities:
    - JobType1
    - {JobType2: 100, JobType3: 50}
    - JobType4
by-site:
    SiteA:
        runnning-limits:  # JobType 1/2 have fixed limits, default is unlimited
            JobType1: 100
            JobType2: 500
```

If there are multiple equal priority jobs, the final decider is FIFO.

## Other requirements

The source of truth for matching should be in the `JobDB`, though the matching could have an external store of job metadata. If this is the case it should be possible to rebuild this external store of metadata promptly. This may be useful to:

* Recover from a service interuption which casues the external store to be lost. This removes the need for data security in the external store of metadata.
* Ensure the system stays consistent with the JobDB over time.

## Open questions

* Do we want to support fractional CPU to allow overcomitting?
    * Yes
* Should jobs be matched to storage elements as an alternative to sites?
    * [name=Federico] I would say no, as the SE->sites should be already done by the (future) "Optimizers"
    * [name=Chris B] the operational exception I see to that is when we add helpers which is currently painful to then reassign the jobs to a different site. It also imposes a lot of uniformity on a site that may or may not be a good thing.
        * [name=Federico] This exact point was indeed raised and discussed long time ago already. The point is valid but still using also SE jobs matching would impose additional care (clearly it can't be done together with sites-matching). So, I am not convinced.
        * Conclusion: This logic should be in optimizers (as we have now)
* We don't want to support fractional GPUs?
    * [name=Federico] ouf... maybe! ???
    * [name=Chris B] this is extremely messy as its not as simple as the time sharing were acustomed to with CPUs. Maybe we should document a plan for how it could be supported without actually implementing it?
        * [name=Federico] ..probably. I mean, we can't figure out ALL the future requirements, so at some point we should just give up and KISS
        * Conclusion: no we don't want that
* Assume homogeneous resources for multiple nodes?
    * [name=Federico] I don't understand what you mean
    * [name=Chris B] Would we have a multi-node job with 128 riscv cores on one machine, 16 arm cores and 1 nvidia GPU on another and, a third with 8 x86 cores and an AMD GPU.
    * [name=Alexandre] In general I don't think so, different nodes are separated into different partitions in HPCs
    * Conclusion: no we don't want that because no use case for now.
* Can we get rid of rescheduling
    * [name=Federico] by all means!
    * Conclusion: we still want to reoptimize
* Figure out what external interactions need to be considered (e.g. job killing)
* Can we get rid of per-job priority?
    * [name=Federico] yes
    * Conclusion: we will use other mechanism (job type, group, ...)

---

## Implementation Details

### Definition of Done

- Functional prototype showing new matching methods based on Redis
- Performance test suite for the prototype (based on locust)
- Technical documentation (Redis options, lua requests) and comparative analysis
See which Redis characteristics we should rely on (lot of parameters that can be fine tuned)

### Scope

- Prototyping the logic to match jobs with pilots based on Redis primitives, lua
- Performance test suite of the prototype

What's not part of the student job (at least not a priority):
- The generation of the job/pilot criteria
- The CWL aspects


This should be part of a separate repo, at the end of the internship, we can think on how to add the few lines of logic in `diracx-logic`

### Redis

- use a pixi workspace with redis-server and Locust
- we provide 3-5 of job/pilot yaml files: the student focuses on moving them into Redis and try to play with them (add workers to test the conccurency).
- student builds "dummy framework" to test matching pilot yaml to job yaml (simple lookup in python initially)
    - building functional tests
- student puts jobs into redis and write lua to do matching
    - start with a dumb for loop over matching specs
- start testing scale
- start introducing rate limits/round-robin


### Filling redis (probably a later project)

- replaces the current optimisers
- needs the DFC and CWL to be known
