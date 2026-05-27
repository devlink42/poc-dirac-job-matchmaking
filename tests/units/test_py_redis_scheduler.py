# #!/usr/bin/env python3
#
# from __future__ import annotations
#
# from unittest.mock import MagicMock
#
# from matchmaking.core.py_redis.match_making import PY_REDIS_JOB_KEY
# from matchmaking.core.py_redis.scheduler import fetch_candidate_jobs, select_job_from_redis
#
#
# def _fake_redis(job_ids, raw_jobs):
#     """Build a Redis mock returning the given ids/payloads for sampling."""
#     client = MagicMock()
#     client.hrandfield.return_value = job_ids
#     client.hmget.return_value = raw_jobs
#
#     return client
#
#
# def test_fetch_candidate_jobs_empty_pool_returns_empty():
#     client = _fake_redis([], [])
#
#     assert fetch_candidate_jobs(client, 10) == []
#     client.hmget.assert_not_called()
#
#
# def test_fetch_candidate_jobs_skips_missing_and_malformed(load_job):
#     job = load_job("job_01_mcsimulation_any_site")
#     client = _fake_redis(["a", "b", "c"], [job.model_dump_json(), None, "{not-json"])
#
#     fetched = fetch_candidate_jobs(client, 3)
#
#     assert len(fetched) == 1
#     assert fetched[0].job_id == job.job_id
#     client.hrandfield.assert_called_once_with(PY_REDIS_JOB_KEY, 3)
#
#
# def test_select_job_from_redis_no_candidates_returns_none(load_node):
#     node = load_node("node_01_cern_typical")
#     client = _fake_redis([], [])
#
#     assert select_job_from_redis(node, client, candidate_jobs_count=5) is None
#
#
# def test_select_job_from_redis_no_compatible_returns_none(example_config, load_job, load_node):
#     job = load_job("job_04_wgproduction_with_ram")
#     node = load_node("node_01_cern_typical")
#     client = _fake_redis(["job_04"], [job.model_dump_json()])
#
#     assert select_job_from_redis(node, client, candidate_jobs_count=5, config=example_config) is None
#
#
# def test_select_job_from_redis_returns_selected_job(example_config, load_job, load_node):
#     job = load_job("job_01_mcsimulation_any_site")
#     node = load_node("node_01_cern_typical")
#     client = _fake_redis(["job_01"], [job.model_dump_json()])
#
#     selected = select_job_from_redis(node, client, candidate_jobs_count=5, config=example_config)
#
#     assert selected is not None
#     assert selected.job_id == job.job_id
