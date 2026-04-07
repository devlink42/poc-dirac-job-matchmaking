import yaml
from pydantic import ValidationError

from src.models.job import Job
from src.models.node import Node


def valid_job_with_node(job: Job, node: Node) -> bool:
    # 1. System check
    if job.system.name != node.system.name:
        return False

    if job.system.glibc is not None and node.system.glibc < job.system.glibc:
        return False

    if job.system.user_namespaces is not None and job.system.user_namespaces != node.system.user_namespaces:
        return False

    # 2. Architecture check
    if job.cpu.architecture.name != node.cpu.architecture.name:
        return False

    if node.cpu.architecture.microarchitecture_level < job.cpu.architecture.microarchitecture_level.min:
        return False

    if job.cpu.architecture.microarchitecture_level.max is not None and node.cpu.architecture.microarchitecture_level > job.cpu.architecture.microarchitecture_level.max:
        return False

    # 3. CPU Cores check
    if node.cpu.num_cores < job.cpu.num_cores.min:
        return False

    # 4. RAM check
    if job.cpu.ram_mb:
        required_ram_request = job.cpu.ram_mb.request.overhead or 0

        if job.cpu.ram_mb.request.per_core:
            required_ram_request += job.cpu.ram_mb.request.per_core * node.cpu.num_cores

        if node.cpu.ram_mb < required_ram_request:
            return False

    # 5. Wall-time and CPU work
    if node.wall_time < job.wall_time:
        return False

    if node.cpu_work < job.cpu_work:
        return False

    # 6. GPU check (if required)
    if job.gpu:
        if node.gpu.count < job.gpu.count.min:
            return False
        if job.gpu.count.max is not None and node.gpu.count > job.gpu.count.max:
            return False
    elif node.gpu.count > 0:
        pass

    # 7. Tags check (all job tags must be present in node tags)
    if job.tags:
        job_tags = set(tag.strip() for tag in job.tags.replace(',', ' ').split())
        node_tags = set(node.tags)

        if not job_tags.issubset(node_tags):
            return False

    return True


def valid_pilot(job: str, pilot: str) -> list[Job]:
    yaml_job = yaml.safe_load(job)
    yaml_node = yaml.safe_load(pilot)

    try:
        node_obj = Node.model_validate(yaml_node)
    except ValidationError as e:
        print(f"Invalid node specification: {e}")
        return []

    jobs = yaml_job.get("matching_specs", [])
    jobs_match = []

    for job_spec in jobs:
        try:
            job_obj = Job.model_validate(job_spec)

            if valid_job_with_node(job_obj, node_obj):
                jobs_match.append(job_obj)
        except ValidationError as e:
            print(f"Invalid job specification: {e}")
            continue

    return jobs_match


def main(*args, **kwargs):
    pass


if __name__ == "__main__":
    main()
