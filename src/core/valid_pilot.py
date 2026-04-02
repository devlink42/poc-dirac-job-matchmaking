import yaml
from pydantic import ValidationError

from src.models.job import Job
from src.models.node import Node


def valid_pilot(job, pilot) -> bool:
    yaml_job = yaml.safe_load(job)
    yaml_node = yaml.safe_load(pilot)

    jobs = yaml_job["matching_specs"]

    for job in jobs:
        try:
            Job.model_validate(job)
        except ValidationError as e:
            raise ValidationError(f"Invalid job specification: {e}") from e

    try:
        Node.model_validate(yaml_node)
    except ValidationError as e:
        raise ValidationError(f"Invalid node specification: {e}") from e

    return True
