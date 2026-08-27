#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path
from typing import Self

import yaml
from pydantic import BaseModel


class YamlLoadableModel(BaseModel):
    """Base model that provides functionality to instantiate a Pydantic model
    directly from a YAML configuration file.
    """

    @classmethod
    def load_from_yaml(cls, path: str | Path) -> Self:
        """Load and apply the configuration from a YAML file.

        Args:
            path: The file system path to the YAML configuration file.

        Returns:
            An instance of the class calling this method.

        Raises:
            FileNotFoundError: If the specified file path does not exist.
        """
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"No such file or directory: '{file_path}'")

        with open(file_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return cls.model_validate(data or {})
