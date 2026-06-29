# Changelog

## [0.1.1](https://github.com/devlink42/poc-dirac-job-matchmaking/compare/v0.1.0...v0.1.1) (2026-06-29)


### Features

* add argparse CLI for job and node validation with enhanced fallback logic ([399b1a2](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/399b1a21816e94a73f8acf179540e1178aa13ebb))
* add comprehensive test coverage for CLI, models, and validation functions ([0956ee5](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/0956ee577de8e9a69b6bcd475fe62e13ae35f41d))
* add configurable logging to CLI with log level argument ([282e38e](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/282e38e01910547050ac7b98ae497eb8dd0ceec1))
* add detailed debugging logs to job-node matching process for better traceability ([b909d21](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/b909d21f1e003f0265e4aacccb7d71f518634c14))
* add driver version validation for CPU and GPU matching ([5f847f1](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/5f847f1026480dafeed34b9817d510ab70db4162))
* add enums for job metadata, enhance typing in models, and update dependencies ([4fe220c](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/4fe220ce66ca7d6089c33e2e63d8f3817e365087))
* add Job model and update dependencies ([f09ab1c](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/f09ab1c9ffe2d2fa11324230cbf40a942f758108))
* add job scheduling logic, update configs, and improve pre-commit setup ([8b69177](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/8b69177fbddb783b6a56aac72b78aa866f0d2e9d))
* add new pilots and jobs with updated specifications and validation fixes ([3f89819](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/3f8981904169659830650164188e60a1fa675486))
* add Pixi package manager configuration and environment files ([3751902](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/37519027b36d8c95008b46c02f8b12636e5d7404))
* add pre-commit ([5d89562](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/5d895625fbb1762ec89420ed8aa92e08584c102e))
* add RAM testing jobs and update match making logic ([732fd32](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/732fd3242c7429da4bdf51580a7c2b8964299968))
* add scheduling config loader, YAML example, and improve coverage reporting ([6ac68ec](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/6ac68ecbabf3c0e72fcd91c9370b365eb44902b3))
* add tests for edge cases in valid_pilot and CLI validation functions ([1b82e66](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/1b82e667d9e87f66e4f6b581fd724a5eac4e204b))
* add utility models and extend Job and Node with stricter typing and defaults ([2e1c733](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/2e1c733413e05d5216a3686c83a5a7f63043941f))
* add valid_pilot function for job and node validation ([04853d5](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/04853d5646b08c193499bd983453647907526349))
* configure logger for tests, update lint tasks, and enhance error logging ([f824cb4](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/f824cb4e6bb48ce1b8ca01b7b3aca1cba41aea00))
* enhance GPU matching logic, add version validation and extend model support ([c0d539b](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/c0d539b0fe72bbad412e8ede1e3f03b6beba9156))
* enhance job-node validation, CI workflow, and test coverage with linting and coverage reports ([8f05a80](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/8f05a807d64a94343d0b42679ac2f3d414ff96d5))
* enhance pyproject tasks with new lint commands and expanded test coverage reporting ([6b25655](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/6b25655e27395d91ae772a110689f301580d1c59))
* enhance validation in Job and Node models with stricter typing and aliases ([8d9a6d2](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/8d9a6d269588d72ce49dbac785d6354f74b24be1))
* expand test coverage for valid_pilot with additional parameterized cases ([b6f4569](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/b6f4569c25c19d843430f303cbb64847b43c4dd6))
* extend Job model and add Node model ([ba9fa52](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/ba9fa52a694ea2ee1c4053d7c76332d0de1e8dae))
* Implement job prioritization logic ([21c85b1](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/21c85b18442742ffc3c271476dff609f8fa59dc7))
* improve job-node validation logic, CLI usability, and logging feedback ([eb28847](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/eb288473bce5237b4071f7208c87fc546ef9da5c))
* improve job-node validation logic, CLI usability, and logging feedback ([953bed3](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/953bed3958274ba8f749e9996cbaba88aa63c197))
* improve validation messages and add exhaustive job-node compatibility tests ([cbdf66e](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/cbdf66e3531669dddecd525c72b7744a808ecc88))
* improve validation, typing, and CLI functionality across models and tests ([7cdd5f8](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/7cdd5f8ac3064ee4a5a3e98985c52de79eb999b5))
* refine job and node models with optional IDs and enhance job-node validation logic ([3f1f09b](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/3f1f09b8e16d93dd7d5ffffffccd23e594fd49ee))
* support boolean tag expressions for job-node matching ([0e8d08a](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/0e8d08a653484ee5bdd263281238ddd2d51372f9))
* update dependencies and expand test cases for job-node matching validation ([85543b2](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/85543b2443aa5ed884876224496fe14b91cf68e6))
* update pilot_04 config and README for adjusted resources and validation ([a829898](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/a8298981567a5d347e12a67fa19799cb4becf821))


### Bug Fixes

* add check validator gpu ([497fd56](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/497fd56dfef0440793d3b5a89b96d3653b8dfd42))
* correct regex for tag expression parsing and update test expectations ([b397ffa](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/b397ffa4b0a8d3373a44cb72c181cd7d297f3ae8))
* improve select_job, split in function, add consistency for Job ([98997bd](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/98997bd40594959f37654bc3aa5c05ed8015077e))
* prerelease CI ([0ff8752](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/0ff87526152e9b6873481ae3de698d98b1c9b1ef))
* remove `Returns:` from docstring ([7818c96](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/7818c962ea037d97fd1072960c61ec94f2585ff1))
* standardize error message language in node model ([873500e](https://github.com/devlink42/poc-dirac-job-matchmaking/commit/873500e82bfdc2fc93a0c4663ab34ed9f08b49db))
