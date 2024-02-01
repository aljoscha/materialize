# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from pathlib import Path

from materialize import mzbuild
from materialize.xcompile import Arch

from . import deploy_util
from .deploy_util import MZ_CLI_VERSION


def main() -> None:
    repos = [
        mzbuild.Repository(Path("."), Arch.X86_64, coverage=False, sanitizer="none"),
        mzbuild.Repository(Path("."), Arch.AARCH64, coverage=False, sanitizer="none"),
    ]

    print("--- Tagging Docker images")
    deps = [[repo.resolve_dependencies([repo.images["mz"]])["mz"]] for repo in repos]

    mzbuild.publish_multiarch_images(f"v{MZ_CLI_VERSION.str_without_prefix()}", deps)
    if deploy_util.is_latest_version():
        mzbuild.publish_multiarch_images("latest", deps)


if __name__ == "__main__":
    main()
