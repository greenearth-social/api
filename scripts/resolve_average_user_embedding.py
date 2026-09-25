#!/usr/bin/env python3
"""Resolve the average embedding for a deployment without changing cloud state."""

import argparse
import sys
from pathlib import Path

# Follow the other API scripts' app.lib imports, regardless of working directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from app.lib.average_user_embedding_artifact import ArtifactValidationError  # noqa: E402
from app.lib.average_user_embedding_publication import (  # noqa: E402
    DEFAULT_PROJECT_ID,
    PublicationError,
    resolve_artifact,
)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--environment", choices=("stage", "prod"), required=True)
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    parser.add_argument(
        "--artifact-uri",
        help="use an exact timestamped gs:// artifact instead of the environment's default",
    )
    args = parser.parse_args(argv)
    # This is deployment preflight, not runtime loading: resolve and validate
    # the selection now so deploy.sh can pin the exact artifact on the revision.
    try:
        result = resolve_artifact(args.environment, args.project_id, args.artifact_uri)
    except (PublicationError, ArtifactValidationError) as error:
        print(f"Average embedding selection failed: {error}", file=sys.stderr)
        return 1
    except Exception as error:
        # Unexpected SDK errors may include credentials or response bodies.
        # A nonzero exit and empty stdout prevent treating an error as a URI.
        print(f"Average embedding selection failed ({type(error).__name__})", file=sys.stderr)
        return 1

    print(
        f"Average embedding: run_id={result['run_id']} "
        f"user_model_uuid={result['user_model_uuid']} "
        f"post_model_uuid={result['post_model_uuid']} "
        f"dimension={result['dimension']} contributing_users={result['contributing_users']}",
        file=sys.stderr,
    )
    # deploy.sh consumes stdout as a single URI; diagnostics belong on stderr.
    print(result["artifact_uri"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
