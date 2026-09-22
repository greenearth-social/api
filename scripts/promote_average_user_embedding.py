#!/usr/bin/env python3
"""Select an inspected local or GCS average embedding for future deployments."""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from app.lib.average_user_embedding_artifact import ArtifactValidationError  # noqa: E402
from app.lib.average_user_embedding_publication import (  # noqa: E402
    DEFAULT_PROJECT_ID,
    PublicationError,
    promote_artifact,
)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="Local artifact path or exact gs://bucket/object.json URI")
    parser.add_argument("--environment", choices=("stage", "prod"), required=True)
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    try:
        result = promote_artifact(args.source, args.environment, args.project_id)
    except (ArtifactValidationError, PublicationError) as error:
        message = str(error)
    except OSError as error:
        message = f"Unable to read local artifact ({type(error).__name__})"
    except KeyboardInterrupt:
        message = "Promotion interrupted; inspect the current default before retrying"
    else:
        print(f"Previous: {result['previous_artifact_uri'] or '(none)'}", file=sys.stderr)
        print(f"Selected: {result['artifact_uri']}", file=sys.stderr)
        print(
            "Promotion does not deploy the API or change running services.",
            file=sys.stderr,
        )
        print(json.dumps({"status": "success", **result}))
        return 0
    print(f"Promotion failed: {message}", file=sys.stderr)
    print(json.dumps({"status": "failed", "error": message}))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
