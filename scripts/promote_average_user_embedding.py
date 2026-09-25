#!/usr/bin/env python3
"""Select an inspected local or GCS average embedding for future deployments."""

import argparse
import json
import logging
import sys
from pathlib import Path

# Resolve app imports relative to this script, including when invoked outside the repo.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from app.lib.average_user_embedding_artifact import ArtifactValidationError  # noqa: E402
from app.lib.average_user_embedding_publication import (  # noqa: E402
    DEFAULT_PROJECT_ID,
    PublicationError,
    promote_artifact,
)


def main(argv=None) -> int:
    # Choosing stage or prod is explicit; an omitted flag must not promote by accident.
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="Local artifact path or exact gs://bucket/object.json URI")
    parser.add_argument("--environment", choices=("stage", "prod"), required=True)
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    args = parser.parse_args(argv)
    # Keep progress on stderr and one machine-readable result on stdout, like the
    # generation command. Cloud validation and writes belong to the shared helper.
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stderr)
    try:
        result = promote_artifact(args.source, args.environment, args.project_id)
    except (ArtifactValidationError, PublicationError) as error:
        message = str(error)
    except OSError as error:
        message = f"Unable to read local artifact ({type(error).__name__})"
    except KeyboardInterrupt:
        # An interrupted upload may have completed remotely before the client saw it.
        message = "Promotion interrupted; inspect the current default before retrying"
    else:
        # Changing a default selects an artifact for a future deployment; it does
        # not change the code or the artifact used by an already-running service.
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
