"""Shared Elasticsearch utilities.

Helpers for working with Elasticsearch responses that are used across
routers and candidate generators.
"""

import logging

from elastic_transport import ObjectApiResponse
from fastapi import HTTPException

logger = logging.getLogger(__name__)


def unwrap_es_response(resp) -> dict:
    """Unwrap an Elasticsearch response, handling both ObjectApiResponse and dict.

    Raises ``HTTPException`` with 502 if the response type is unexpected.
    """
    if isinstance(resp, ObjectApiResponse):
        return resp.body
    elif isinstance(resp, dict):
        return resp
    else:
        logger.error("Unexpected Elasticsearch response type: %s", type(resp))
        raise HTTPException(status_code=502, detail="Invalid Elasticsearch response")
