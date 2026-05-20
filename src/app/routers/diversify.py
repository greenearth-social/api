from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from ..lib.diversify import mmr_rerank
from ..models import CandidatePost
from ..security import verify_api_key

router = APIRouter(tags=["diversify"], dependencies=[Depends(verify_api_key)])


class DiversifyRequest(BaseModel):
    """Ordered list of candidates to rerank for diversity."""

    candidates: list[CandidatePost] = Field(
        ...,
        description=(
            "Candidate posts to rerank, in their current order (e.g. as "
            "returned by /rank/predict). MiniLM L12 embeddings must be "
            "present on each post for MMR to compute similarity."
        ),
    )


class DiversifyResponse(BaseModel):
    """Reranked candidate list with improved topical variety."""

    candidates: list[CandidatePost] = Field(
        ...,
        description="Candidates reordered by MMR to reduce topical redundancy.",
    )


@router.post("/diversify", response_model=DiversifyResponse)
async def diversify(payload: DiversifyRequest) -> DiversifyResponse:
    """Rerank candidates using Maximal Marginal Relevance (MMR).

    MMR balances relevance against redundancy: each successive post is chosen
    to be as relevant as possible while being as dissimilar as possible from
    the posts already selected. This reduces topical clustering in the final
    feed without sacrificing overall quality.

    Call this endpoint as the final step after `/rank/predict` (or directly
    after `/candidates/generate` if skipping ranking).  Posts must carry
    `minilm_l12_embedding` values for similarity to be computed; posts
    without embeddings are appended to the end of the reranked list.
    """
    return DiversifyResponse(candidates=mmr_rerank(payload.candidates))
