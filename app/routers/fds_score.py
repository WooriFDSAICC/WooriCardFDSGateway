from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Request

from app.constants.scoring_constants import ApiConstants, ScoringConstants
from app.middleware.rate_limit import apply_rate_limit
from app.middleware.security import verify_api_key
from app.models.fds_models import FdsScoreErrorResponse, FdsScoreRequest, FdsScoreResponse
from app.services.scoring.fds_scoring_pipeline import FdsScoringPipeline
from app.utils.pii_masker import PiiMasker

logger = logging.getLogger(__name__)

router = APIRouter(tags=["fds-scoring"])


def _get_pipeline(request: Request) -> FdsScoringPipeline:
    return request.app.state.fds_scoring_pipeline


@router.post(
    ApiConstants.FDS_SCORE_PATH,
    response_model=FdsScoreResponse,
    responses={500: {"model": FdsScoreErrorResponse}},
    summary="실시간 FDS 가변형 스코어링",
    dependencies=[Depends(verify_api_key), Depends(apply_rate_limit)],
)
async def score_transaction(
    request_body: FdsScoreRequest,
    request: Request,
) -> FdsScoreResponse:
    pipeline = _get_pipeline(request)
    try:
        result = await pipeline.score(request_body)
        logger.info(
            "[FdsScoreAPI] session=%s user=%s score=%d flag=%s rules=%s",
            request_body.session_id,
            PiiMasker.mask_user_id(request_body.user_id),
            result.fds_score,
            result.fds_flag,
            result.matched_rules,
        )
        return result
    except Exception as exc:
        logger.exception("[FdsScoreAPI] Scoring failed session=%s", request_body.session_id)
        raise HTTPException(
            status_code=500,
            detail=FdsScoreErrorResponse(
                status=ScoringConstants.STATUS_FAILED,
                message="FDS scoring pipeline error",
                detail=str(exc),
            ).model_dump(),
        ) from exc
