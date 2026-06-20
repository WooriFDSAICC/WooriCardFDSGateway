from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from app.constants.scoring_constants import FeatureStoreConstants
from app.middleware.security import verify_internal_api_key, verify_internal_network
from app.services.scoring.feature_store_service import FeatureStoreService
from app.utils.pii_masker import PiiMasker

router = APIRouter(
    prefix="/internal",
    tags=["internal"],
    dependencies=[Depends(verify_internal_api_key), Depends(verify_internal_network)],
)


class FeatureUpsertRequest(BaseModel):
    entity_key: str = Field(description="user_id 또는 session_id")
    transfer_sum_30m: Optional[float] = None
    stt_keyword_count: Optional[int] = None
    malicious_app_active: Optional[int] = None
    last_stt_text: Optional[str] = None


def _get_feature_store(request: Request) -> FeatureStoreService:
    return request.app.state.feature_store


@router.put("/feature-store/{entity_key}")
async def upsert_feature_store(
    entity_key: str,
    body: FeatureUpsertRequest,
    request: Request,
) -> Dict[str, Any]:
    store = _get_feature_store(request)
    features: Dict[str, Any] = {}
    if body.transfer_sum_30m is not None:
        features[FeatureStoreConstants.FIELD_TRANSFER_SUM_30M] = body.transfer_sum_30m
    if body.stt_keyword_count is not None:
        features[FeatureStoreConstants.FIELD_STT_KEYWORD_COUNT] = body.stt_keyword_count
    if body.malicious_app_active is not None:
        features[FeatureStoreConstants.FIELD_MALICIOUS_APP_ACTIVE] = body.malicious_app_active
    if body.last_stt_text is not None:
        features[FeatureStoreConstants.FIELD_LAST_STT_TEXT] = body.last_stt_text

    await store.upsert_context_features(entity_key, features)
    return {"status": "SUCCESS", "entity_key": entity_key, "updated_fields": list(features.keys())}


@router.get("/feature-store/{entity_key}")
async def get_feature_store(entity_key: str, request: Request) -> Dict[str, Any]:
    store = _get_feature_store(request)
    features = await store.fetch_context_features(session_id=None, user_id=entity_key)
    if FeatureStoreConstants.FIELD_LAST_STT_TEXT in features:
        features[FeatureStoreConstants.FIELD_LAST_STT_TEXT] = PiiMasker.mask_stt_text(
            str(features[FeatureStoreConstants.FIELD_LAST_STT_TEXT])
        )
    return {"status": "SUCCESS", "entity_key": PiiMasker.mask_user_id(entity_key), "features": features}
