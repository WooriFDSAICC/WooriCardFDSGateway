from __future__ import annotations

import json
import logging
from typing import Any, Dict, List

import httpx

from app.config import settings
from app.constants.scoring_constants import FeatureStoreConstants, ScoringConstants
from app.constants.triton import TritonConstants
from app.models.internal_models import EnrichedFeatureVector, ModelInferenceResult

logger = logging.getLogger(__name__)


class MlInferenceService:
    """Triton Model Worker — FDS LightGBM 추론 클라이언트."""

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    async def startup(self) -> None:
        self._client = httpx.AsyncClient(
            base_url=settings.triton_http_url,
            timeout=httpx.Timeout(
                TritonConstants.HTTP_TIMEOUT_SECONDS,
                connect=TritonConstants.HTTP_CONNECT_TIMEOUT_SECONDS,
            ),
        )
        logger.info("[MlInference] Triton client url=%s model=%s", settings.triton_http_url, settings.triton_fds_model)

    async def shutdown(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def infer(self, vector: EnrichedFeatureVector) -> ModelInferenceResult:
        if self._client is None:
            raise RuntimeError("MlInferenceService not started")

        path = TritonConstants.INFER_PATH_TEMPLATE.format(model=settings.triton_fds_model)
        payload = {
            "inputs": [
                {
                    "name": "FEATURES_JSON",
                    "shape": [1],
                    "datatype": "BYTES",
                    "data": [json.dumps(vector.merged, ensure_ascii=False)],
                },
                {
                    "name": "CONTEXT_JSON",
                    "shape": [1],
                    "datatype": "BYTES",
                    "data": [json.dumps(vector.context_features, ensure_ascii=False)],
                },
            ]
        }
        response = await self._client.post(path, json=payload)
        response.raise_for_status()
        outputs = _index_outputs(response.json().get("outputs") or [])

        raw_score = float(outputs.get("RAW_SCORE", 0.0))
        normalized = int(outputs.get("NORMALIZED_SCORE", 0))
        normalized = max(ScoringConstants.SCORE_MIN, min(ScoringConstants.SCORE_MAX, normalized))
        model_version = str(outputs.get("MODEL_VERSION", settings.triton_fds_model))

        ctx = vector.context_features
        importance = {
            ScoringConstants.KEY_MALICIOUS_APP_WEIGHT: float(
                vector.merged.get(ScoringConstants.KEY_MALICIOUS_APP_WEIGHT, 0) or 0
            ),
            FeatureStoreConstants.FIELD_STT_KEYWORD_COUNT: float(
                ctx.get(FeatureStoreConstants.FIELD_STT_KEYWORD_COUNT, 0)
            ),
            FeatureStoreConstants.FIELD_TRANSFER_SUM_30M: float(
                ctx.get(FeatureStoreConstants.FIELD_TRANSFER_SUM_30M, 0)
            ),
        }

        logger.debug(
            "[MlInference] entity=%s raw=%.4f normalized=%d",
            vector.entity_key,
            raw_score,
            normalized,
        )
        return ModelInferenceResult(
            raw_score=raw_score,
            normalized_score=normalized,
            feature_importance=importance,
            model_version=model_version,
        )


def _index_outputs(outputs: List[Dict[str, Any]]) -> Dict[str, Any]:
    indexed: Dict[str, Any] = {}
    for item in outputs:
        name = item.get("name")
        data = item.get("data")
        if name is None:
            continue
        if isinstance(data, list) and data:
            indexed[name] = data[0]
        else:
            indexed[name] = data
    return indexed
