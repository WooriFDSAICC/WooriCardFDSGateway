from __future__ import annotations

import re
from typing import Optional


class PiiMasker:
    """STT 텍스트, user_id 등 PII 로그/Kafka 마스킹."""

    _USER_ID_PATTERN = re.compile(r"^(.{2}).*(.{2})$")
    _PHONE_PATTERN = re.compile(r"\d{3}-?\d{3,4}-?\d{4}")

    @classmethod
    def mask_user_id(cls, user_id: Optional[str]) -> str:
        if not user_id:
            return "***"
        if len(user_id) <= 4:
            return "*" * len(user_id)
        return cls._USER_ID_PATTERN.sub(r"\1****\2", user_id)

    @classmethod
    def mask_stt_text(cls, text: Optional[str], visible_chars: int = 4) -> str:
        if not text:
            return ""
        if len(text) <= visible_chars:
            return "*" * len(text)
        return text[:visible_chars] + "*" * (len(text) - visible_chars)

    @classmethod
    def mask_phone_in_text(cls, text: str) -> str:
        return cls._PHONE_PATTERN.sub("***-****-****", text)
