#!/usr/bin/env python3
"""Uvicorn entrypoint: uvicorn run:app --host 0.0.0.0 --port 8010"""

from app.main import app

__all__ = ["app"]
