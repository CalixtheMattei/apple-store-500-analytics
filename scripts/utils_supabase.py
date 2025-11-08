"""Utility helpers for interacting with Supabase within pipeline scripts."""
from __future__ import annotations

import os
from typing import Set

from dotenv import load_dotenv
from supabase import create_client

__all__ = ["get_client", "get_existing_ids"]

_client = None


def get_client():
    """Return a cached Supabase client using service role credentials."""
    global _client
    if _client is None:
        load_dotenv()
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if not url or not key:
            raise EnvironmentError(
                "Missing Supabase credentials (SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY)."
            )
        _client = create_client(url, key)
    return _client


def get_existing_ids(source: str, app: str, country: str, page_size: int = 1000) -> Set[str]:
    """Return existing ``source_review_id`` values for one (source, app, country)."""
    client = get_client()
    ids: Set[str] = set()
    start = 0
    while True:
        response = (
            client.table("clean_reviews")
            .select("source_review_id")
            .eq("source", source)
            .eq("app_name", app)
            .eq("country", country)
            .range(start, start + page_size - 1)
            .execute()
        )
        records = response.data or []
        batch = [str(row["source_review_id"]) for row in records if "source_review_id" in row]
        ids.update(batch)
        if len(batch) < page_size:
            break
        start += page_size
    return ids
