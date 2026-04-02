from difflib import SequenceMatcher
import re
import unicodedata
from typing import Optional


def percent(value: Optional[float], digits: int = 1) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.{digits}f}%"


def signed(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.{digits}f}"


def normalize_search(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", ascii_only.lower())


def score_match(query: str, candidate: str) -> int:
    normalized_query = normalize_search(query)
    normalized_candidate = normalize_search(candidate)

    if not normalized_query or not normalized_candidate:
        return 0
    if normalized_query == normalized_candidate:
        return 100
    if normalized_candidate.startswith(normalized_query):
        return 75
    if normalized_query in normalized_candidate:
        return 50
    ratio = SequenceMatcher(None, normalized_query, normalized_candidate).ratio()
    if min(len(normalized_query), len(normalized_candidate)) >= 5 and ratio >= 0.84:
        return int(ratio * 100)
    return 0
