from typing import List, Optional


def one_line_report(summary: str) -> str:
    return summary


def one_line_error(untracked_errors: List[str]) -> Optional[str]:
    if not untracked_errors:
        return None
    return untracked_errors[0]


def game_context_line(official_date: Optional[str], matchup: Optional[str], detail: str) -> str:
    prefix = " ".join(part for part in [official_date, matchup] if part)
    return f"{prefix} {detail}".strip()
