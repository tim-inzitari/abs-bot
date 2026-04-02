import os

from app.main import main as run_bot
from app.updater_main import main as run_updater


def main() -> None:
    role = os.getenv("APP_ROLE", "bot").strip().lower()

    if role == "bot":
        run_bot()
        return

    if role == "updater":
        run_updater()
        return

    raise SystemExit(f"Unsupported APP_ROLE={role!r}. Use 'bot' or 'updater'.")


if __name__ == "__main__":
    main()
