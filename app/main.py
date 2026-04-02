from app.config import load_settings
from app.discord_bot import AbsBot


def main() -> None:
    settings = load_settings()
    bot = AbsBot(settings)
    bot.run(settings.discord_token)


if __name__ == "__main__":
    main()
