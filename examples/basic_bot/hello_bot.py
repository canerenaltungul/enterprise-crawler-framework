from __future__ import annotations

"""
Enterprise Crawler Framework - Basic Bot Example

Run from the repository root:

    python examples/basic_bot/hello_bot.py

The example intentionally uses only the framework's top-level public API.
"""

from enterprise_crawler import (
    BaseBot,
    Crawler,
)


class HelloBot(BaseBot):
    def execute(self) -> None:
        print("Hello from Enterprise Crawler Framework!")

        self.mark_record_processed()


def main() -> int:
    with HelloBot(
        bot_name="hello-bot"
    ) as bot:
        crawler = Crawler(
            bot
        )

        result = crawler.run()

    print(
        f"status={result.status.value}"
    )

    print(
        "records_processed="
        f"{result.records_processed}"
    )

    return (
        0
        if result.errors == 0
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(
        main()
    )