from pathlib import Path

DELTA_PATHS = [
    "/opt/workspace/data/bronze/orders",
    "/opt/workspace/data/silver/orders",
    "/opt/workspace/data/gold/orders",
]


def main() -> None:
    missing = [p for p in DELTA_PATHS if not Path(p).exists()]
    if missing:
        raise SystemExit(
            f"Validation failed: the following streaming output paths are missing: {missing}"
        )

    print("Stream validation succeeded. All Delta paths exist:")
    for path in DELTA_PATHS:
        print(f" - {path}")


if __name__ == "__main__":
    main()
