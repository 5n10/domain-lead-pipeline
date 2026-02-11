from __future__ import annotations

import json

from domain_pipeline.metrics import collect_metrics


def main() -> None:
    metrics = collect_metrics()
    print(json.dumps(metrics, indent=2, default=str))


if __name__ == "__main__":
    main()
