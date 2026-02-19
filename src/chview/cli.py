"""CLI entry point for CHView.

Invoked via:
  chview              # installed via pyproject.toml [project.scripts]
  python -m chview    # via __main__.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main() -> None:
    """Launch the CHView Streamlit application."""
    app_path = Path(__file__).parent / "app.py"

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--",
        *sys.argv[1:],
    ]

    try:
        result = subprocess.run(cmd, check=False)
        sys.exit(result.returncode)
    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
