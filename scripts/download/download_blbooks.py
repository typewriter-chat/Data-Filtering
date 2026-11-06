"""Download the raw BL Books dataset shards to local disk.

Run this once before ``explore_blbooks.py``.  It uses
``huggingface_hub.snapshot_download``, which:
  - is fully resumable (checksums already-downloaded files and skips them)
  - supports ``--include`` globs so only the needed century configs are fetched

After this script completes, ``explore_blbooks.py`` can be pointed at the
local directory via ``--local-dir`` and will never touch the network again.

Usage
-----
    # Download only 1700s and 1800s configs (matches our year range 1700–1920)
    uv run python scripts/download/download_blbooks.py \\
        --local-dir data/raw/blbooks \\
        --configs 1700_1799 1800_1899

    # Then process locally (no network required)
    uv run python scripts/explore_blbooks.py \\
        --local-dir data/raw/blbooks
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from huggingface_hub import snapshot_download

_HF_REPO = 'TheBritishLibrary/blbooks'
_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_LOCAL_DIR = _REPO_ROOT / 'data' / 'raw' / 'blbooks'
_ALL_CONFIGS = ['1510_1699', '1700_1799', '1800_1899']


def download(local_dir: Path, configs: list[str]) -> None:
    """Download the specified BL Books config shards to local disk.

    Args:
        local_dir: Local directory to store the downloaded files.
        configs: List of config names to download (e.g. ``['1700_1799', '1800_1899']``).
    """
    local_dir.mkdir(parents=True, exist_ok=True)

    # Always include the dataset script and metadata so load_dataset works locally
    allow_patterns = ['*.py', '*.json', '*.md', '*.txt']
    for cfg in configs:
        allow_patterns.append(f'{cfg}/*')

    print(f'Downloading configs {configs} from {_HF_REPO} → {local_dir}')
    print('(Already-downloaded files will be skipped automatically.)\n')

    snapshot_download(
        repo_id=_HF_REPO,
        repo_type='dataset',
        local_dir=str(local_dir),
        allow_patterns=allow_patterns,
        local_dir_use_symlinks=False,  # copy files directly; avoids blob cache in ~/.cache/huggingface/hub/
    )
    print(f'\nDone. Dataset available at {local_dir}')
    print(f'Run explore_blbooks.py with --local-dir {local_dir}')


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Download BL Books raw shards to local disk for offline processing.'
    )
    parser.add_argument(
        '--local-dir',
        default=_DEFAULT_LOCAL_DIR,
        type=Path,
        help='Local directory to store downloaded files.',
    )
    parser.add_argument(
        '--configs',
        default=['1700_1799', '1800_1899'],
        nargs='+',
        choices=_ALL_CONFIGS,
        metavar='CONFIG',
        help='Config(s) to download. Default: 1700_1799 1800_1899.',
    )
    return parser


def main() -> None:
    """Entry point for BL Books raw download."""
    args = _build_parser().parse_args()
    download(args.local_dir, args.configs)


if __name__ == '__main__':
    main()
