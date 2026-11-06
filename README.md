# Historical-LM

Core data downloading and filtering code for our paper: [A Language Model from 1913: Pretraining on Historical Text](https://arxiv.org/abs/2606.02991).

## Data

The cleaned corpora are on HuggingFace:
<https://huggingface.co/datasets/typewriter-ai/TypewriterLM-1913-data>

**Institutional Books is not released.** It makes up the largest part of the
training data, but redistributing it would violate the terms of service of the
[Institutional Books 1.0](https://huggingface.co/datasets/institutional/institutional-books-1.0)
dataset, so it is not on HuggingFace. To reproduce it, download the dataset
yourself and run the scripts below.

## Layout

| Path | Purpose |
|---|---|
| `scripts/download/` | Download the raw corpora. |
| `scripts/process/` | Convert the raw corpora to paragraph-segmented Parquet. |
| `scripts/clean/` | Filter and clean the processed corpora. |
| `src/historical_lm/` | Shared helpers (word counts, corpus headers, VRT parsing). |
| `docs/` | Dataset card (`hf_dataset_card.md`) and per-corpus notes on what the cleaning removes and why (`cleaning/`). |

## Pipeline

Run the scripts in this order: download, then process, then clean. Every
script takes its input and output locations as command-line arguments; run it
with `--help` to see them.

```bash
uv sync
uv run python scripts/download/download_institutional.py --help
uv run python scripts/clean/clean_institutional.py --help
```

For Institutional Books, cleaning keeps books published from 1700 to 1913 whose
OCR scores (`ocr_score_src` and `ocr_score_gen`) are both at least 90 and
removes stamps, call numbers, headers and other non-prose paragraphs; see
[`docs/cleaning/institutional_clean.md`](docs/cleaning/institutional_clean.md).

## Documentation

- [`docs/hf_dataset_card.md`](docs/hf_dataset_card.md): the dataset card
  uploaded to HuggingFace as its README.
- [`docs/cleaning/`](docs/cleaning/): one note per corpus describing the
  cleaning rules and the issues found during inspection.
