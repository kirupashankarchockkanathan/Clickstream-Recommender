# Slides

- `main.tex` is the Beamer deck (10 slides + 3 backup slides). `main.pdf` is the compiled result.
- Charts come from `../../outputs/charts/`; screenshots are in `shots/` (`shot1.png` ... `shot10.png`, plus `shot3_*.png` extra VS Code views). What each shows: `../SCREENSHOT_CHECKLIST.md`.

## Rebuild
From the project root (the charts live outside this folder, so mount the whole project):
```
docker run --rm -v "${PWD}:/w" -w /w/docs/presentation texlive/texlive pdflatex main.tex
docker run --rm -v "${PWD}:/w" -w /w/docs/presentation texlive/texlive pdflatex main.tex
```
(run twice for cross-references; PowerShell syntax). Overleaf also works: upload `main.tex`, `shots/` and the PNGs from `outputs/charts/` into a `charts/` folder and set `\graphicspath{{charts/}}`.

## Honesty notes about the screenshots
- shot1, 2, 4, 6, 8, 10: Chrome headless captures of the real HDFS / Spark / YARN web UIs.
- shot3 (+ `shot3_*`) and shot5: real window captures of VS Code and of the Streamlit dashboard.
- shot7 and shot9: real command output (fsck, dfsadmin, Kafka consumer) rendered to an image, because a terminal cannot be captured headless; the image header says so.
- shot2 shows the Spark SQL plan of a demo aggregation over one week of clean events (`src/python/dag_demo.py`), not the full cleaning job.
