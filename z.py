from pathlib import Path

from scbl_db import ORDERED_MODELS

for model_name in ORDERED_MODELS:
    p = Path(
        f'/Users/saida/work/scbl-utils_project/scbl-utils_v2_restructure/tests/data/{model_name}.csv'
    )
    if p.exists():
        continue

    p.touch()
