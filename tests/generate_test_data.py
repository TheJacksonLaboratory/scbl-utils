from os import chdir
from pathlib import Path

import pandas as pd

# Change directory so relative paths work, define test data dir
chdir(Path(__file__).parent)
data_dir = Path('data')

# Load in samples that will be tested
test_trackingsheet = pd.read_csv(data_dir / 'test-trackingsheet.csv')

# Iterate over each sample and create directories and fake fastq files
for sample_name, df in test_trackingsheet.groupby('sample_name'):
    sample_dir = data_dir / 'fastqs' / sample_name  # type: ignore
    sample_dir.mkdir(exist_ok=True, parents=True)
    for lib in df['libraries']:
        (sample_dir / f'{lib}_INFO.fastq.gz').write_text('\n')
