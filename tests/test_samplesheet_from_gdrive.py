from pathlib import Path
from subprocess import run

from typer.testing import CliRunner
from yaml import Loader, load

from scbl_utils.main import app


def test_samplesheet_from_gdrive(config_dir: Path, dirs: dict[str, Path]):
    runner = CliRunner()

    outsheet = dirs['outputs'] / 'test-samplesheet.yml'
    fastqs = [str(path.absolute()) for path in (dirs['data'] / 'fastqs').iterdir()]
    args = [
        '--config-dir',
        str(config_dir),
        'samplesheet-from-gdrive',
        '--reference-parent-dir',
        str(dirs['references']),
        '--outsheet',
        str(outsheet),
        *fastqs,
    ]
    inputs = '\n'.join(len(fastqs) * ['test-refdata'])

    _ = runner.invoke(app, args=args, input=inputs)

    with outsheet.open() as f:
        samplesheet = load(f, Loader)
    with (dirs['data'] / 'correct-output.yml').open() as f:
        correct = load(f, Loader)

    assert samplesheet == correct
