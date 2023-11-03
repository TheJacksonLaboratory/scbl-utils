from pathlib import Path

from typer.testing import CliRunner
from yaml import Loader, load

from scbl_utils.main import app

# TODO: eventually, parametrize this to test each fastq directory
# individually for more robust testing, so that the two outputs
# don't have to be in the same order
def test_samplesheet_from_gdrive(
    config_dir: Path, dirs: dict[str, Path], tmp_path: Path
):
    runner = CliRunner()

    outsheet = tmp_path / 'test-samplesheet.yml'
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

    correct = dirs['data'] / 'correct-output.yml'
    with outsheet.open() as f, correct.open() as g:
        result_sheet = load(f, Loader)
        correct_sheet = load(g, Loader)

    result_sheet = [
        {
            key: [Path(path).absolute() for path in value] if 'path' in key else value
            for key, value in rec.items()
        }
        for rec in result_sheet
    ]
    correct_sheet = [
        {
            key: [Path(path).absolute() for path in value] if 'path' in key else value
            for key, value in rec.items()
        }
        for rec in correct_sheet
    ]

    assert result_sheet == correct_sheet
