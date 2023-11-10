from copy import deepcopy
from pathlib import Path

from pytest import mark
from typer.testing import CliRunner
from yaml import Loader, load

from scbl_utils.main import app


# TODO: eventually, parametrize this to test each fastq directory
# individually for more robust testing, so that the output is not a
# list of dicts, meaning they don't have to be in the same order
@mark.parametrize(argnames='ref_path_as_str', argvalues=['', '-ref_path-as-str'])
def test_samplesheet_from_gdrive(
    config_dir: Path, dirs: dict[str, Path], tmp_path: Path, ref_path_as_str: str
):
    runner = CliRunner()
    
    outsheet = tmp_path / f'test-samplesheet{ref_path_as_str}.yml'

    fastqs = [str(path.absolute()) for path in (dirs['data'] / 'fastqs').iterdir()]
    
    if ref_path_as_str:
        args = [
            '--config-dir',
            str(config_dir),
            'samplesheet-from-gdrive',
            '--reference-parent-dir',
            str(dirs['references']),
            '--outsheet',
            str(outsheet),
            '-s',
            *fastqs,
        ]
    else:
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
    
    correct = dirs['data'] / f'correct-output{ref_path_as_str}.yml'
    with outsheet.open() as f, correct.open() as g:
        result_sheet = load(f, Loader)
        correct_sheet = load(g, Loader)

    new_sheets = []
    for sheet in (result_sheet, correct_sheet):
        path_absolute_sheet = []
        for record in sheet:
            new_record = {}
            for key, value in record.items():
                if 'path' in key:
                    if isinstance(value, list):
                        new_record[key] = [Path(path).absolute() for path in value]
                    else:
                        new_record[key] = Path(value).absolute()
                else:
                    new_record[key] = value
            path_absolute_sheet.append(new_record)

        new_sheets.append(deepcopy(path_absolute_sheet))

    assert new_sheets[0] == new_sheets[1]
