from pathlib import Path

from pytest import fixture, mark, raises
from typer import Abort

from scbl_utils.utils import validate


def test_direc_creation(tmp_path):
    test_dir = tmp_path / 'test_config_dir'
    assert not test_dir.exists()
    _ = validate.direc(test_dir)
    assert test_dir.exists()


@fixture
def required_files(request):
    return [Path(filename) for filename in request.param]


@fixture
def absolute_paths(tmp_path, required_files: list[Path]):
    return [(tmp_path / path).absolute() for path in required_files]


argname = 'required_files'
N_CASES = 3
required_file_lists = [[f'file{j}' for j in range(i)] for i in range(N_CASES)]


@mark.parametrize(
    argnames='required_files', argvalues=required_file_lists, indirect=True
)
def test_all_paths_exist(tmp_path, required_files, absolute_paths):
    assert all(not path.exists() for path in absolute_paths)
    for path in absolute_paths:
        path.touch()
    assert all(path.exists() for path in absolute_paths)

    result = validate.direc(tmp_path, required_files=required_files)

    assert result == {path.name: path for path in absolute_paths}


@mark.parametrize(
    argnames='required_files', argvalues=required_file_lists[1:], indirect=True
)
def test_no_paths_exist(tmp_path, required_files, absolute_paths):
    assert all(not path.exists() for path in absolute_paths)
    with raises(Abort):
        validate.direc(tmp_path, required_files=required_files)


@mark.parametrize(
    argnames='required_files', argvalues=required_file_lists[1:], indirect=True
)
def test_some_paths_exist(tmp_path, required_files, absolute_paths):
    assert all(not path.exists() for path in absolute_paths)
    for path in absolute_paths[:-1]:
        path.touch()
    with raises(Abort):
        validate.direc(tmp_path, required_files=required_files)
