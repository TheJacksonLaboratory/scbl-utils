# TODO: write docstring?
from pathlib import Path
from scbl_utils.db_models import data, definitions, bases


def main():
    table_modules = [data, definitions]
    tables = {
        obj.__tablename__
        for module in table_modules
        for obj in vars(module).values()
        if hasattr(obj, '__tablename__')
    }
    current_dir = Path(__file__).parent.absolute()

    for table_name in tables:
        (current_dir / 'input_data' / f'{table_name}.csv').touch(exist_ok=True)


if __name__ == '__main__':
    main()
