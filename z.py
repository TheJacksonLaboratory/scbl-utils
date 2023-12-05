from pathlib import Path

base = Path.home() / 'work'
og = base / 'fake_delivery_dir2'
new = base / 'fake_delivery_dir'
new.mkdir(exist_ok=True)
for path in og.iterdir():
    new_path = new / path.name
    new_path.mkdir(exist_ok=True)