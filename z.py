import pandas as pd

data = {
    'Name': ['John', 'Alice', 'Bob', 'Emma', 'Michael', 'Olivia', 'Sophia', 'Daniel'],
    'Age': [25, 30, 27, 22, 35, 29, 31, 26],
    'Major': [
        'Computer Science',
        'Mathematics',
        'Engineering',
        'Mathematics',
        'Biology',
        'Chemistry',
        'Computer Science',
        'History',
    ],
}

df = pd.DataFrame(data)

print(df.groupby('Major').agg({'Name': 'first', 'Age': 'first', 'Major': list}))
