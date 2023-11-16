import pandas as pd

datas = [{'col1': [1, 2, 3], 'col2': [4, 5, 6]}, {'col1': [7, 8, 9, 10], 'col2': [10, 11, 12, 13], 'col3': [14, 15, 16, 17]}]
indexes = ([0, 1, 2], [0, 1, 5, 6])

dfs = [pd.DataFrame(data, index=index) for data, index in zip(datas, indexes)]
df = pd.concat(dfs, axis=0, join='outer')
print(df)