import pandas as pd

# # Create a dummy dataframe
# data = {'name': ['John', 'Mary', 'Peter', 'sarah'],
#     'age': [25, 30, 35, 40],
#     'another_column': ['n/a', 'na', '', 'value']}
# whole_df = pd.DataFrame(data)

# # Define the replacement dictionary
# replacedict = {'john': 'newjohn', 'mary': 'newmary', 'peter': 'newpeter', 'sarah': 'newarah', 'n/a': None, '': None}

# # Replace values in the dataframe
# replace = {rf'(?i)^{key}$': val for key, val in replacedict.items()}
# whole_df.replace(regex=replace, inplace=True)

# print(whole_df)

s1 = pd.Series(['ahmed', 'john', 'maruy'])

s2 = pd.Series(['frank', 'robart'])

print(pd.concat([s1, s2]))
