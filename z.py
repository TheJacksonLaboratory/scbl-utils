import pandas as pd

# Create a dummy dataframe
df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})

# Add 4 empty rows to the dataframe
df = pd.concat([df, pd.DataFrame(columns=df.columns)], sort=False)

# Print the updated dataframe
print(df)
