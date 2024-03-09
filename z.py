import polars as pl

df = pl.DataFrame(
    {
        "foo": [1, 2, 3],
        "bar": [6.0, 7.0, 8.0],
        "ham": ["a", "b", "c"],
        'hotdog': [1, 2, 3],
    }
)
other_df = pl.DataFrame(
    {"apple": ["x", "y", "z"], "ham": ["a", "b", "d"], "hotdog": [5, 6, 7]}
)
common_columns = [column for column in df.columns if column in other_df.columns]
print(df.join(other_df, how='inner', on="ham"))
