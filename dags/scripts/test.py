import pandas as pd

df = pd.read_parquet("combined_kredit_sektoral.parquet")
df.to_csv("output_kredit.csv", index=False)
