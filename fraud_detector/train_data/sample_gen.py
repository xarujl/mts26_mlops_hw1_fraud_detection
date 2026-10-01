import pandas as pd

input_file = "./test.csv"
output_file = "./test_sample_100.csv"

n_rows = 100
df = pd.read_csv(input_file)
df_small = df.sample(n=n_rows, random_state=42)
df_small.to_csv(output_file, index=False)