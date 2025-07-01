import pandas as pd

def load_tranco_domains(csv_path, limit=500):
    df = pd.read_csv(csv_path, header=None, names=["rank", "domain"])
    return df["domain"].head(limit).tolist()