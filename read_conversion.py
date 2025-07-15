import pandas as pd
import os

lst_input_data = os.listdir("input")

print(lst_input_data)

columns_name = [
    "CNPJ_BÁSICO",
    "RAZÃO_SOCIAL",
    "NATUREZA_JURÍDICA",
    "QUALIFICAÇÃO_DO_RESPONSÁVEL",
    "CAPITAL_SOCIAL_DA_EMPRESA",
    "PORTE_DA_EMPRESA",
    "ENTE_FEDERATIVO_RESPONSÁVEL",
]


for data_file in lst_input_data:
    data_path = f"input/{data_file}"
    output_path = f"output/{data_file.replace('.','_')}.parquet"
    print(data_path)
    df_cnpj = pd.read_csv(
        data_path,
        dtype=str,
        sep=";",
        encoding="Latin-1",
        names=columns_name,
        usecols=["CNPJ_BÁSICO", "RAZÃO_SOCIAL"],
    )
    print(output_path)
    df_cnpj.to_parquet(output_path, index=False, engine="pyarrow")
