from tqdm import tqdm
import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO,  # Nível mínimo de logs (INFO, WARNING, ERROR, DEBUG)
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)

logging.info('Início do mapeamento de CNPJs')
try:
    file_path = "cnpj/cnpjs.csv"
    df_cnpj = pd.read_csv(file_path, dtype=str)
    df_cnpj["raiz"] = df_cnpj["ds_cnpj_usuf"].str[:8]
    set_cnpjs = set(df_cnpj["raiz"])

    lst_names = []

    parquet_files = [f for f in os.listdir("output") if f.endswith(".parquet")]

    for parquet_path in tqdm(parquet_files, desc="Lendo arquivos parquet:"):
        full_path = os.path.join("output", parquet_path)
        itp = pd.read_parquet(full_path, engine="fastparquet").itertuples(index=False)

        df_temp = pd.DataFrame(
            [
                row
                for row in tqdm(itp, desc=f"Processando {parquet_path}")
                if row.CNPJ_BÁSICO in set_cnpjs
            ]
        )

        lst_names.append(df_temp)

    df_final = pd.concat(lst_names, ignore_index=True)
    df_final.to_excel("nome_cnpj.xlsx", index=False)

    logging.info("FIM!")
except Exception as e:
    logging.exception(e)