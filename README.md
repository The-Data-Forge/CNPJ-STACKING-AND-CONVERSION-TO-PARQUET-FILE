# $CNPJ-STACKING-AND-CONVERSION-TO-PARQUET-FILE$

## **Objetivo**

Ler os arquivos de domínio público disponibilizados pela Receita Federal, atualmente no formato .csv, e convertê-los para o formato .parquet, que oferece melhor desempenho em leitura, armazenamento e consultas.

### _Problema/Dor_

Para realizar um cadastro simples de estabelecimentos, é necessário no mínimo o `CNPJ` e a `Razão Social`, informações que já estão presentes nos arquivos originais em .csv.

O desenvolvimento deste projeto tem como foco disponibilizar uma forma eficiente de acessar esses dados, sem depender de APIs externas que podem gerar custos adicionais ou sofrer indisponibilidade.

---

## **Dados**

A massa de dados utilizada consiste em todas as empresas disponibilizadas no site da Receita Federal.

| Objeto                 | Descrição |
| ---------------------- | --------- |
| Data de referência     | 2025-06   |
| Quantidade de arquivos | 9         |
| Tipo                   | CSV       |

Total aproximado de **`63.235.730 linhas`**, podendo variar conforme a data de referência.

> Fonte: [Receita Federal – Dados Abertos CNPJ](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj)

---

## **Conversão de CSV para Parquet**

O código realiza a leitura dos arquivos CSV e utiliza **pandas** em conjunto com **pyarrow** para produzir arquivos no formato Parquet, que oferecem melhor compressão e velocidade de acesso.

### **Colunas utilizadas**

- `CNPJ_BÁSICO`
- `RAZÃO_SOCIAL`
- `NATUREZA_JURÍDICA`
- `QUALIFICAÇÃO_DO_RESPONSÁVEL`
- `CAPITAL_SOCIAL_DA_EMPRESA`
- `PORTE_DA_EMPRESA`
- `ENTE_FEDERATIVO_RESPONSÁVEL`

---

### **Empilhamento**

A ideia inicial era gerar um único arquivo _.parquet_ com todos os registros.
Entretanto, para garantir **viabilidade em máquinas com pouca memória RAM** e melhor **performance**, a conversão foi feita em arquivos unitários.

Essa abordagem reduz o consumo de memória tanto no momento da criação quanto em consultas futuras.

> Script: [read_conversion.py](read_conversion.py)

---

## **Leitura dos arquivos**

A leitura eficiente dos dados é essencial, já que o volume ultrapassa dezenas de milhões de linhas.
O foco do código é buscar rapidamente a correspondência entre um `CNPJ` e sua respectiva `Razão Social`.

### **Melhorias aplicadas para extrair cada milissegundo**

#### 1. **Slicing vetorizado**

Uso de slicing direto em colunas do DataFrame:

```python
df_cnpj['raiz'] = df_cnpj['ds_cnpj_usuf'].str[:8]
```

Essa operação é **100% vetorizada** e roda até **10x mais rápido** que laços convencionais.
→ Menos conversão, menos trabalho, mais velocidade.

---

#### 2. **Escolha do engine Parquet**

- **fastparquet**: mais leve, eficiente em memória, ótimo para leitura e escrita simples.
- **pyarrow**: geralmente mais escalável e rápido em datasets muito grandes.

📌 Nos testes realizados, **fastparquet apresentou melhor performance** para o caso específico deste projeto.

---

#### 3. **Uso de generators ao invés de DataFrames completos**

```python
itp = pd.read_parquet(full_path, engine="fastparquet").itertuples(index=False)
```

O método `itertuples()` gera um **iterator leve** (generator), consumindo memória mínima ao percorrer milhões de registros.

---

#### 4. **List comprehension no lugar de `.append()`**

```python
lst_names = [
    tp_cnpj_name
    for tp_cnpj_name in tqdm(itp_name_cnpj, desc="Iterando sobre os CNPJs: ")
    if tp_cnpj_name.CNPJ_BÁSICO in set_cnpjs
]
```

Vantagens:

- Compilação interna em **C**, muito mais rápida.
- Evita custo de realocação de `.append()` em listas grandes.
- Checagem inline em `set_cnpjs`, com lookup O(1).
- Integração fluida com `tqdm` para acompanhamento.

#### **Resumo:**

Essas escolhas de implementação tornam o processo de leitura e busca de CNPJs **mais rápido, com menor uso de memória** e preparado para lidar com bases da ordem de dezenas de milhões de registros.

## **Resultados**

```powershell
05/08/2025 21:42:57 [INFO] - Início do mapeamento de CNPJs
Processando K3241_K03200Y0_D50614_EMPRECSV.parquet: 22781990it [00:11, 1902800.46it/s]             | 0/10 [00:00<?, ?it/s]
Processando K3241_K03200Y1_D50614_EMPRECSV.parquet: 4494860it [00:02, 1830895.64it/s]      | 1/10 [00:18<02:44, 18.29s/it]
Processando K3241_K03200Y2_D50614_EMPRECSV.parquet: 4494860it [00:02, 1973676.78it/s]      | 2/10 [00:22<01:18,  9.87s/it]
Processando K3241_K03200Y3_D50614_EMPRECSV.parquet: 4494860it [00:02, 1849135.87it/s]      | 3/10 [00:25<00:47,  6.85s/it]
Processando K3241_K03200Y4_D50614_EMPRECSV.parquet: 4494860it [00:02, 1895576.94it/s]      | 4/10 [00:28<00:32,  5.44s/it]
Processando K3241_K03200Y5_D50614_EMPRECSV.parquet: 4494860it [00:02, 1834418.73it/s]      | 5/10 [00:32<00:23,  4.67s/it]
Processando K3241_K03200Y6_D50614_EMPRECSV.parquet: 4494860it [00:02, 1853271.23it/s]      | 6/10 [00:35<00:16,  4.24s/it]
Processando K3241_K03200Y7_D50614_EMPRECSV.parquet: 4494860it [00:02, 1908660.69it/s]      | 7/10 [00:38<00:11,  3.96s/it]
Processando K3241_K03200Y8_D50614_EMPRECSV.parquet: 4494860it [00:02, 1825877.29it/s]      | 8/10 [00:42<00:07,  3.75s/it]
Processando K3241_K03200Y9_D50614_EMPRECSV.parquet: 4494860it [00:02, 1869910.08it/s]      | 9/10 [00:45<00:03,  3.64s/it]
Lendo arquivos parquet:: 100%|████████████████████████████████████████████████████████████| 10/10 [00:48<00:00,  4.90s/it]
05/08/2025 21:43:46 [INFO] - FIM!
```

Em média **`1.874.422,37`** Iterações por segundo e 48s para mapear **`63.235.730`** linhas.


