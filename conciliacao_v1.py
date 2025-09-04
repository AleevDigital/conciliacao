import pandas as pd
import numpy as np
import re
import io

# Tratamento do Extrato Bancário
def tratamento_extrato_bb(xlsx_bytes, salvar_em=None):
    # Lê o XLSX a partir de bytes (sem usar caminho no disco)
    extrato = pd.read_excel(io.BytesIO(xlsx_bytes), dtype=str, header=None, engine="openpyxl")

    # Remove cabeçalho/rodapé padrão do relatório
    extrato = extrato.iloc[12:-2]

    # Filtros de linhas indesejadas
    extrato = extrato[~extrato.iloc[:, 0].str.contains(r'autoatendimento|Evaluation', case=False, na=False)]
    extrato = extrato[~extrato.iloc[:, 0].str.contains(r'A CONTA NAO FOI MOVIMENTADA', case=False, na=False)]
    extrato = extrato[~extrato.iloc[:, 3].str.contains(r'500 Tar DOC/TED', case=False, na=False)]

    # Junta tudo para facilitar split
    extrato["Linha_Unica"] = extrato.fillna("").astype(str).agg("|".join, axis=1)

    # Split nas primeiras 7 colunas (ajuste se necessário)
    extrato_split = extrato["Linha_Unica"].str.split(r"\|+", expand=True)
    extrato_split = extrato_split[[0, 1, 2, 3, 4, 5, 6]]
    extrato_split.columns = ["Data", "Agencia de Origem", "Lote", "Historico", "Documento", "Valor", "Saldo"]

    # Corrige "Histórico" quando a linha de valores vem quebrada
    extrato_split["Valor_Seguinte"] = extrato_split["Valor"].shift(-1)
    extrato_split["Historico_Seguinte"] = extrato_split["Historico"].shift(-1)

    # Se a próxima linha NÃO tiver valor, concatena o histórico
    extrato_split["Historico_Corrigido"] = np.where(
        extrato_split["Valor_Seguinte"].notna(),
        extrato_split["Historico"],
        (extrato_split["Historico"].fillna("") + " " + extrato_split["Historico_Seguinte"].fillna("")).str.strip()
    )

    # Limpa linhas com data inválida
    extrato_split["Data"] = extrato_split["Data"].astype(str).str.strip()
    mask_data_valida = (
        extrato_split["Data"].notna() &
        extrato_split["Data"].ne("") &
        extrato_split["Data"].str.lower().ne("nan")
    )
    extrato_split = extrato_split[mask_data_valida]

    # Seleciona e renomeia colunas finais
    extrato_split = extrato_split[["Data", "Agencia de Origem", "Lote", "Historico_Corrigido", "Documento", "Valor", "Saldo"]]
    extrato_split = extrato_split.rename(columns={"Historico_Corrigido": "Histórico"})

    # Separa Débito/Crédito a partir do sufixo D/C
    valor_str = extrato_split["Valor"].astype(str).str.strip()
    extrato_split["Débito"] = np.where(valor_str.str.endswith("D"), extrato_split["Valor"], np.nan)
    extrato_split["Débito"] = extrato_split["Débito"].str.replace("D", "", regex=False).str.strip()
    extrato_split["Crédito"] = np.where(valor_str.str.endswith("C"), extrato_split["Valor"], np.nan)
    extrato_split["Crédito"] = extrato_split["Crédito"].str.replace("C", "", regex=False).str.strip()

    # Normaliza valores (troca ponto dos milhares e garante "0,00")
    for col in ["Débito", "Crédito"]:
        extrato_split[col] = extrato_split[col].str.replace(".", "", regex=False)
        extrato_split[col] = extrato_split[col].fillna("0,00")

    # Mantém somente colunas necessárias
    extrato_split = extrato_split[["Data", "Histórico", "Documento", "Débito", "Crédito", "Saldo"]]

    # Chave de conciliação
    extrato_split["Chave Procx"] = extrato_split.apply(
        lambda r: f'{r["Data"]}|{r["Débito"]}|{r["Crédito"]}', axis=1
    )

    # Limpa NaN/espaços em colunas-alvo
    cols_alvo = ["Histórico", "Saldo"]
    extrato_split[cols_alvo] = (
        extrato_split[cols_alvo]
        .fillna("-")
        .apply(lambda s: s.astype(str)
                         .str.replace(r"\bnan\b", "-", flags=re.IGNORECASE, regex=True)
                         .str.replace(r"\s+", " ", regex=True)
                         .str.strip())
    )
    extrato_split["Histórico"] = extrato_split["Histórico"].str.replace("-", "", regex=False)

    # Salvar opcionalmente em disco
    if salvar_em:
        extrato_split.to_excel(salvar_em, index=False)

    return extrato_split


def tratamento_sistema_BB(xlsx_bytes, salvar_em=None):
    sistema = pd.read_excel(io.BytesIO(xlsx_bytes), dtype=str, header=None, engine="openpyxl")

    # Remove cabeçalhos/rodapés
    sistema = sistema.iloc[7:]
    sistema = sistema[~sistema.iloc[:, 3].str.contains(
        r'Total|Saldo Atual|Total Geral|Saldo Anterior|Histórico|CONTA ÚNICA|Página',
        case=False, na=False)]
    sistema = sistema[~sistema.iloc[:, 0].str.contains(r'Evaluation|Banco:|Conta:', case=False, na=False)]
    sistema = sistema[~sistema.iloc[:, 0].isna()]

    # A primeira linha após filtro vira cabeçalho
    sistema.columns = sistema.iloc[0]
    sistema = sistema.iloc[2:]  # pula linha de separador

    # Seleção e renomeação
    sistema = sistema[["NLanc", "Dtlan", "Histórico", "Debito", "Crédito", "Saldo"]]
    sistema = sistema.rename(columns={"Dtlan": "Data", "Debito": "Débito"})

    # Normaliza números em "Débito" e "Crédito"
    for col in ["Débito", "Crédito"]:
        s = pd.to_numeric(sistema[col], errors="coerce").round(2)
        sistema[col] = s.map(lambda x: f"{x:.2f}".replace(".", ",") if pd.notna(x) else "")
        sistema[col] = sistema[col].fillna("0,00")

    # Remove possíveis linhas de cabeçalho remanescentes
    sistema = sistema[~sistema["Data"].astype(str).str.contains(r'Dtlan', case=False, na=False)]

    # Chave de conciliação
    sistema["Chave Procx"] = sistema.apply(
        lambda r: f'{r["Data"]}|{r["Débito"]}|{r["Crédito"]}', axis=1
    )

    if salvar_em:
        sistema.to_excel(salvar_em, index=False)

    return sistema

def concilaicao(df_extrato, df_sistema):
    df_conciliado = pd.merge(df_extrato, df_sistema,how="inner", on = "Chave Procx",suffixes=("_Extrato","_Sistema"))
    df_conciliado = df_conciliado[["Data_Extrato","Histórico_Extrato","Débito_Extrato","Crédito_Extrato","Data_Sistema","Histórico_Sistema","Débito_Sistema","Crédito_Sistema"]]

    return df_conciliado

def to_number_brl(series: pd.Series) -> pd.Series:
    s = series.copy()

    # aplica limpeza só nas células que são string
    is_str = s.apply(lambda x: isinstance(x, str))
    s.loc[is_str] = (
        s.loc[is_str]
          .str.replace(r"\s+", "", regex=True)  # remove espaços/NBSP
          .str.replace("R$", "", regex=False)   # remove símbolo de moeda (se houver)
          .str.replace(".",  "", regex=False)   # remove separador de milhar
          .str.replace(",", ".", regex=False)   # vírgula -> ponto
    )
    # agora converte toda a coluna para número
    return pd.to_numeric(s, errors="coerce")


def write_resumo_sheet(writer, rows, sheet_name="Resumo"):
    """
    Cria a aba 'Resumo' estilizada com 3 colunas: Indicador | Mov. | Valor

    rows: lista de itens. Cada item pode ser:
      (label, value, kind)                           # compatível com a versão anterior
      (label, value, kind, flow)                     # NOVO: com movimento
    Onde:
      - kind ∈ {'money', 'text', 'money_diff'}
      - flow ∈ {'credito', 'debito', None}           # 'credito'/'debito' controlam a tag e a cor

    Exemplo:
        rows = [
          ("Total Créditos A (ERP)", 150000.00, 'money', 'credito'),
          ("Total Débitos A (ERP)",  12000.00,  'money', 'debito'),
          ("Itens conciliados",      "120 (R$ 148.900,00)", 'text', None),
          ("Só no ERP",              "8 (R$ 1.200,00)",     'text', None),
          ("Só no Banco",            "5 (R$ 850,00)",       'text', None),
          ("Diferença líquida (A–B)", 50.00, 'money_diff', 'credito'),  # ou 'debito'
        ]
    """
    wb = writer.book
    ws = wb.add_worksheet(sheet_name)

    # --- Paleta (dark)
    bg_header = "#1F2937"
    bg_row    = "#111827"
    fg_text   = "#E5E7EB"
    border    = "#374151"
    green     = "#22C55E"
    red       = "#EF4444"

    # --- Cabeçalho
    head_l = wb.add_format({"bold": True,"font_color": fg_text,"bg_color": bg_header,
                            "bottom": 1,"bottom_color": border,"align": "left","valign": "vcenter"})
    head_c = wb.add_format({"bold": True,"font_color": fg_text,"bg_color": bg_header,
                            "bottom": 1,"bottom_color": border,"align": "center","valign": "vcenter"})
    head_r = wb.add_format({"bold": True,"font_color": fg_text,"bg_color": bg_header,
                            "bottom": 1,"bottom_color": border,"align": "right","valign": "vcenter"})

    # --- Linhas padrão
    row_l = wb.add_format({"font_color": fg_text,"bg_color": bg_row,"align": "left","valign": "vcenter"})
    row_c = wb.add_format({"font_color": fg_text,"bg_color": bg_row,"align": "center","valign": "vcenter"})
    row_r = wb.add_format({"font_color": fg_text,"bg_color": bg_row,"align": "right","valign": "vcenter"})
    row_money = wb.add_format({"font_color": fg_text,"bg_color": bg_row,"align": "right",
                               "valign": "vcenter","num_format": '"R$" #,##0.00'})

    # --- Separadores sutis
    row_sep_l = wb.add_format({"font_color": fg_text,"bg_color": bg_row,"align": "left",
                               "valign": "vcenter","bottom": 1,"bottom_color": border})
    row_sep_c = wb.add_format({"font_color": fg_text,"bg_color": bg_row,"align": "center",
                               "valign": "vcenter","bottom": 1,"bottom_color": border})
    row_sep_r = wb.add_format({"font_color": fg_text,"bg_color": bg_row,"align": "right",
                               "valign": "vcenter","bottom": 1,"bottom_color": border})

    # --- Tags de movimento (Crédito/Débito)
    tag_credit = wb.add_format({"font_color": green,"bg_color": bg_row,"align": "center",
                                "valign": "vcenter","bold": True})
    tag_debit  = wb.add_format({"font_color": red,"bg_color": bg_row,"align": "center",
                                "valign": "vcenter","bold": True})

    # --- Linha de diferença (borda superior forte)
    diff_l = wb.add_format({"font_color": fg_text,"bg_color": bg_row,"align": "left",
                            "valign": "vcenter","top": 2,"top_color": border,"bold": True})
    diff_money_neutral = wb.add_format({"font_color": fg_text,"bg_color": bg_row,"align": "right",
                                        "valign": "vcenter","top": 2,"top_color": border,
                                        "bold": True,"num_format": '"R$" #,##0.00'})
    diff_money_credit  = wb.add_format({"font_color": green,"bg_color": bg_row,"align": "right",
                                        "valign": "vcenter","top": 2,"top_color": border,
                                        "bold": True,"num_format": '"R$" #,##0.00'})
    diff_money_debit   = wb.add_format({"font_color": red,"bg_color": bg_row,"align": "right",
                                        "valign": "vcenter","top": 2,"top_color": border,
                                        "bold": True,"num_format": '"R$" #,##0.00'})

    # --- Larguras
    ws.set_column("A:A", 44)   # Indicador
    ws.set_column("B:B", 10)   # Mov.
    ws.set_column("C:C", 22)   # Valor

    # Cabeçalho
    ws.set_row(0, 26)
    ws.write(0, 0, "Indicador", head_l)
    ws.write(0, 1, "Mov.",      head_c)
    ws.write(0, 2, "Valor",     head_r)
    ws.freeze_panes(1, 0)

    # --- Escrita das linhas
    last = len(rows) - 1
    r = 1
    for i, item in enumerate(rows):
        # Normaliza tuple 3× ou 4×
        if len(item) == 3:
            label, value, kind = item
            flow = None
        else:
            label, value, kind, flow = item

        # Formatos com separador (para todas menos a última)
        fmt_l = row_sep_l if i < last else row_l
        fmt_c = row_sep_c if i < last else row_c
        fmt_r = row_sep_r if i < last else row_r

        # Mov. (Crédito/Débito)
        if flow == "credito":
            mov_fmt, mov_txt = tag_credit, "CRÉD."
        elif flow == "debito":
            mov_fmt, mov_txt = tag_debit, "DÉB."
        else:
            mov_fmt, mov_txt = fmt_c, ""

        if i == last and kind == "money_diff":
            # linha de diferença com borda superior + cor pela tag
            ws.set_row(r, 24)
            ws.write(r, 0, label, diff_l)
            ws.write(r, 1, mov_txt, mov_fmt)
            if flow == "credito":
                ws.write_number(r, 2, float(value), diff_money_credit)
            elif flow == "debito":
                ws.write_number(r, 2, float(value), diff_money_debit)
            else:
                ws.write_number(r, 2, float(value), diff_money_neutral)
        else:
            ws.set_row(r, 22)
            ws.write(r, 0, label, fmt_l)
            ws.write(r, 1, mov_txt, mov_fmt)
            if kind == "money":
                ws.write_number(r, 2, float(value), row_money)
            elif kind == "text":
                ws.write(r, 2, str(value), fmt_r)
            else:
                # fallback seguro
                try:
                    ws.write_number(r, 2, float(value), row_money)
                except Exception:
                    ws.write(r, 2, str(value), fmt_r)

        r += 1


def _prep(df, col_data, col_deb, col_cred, dayfirst=True):
    df = df.copy()
    df[col_data] = pd.to_datetime(df[col_data].astype(str), dayfirst=dayfirst, errors='coerce')
    df[col_deb]  = pd.to_numeric(df[col_deb],  errors='coerce').fillna(0).round(2)
    df[col_cred] = pd.to_numeric(df[col_cred], errors='coerce').fillna(0).round(2)
    df = df[df[col_data].notna()].reset_index(drop=True)
    return df

def _make_key(df, col_data, col_deb, col_cred):
    # garante datetime aqui também
    col = pd.to_datetime(df[col_data], errors='coerce')
    return (
        col.dt.strftime('%Y-%m-%d') + '|' +
        df[col_deb].map(lambda x: f'{x:.2f}') + '|' +
        df[col_cred].map(lambda x: f'{x:.2f}')
    )

def buscar_aproximado_data_pra_frente(
    df_banco_nc, df_sis_nc,
    *,  # força nomeados
    col_data_b='Data', col_deb_b='Débito', col_cred_b='Crédito',
    col_data_s='Data', col_deb_s='Débito', col_cred_s='Crédito',
    limite_dias=10, dayfirst=True
):
    # 1) prepara e cria IDs
    b = _prep(df_banco_nc, col_data_b, col_deb_b, col_cred_b, dayfirst=dayfirst).reset_index(drop=True)
    s = _prep(df_sis_nc,   col_data_s, col_deb_s, col_cred_s, dayfirst=dayfirst).reset_index(drop=True)
    b['id_bco'] = b.index
    s['id_sis'] = s.index

    conciliados = []

    for d in range(1, limite_dias + 1):
        if b.empty or s.empty:
            break

        # chave banco (data original)
        b2 = b.copy()
        b2['_chave'] = _make_key(b2, col_data_b, col_deb_b, col_cred_b)
        b_sel = b2[['id_bco', '_chave', col_data_b, col_deb_b, col_cred_b]].rename(columns={
            col_data_b: 'data_banco',
            col_deb_b:  'debito_banco',
            col_cred_b: 'credito_banco',
        })

        # chave sistema (data + d dias) — sobrescreve a coluna de data diretamente
        s_tmp = s.copy()
        s_tmp['data_sistema_original'] = s_tmp[col_data_s]
        s_tmp[col_data_s] = s_tmp[col_data_s] + pd.to_timedelta(d, unit='D')
        s_tmp['_chave'] = _make_key(s_tmp, col_data_s, col_deb_s, col_cred_s)
        s_sel = s_tmp[['id_sis', '_chave', col_data_s, 'data_sistema_original', col_deb_s, col_cred_s]].rename(columns={
            col_data_s: 'data_sistema_ajustada',
            col_deb_s:  'debito_sistema',
            col_cred_s: 'credito_sistema',
        })

        # merge usando nomes já padronizados -> sem sufixos confusos
        m = b_sel.merge(s_sel, on='_chave', how='inner')

        if not m.empty:
            m['offset_dias'] = d
            conciliados.append(m)

            # tira IDs já usados para garantir 1-para-1
            usados_b = set(m['id_bco'])
            usados_s = set(m['id_sis'])
            b = b[~b['id_bco'].isin(usados_b)].copy()
            s = s[~s['id_sis'].isin(usados_s)].copy()

    # monta resultado
    if conciliados:
        approx = pd.concat(conciliados, ignore_index=True)
        approx = approx[['id_bco','id_sis','data_banco','debito_banco','credito_banco',
                         'data_sistema_original','data_sistema_ajustada','debito_sistema','credito_sistema',
                         'offset_dias']].sort_values(['offset_dias','data_banco']).reset_index(drop=True)
    else:
        approx = pd.DataFrame(columns=[
            'id_bco','id_sis','data_banco','debito_banco','credito_banco',
            'data_sistema_original','data_sistema_ajustada','debito_sistema','credito_sistema','offset_dias'
        ])

    pend_banco = b.drop(columns=['id_bco'])
    pend_sis   = s.drop(columns=['id_sis'])

    return approx, pend_banco, pend_sis

def procecsso(caminho_extrato, caminho_sistema):
    extrato = tratamento_extrato_bb(caminho_extrato)
    sistema = tratamento_sistema_BB(caminho_sistema)
    df_conciliado = concilaicao(extrato, sistema)

    extrato["Crédito"] = to_number_brl(extrato["Crédito"])
    extrato["Débito"]  = to_number_brl(extrato["Débito"])
    df_conciliado["Débito_Extrato"] = to_number_brl(df_conciliado["Débito_Extrato"])
    df_conciliado["Crédito_Extrato"] = to_number_brl(df_conciliado["Crédito_Extrato"])
    sistema["Crédito"] = to_number_brl(sistema["Crédito"])
    sistema["Débito"] = to_number_brl(sistema["Débito"])

    # seus cálculos
    total_credito_extrato = float(extrato["Crédito"].sum(skipna=True))
    total_debito_extrato  = float(extrato["Débito"].sum(skipna=True))
    total_credito_sistema = float(sistema["Crédito"].sum(skipna=True))
    total_debito_sistema = float(sistema["Débito"].sum(skipna=True))

    quantidade_itens_conciliados = df_conciliado.shape[0]
    itens_conciliados_debito = df_conciliado["Débito_Extrato"].sum(skipna=True)
    itens_conciliados_credito = df_conciliado["Crédito_Extrato"].sum(skipna=True)
    erp_creditos = sistema['Crédito'].sum(skipna = True)
    erp_debitos = sistema['Débito'].sum(skipna=True)
    extrato_creditos = extrato["Crédito"].sum(skipna= True)
    extrato_debito = extrato["Débito"].sum(skipna = True)
    diferenca_liquida_credito = erp_creditos-extrato_creditos
    diferenca_liquida_debito = erp_debitos-extrato_debito

    apenas_sistema = pd.merge(sistema, extrato, how="left", on= 'Chave Procx')
    apenas_sistema = apenas_sistema[apenas_sistema['Data_y'].isna()]
    apenas_sistema
    apenas_extrato = pd.merge(extrato, sistema, how="left", on= 'Chave Procx')
    apenas_extrato = apenas_extrato[apenas_extrato['Data_y'].isna()]
    apenas_extrato

    quantidade_itens_nao_conciliados_extratos = apenas_extrato.shape[0]
    quanitdade_itens_nao_conciliados_sistema = apenas_sistema.shape[0] 


    rows = [
    ("Total Créditos Base Sistema", total_credito_sistema,'money','credito'),
    ("Total Débito Base Sistema", total_debito_sistema,'money','debito'),
    ("Total Créditos Base Extrato", total_credito_extrato,'money','credito'),
    ("Total Débito Base Extrato", total_debito_extrato,'money','debito'),
    ("Quantidade de Itens Conciliados",quantidade_itens_conciliados,  'text', None),
    ("Total Itens Conciliados Crédito",itens_conciliados_credito, 'money', "credito"),
    ("Total Itens Conciliados Débito",itens_conciliados_debito,     'money', "debito"),
    ("Diferença Líquida",diferenca_liquida_credito,'money_diff', "credito"),
    ("Diferença líquida",diferenca_liquida_debito, 'money_diff', 'debito'),
    ("Itens Não Identificados - Sistema",quanitdade_itens_nao_conciliados_sistema,'text',None),
    ("Itens Não Identificados - Extrato", quantidade_itens_nao_conciliados_extratos,'text', None)
    
    ]
    aprox, pend_banco, pend_sis = buscar_aproximado_data_pra_frente(
        apenas_extrato, apenas_sistema,
        col_data_b='Data_x', col_deb_b='Débito_x', col_cred_b='Crédito_x',
        col_data_s='Data_x', col_deb_s='Débito_x', col_cred_s='Crédito_x',
        limite_dias=10  # testa +1 até +10 dias
        )

    aprox = aprox[["data_banco","debito_banco","credito_banco","data_sistema_original","debito_sistema","credito_sistema"]]
    aprox.columns = ["Data_Extrato","Débito_Extrato","Crédito_Extrato","Data_Sistema","Débito_Sistema","Crédito_Sistema"]
    df_conciliado = pd.concat([df_conciliado,aprox], ignore_index=True)

        # Dicionário para salvar os resultados do extrato
    resultado_extrato = {}
    for nome, grupos in apenas_extrato.groupby("Data_x"):
        soma_extrato_debito = round(grupos["Débito_x"].sum(), 2)
        soma_extrato_credito = round(grupos["Crédito_x"].sum(), 2)
        resultado_credito_debito = soma_extrato_debito - soma_extrato_credito
        resultado_extrato[nome] = resultado_credito_debito

    # Dicionário para salvar os resultados do sistema
    resultado_sistema = {}
    for nome, grupos in apenas_sistema.groupby("Data_x"):
        soma_sistema_debito = round(grupos["Débito_x"].sum(), 2)
        soma_sistema_credito = round(grupos["Crédito_x"].sum(), 2)
        resultado_credito_debito = soma_sistema_debito - soma_sistema_credito
        resultado_sistema[nome] = round(resultado_credito_debito, 2)

    # Lista para guardar datas que bateram
    datas_iguais = []

    # Defina a tolerância
    TOLERANCIA = 10.00

    # Comparando os dois
    for data in resultado_extrato:
        valor_extrato = resultado_extrato[data]
        valor_sistema = resultado_sistema.get(data, None)

        if valor_sistema is None:
            print(f"⚠️ Data {data} está no extrato mas não no sistema.")
        elif abs(valor_extrato - valor_sistema) <= TOLERANCIA:  # <<--- tolerância
            print(f"✅ {data} | Valores próximos (diferença ≤ {TOLERANCIA}): "
                f"Extrato={valor_extrato} | Sistema={valor_sistema}")
            datas_iguais.append(data)
        else:
            print(f"❌ {data} | Extrato: {valor_extrato} | Sistema: {valor_sistema}")


    # Extrato
    extrato_resumo = apenas_extrato.groupby("Data_x").apply(
        lambda g: round(g["Débito_x"].sum() - g["Crédito_x"].sum(), 2)
    ).reset_index(name="resultado_extrato")

    # Sistema
    sistema_resumo = apenas_sistema.groupby("Data_x").apply(
        lambda g: round(g["Débito_x"].sum() - g["Crédito_x"].sum(), 2)
    ).reset_index(name="resultado_sistema")
    comparacao = extrato_resumo.merge(sistema_resumo, on="Data_x", how="inner")

    # Apenas datas onde os resultados são iguais
    datas_ok = comparacao[
        comparacao["resultado_extrato"] == comparacao["resultado_sistema"]
    ]["Data_x"]
    # Filtra no extrato
    extrato_filtrado = apenas_extrato[apenas_extrato["Data_x"].isin(datas_ok)]
    extrato_filtrado = extrato_filtrado[["Data_x","Histórico_x","Documento","Débito_x","Crédito_x","Saldo_x"]]
    extrato_filtrado.columns = ["Data","Historico","Documento","Débito","Crédito","Saldo"]
    extrato_filtrado["Origem"] = "Extrato"
    # Filtra no sistema
    sistema_filtrado = apenas_sistema[apenas_sistema["Data_x"].isin(datas_ok)]
    sistema_filtrado = sistema_filtrado[["Data_x","Histórico_x","Documento","Débito_x","Crédito_x","Saldo_x"]]
    sistema_filtrado.columns = ["Data","Historico","Documento","Débito","Crédito","Saldo"]
    sistema_filtrado["Origem"] = "Sistema"

    # Junta os dois em um só (opcional)
    novo_dataframe = pd.concat([extrato_filtrado, sistema_filtrado], ignore_index=True)

        # Remove as datas que bateram dos DataFrames de não identificados
    apenas_extrato = apenas_extrato[~apenas_extrato["Data_x"].isin(datas_iguais)]
    apenas_sistema = apenas_sistema[~apenas_sistema["Data_x"].isin(datas_iguais)]

    apenas_extrato = apenas_extrato[["Data_x","Histórico_x","Documento","Débito_x","Crédito_x","Saldo_x"]]
    apenas_extrato.columns = ["Data","Historico","Documento","Débito","Crédito","Saldo"]
    apenas_extrato = apenas_extrato[~apenas_extrato.iloc[:,1].str.contains(r'500 Tar DOC/TED', case=False, na=False)] 

    apenas_sistema = apenas_sistema[["Data_x","Histórico_x","Documento","Débito_x","Crédito_x","Saldo_x"]]
    apenas_sistema.columns = ["Data","Historico","Documento","Débito","Crédito","Saldo"]

    buffer = io.BytesIO()

    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        df_conciliado.to_excel(writer, sheet_name="Valores Exatos Conciliados", index=False)
        novo_dataframe.to_excel(writer, sheet_name="Identificados Por Soma", index=False)
        apenas_extrato.to_excel(writer, sheet_name="Não Identificados-Extrato", index=False)
        apenas_sistema.to_excel(writer, sheet_name="Não Identificados-Sistema", index=False)

        
    buffer.seek(0)
    return buffer.getvalue()  # bytes prontos para download
    

