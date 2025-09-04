import streamlit as st
import conciliacao_v1 as  cc
from converter import convert_pdf_bytes_to_xlsx_bytes, sniff_output_filename, ConversionError

import time 

st.title("Conciliação Bancária")

col1, col2, col3 = st.columns(3)
extrato = st.file_uploader("Selecione o Extrato","pdf")
sistema = st.file_uploader("Selecione o Arquivo do Sistema","pdf")
banco = col3.selectbox("Selecione o Banco:", options=["Banco do Brasil","Caixa Econômica"])


if extrato is not None and sistema is not None:
    iniciar = st.button("Iniciar Processo")

    if iniciar:
        pdf_bytes = extrato.read()
        pdf2_bytes = sistema.read()
        try:
            xlsx_bytes = convert_pdf_bytes_to_xlsx_bytes(pdf_bytes, minimize_worksheets=True)
            extrato_planilha = xlsx_bytes
            xlsx_bytes_sistema =  convert_pdf_bytes_to_xlsx_bytes(pdf2_bytes, minimize_worksheets=True)
            sistema_planilha = xlsx_bytes_sistema

            planilha_final = cc.procecsso(extrato_planilha,sistema_planilha)
            st.download_button(
                label="📥 Baixar conciliação.xlsx",
                data=planilha_final,
                file_name=f"Conciliação_{banco}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        except ConversionError as e:
            st.error("Erro na conversão: %s" % e)
        except Exception as e:
            st.error("Erro inesperado: %s" % e)





