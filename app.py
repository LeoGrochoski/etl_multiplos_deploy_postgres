import streamlit as st
from pipeline_01 import pipeline

st.title("Processador de Arquivos")

if st.button("Processar"):
    with st.spinner("Processando"):
        logs = pipeline()
        for log in logs:
            st.write(log)