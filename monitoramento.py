import pandas as pd
import os
from fastapi import FastAPI, Query, HTTPException
from azure.monitor.query import LogsQueryClient
from azure.identity import DefaultAzureCredential
from datetime import timedelta
from typing import Literal, List, Dict, Any
import uvicorn  # Para rodar localmente

# 2. Inicialização do FastAPI
app = FastAPI(
    title="Monitoramento",
    description="Um backend que consulta o Application Insights usando Identidade Gerenciada."
)

# Pega o Workspace ID das "Configurações do Aplicativo" do App Service
app_insights_workspace_id = os.environ.get("LOG_ANALYTICS_WORKSPACE_ID")
if not app_insights_workspace_id:
    print("AVISO: Variável de ambiente LOG_ANALYTICS_WORKSPACE_ID não definida.")

credential = DefaultAzureCredential()
logs_client = LogsQueryClient(credential)


# --- FUNÇÃO ATUALIZADA ---
def run_analytics_query(
    dias: int, 
    coluna_alvo: str,  # --- CORRIGIDO (nome) ---
    operacao: str, 
    nome_coluna: str, 
    agrupar_por_modelo: bool,
    analisar_por_job: bool,
    resultado_diario: bool
) -> List[Dict[str, Any]]:
    """
    Executa uma query parametrizada no Log Analytics Workspace.
    """
    
    # 1. Colunas de agrupamento base
    group_by_columns = ["projeto", "usuario_executor"]
    if agrupar_por_modelo:
        group_by_columns.append("model_name")

    # 2. Lista de expressões 'extend'
    extend_expressions = [
        "projeto = tostring(msg_data.projeto)",
        "usuario_executor = tostring(msg_data.usuario_executor)",
        "model_name = tostring(msg_data.model_name)",
        f"{coluna_alvo} = todouble(msg_data.{coluna_alvo})"
    ]
    
    daily_order_by_kql = ""

    if resultado_diario:
        daily_bin_column = "dia"
        extend_expressions.append(f"{daily_bin_column} = bin(todatetime(msg_data.data_hora), 1d)")
        group_by_columns.append(daily_bin_column)
        daily_order_by_kql = f"{daily_bin_column} asc, "

    # 3. Cláusula de agrupamento final (Estágio 2)
    group_by_clause = ", ".join(group_by_columns)
    
    # 4. Cláusula de ordenação final
    order_by_clause = f"projeto asc, {daily_order_by_kql}{nome_coluna} desc"
    
    kql_query = ""
    
    if not analisar_por_job:
        # --- LÓGICA PADRÃO: Agregação por evento ---
        extend_clause = ",\n            ".join(extend_expressions)
        
        # --- CORRIGIDO (coluna_alvo) ---
        kql_query = f"""
        AppTraces 
        | extend msg_data = parse_json(Message)
        | extend
            {extend_clause}
        | where isnotnull({coluna_alvo}) 
        | summarize
            {nome_coluna} = {operacao}({coluna_alvo})
            by {group_by_clause}
        | order by {order_by_clause}
        """
    else:
        # --- NOVA LÓGICA: Agregação por Job (2 Estágios) ---
        if operacao == "sum":
             raise HTTPException(status_code=400, 
                detail="A operação 'sum' não é permitida com 'analisar_por_job=True', pois a agregação primária já é uma soma.")
        
        extend_expressions.append("job_id = tostring(msg_data.job_id)")
        extend_clause = ",\n            ".join(extend_expressions)
        
        stage_1_groupby_columns = group_by_columns + ["job_id"]
        stage_1_groupby_clause = ", ".join(stage_1_groupby_columns)
        
        # --- CORRIGIDO (coluna_alvo) ---
        kql_query = f"""
        AppTraces 
        | extend msg_data = parse_json(Message)
        | extend
            {extend_clause}
        | where isnotnull({coluna_alvo}) and isnotnull(job_id)
        
        // Estágio 1: "soma do job_id" (Soma os tokens para cada job E dia)
        | summarize 
            job_token_total = sum
