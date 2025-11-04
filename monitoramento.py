# 1. Imports
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
    coluna_token: str, 
    operacao: str, 
    nome_coluna: str, 
    agrupar_por_modelo: bool,
    analisar_por_job: bool,
    resultado_diario: bool # --- PARÂMETRO ADICIONADO ---
) -> List[Dict[str, Any]]:
    """
    Executa uma query parametrizada no Log Analytics Workspace.
    """
    
    # --- LÓGICA DE CONSTRUÇÃO DE QUERY ATUALIZADA ---

    # 1. Colunas de agrupamento base
    group_by_columns = ["projeto", "usuario_executor"]
    if agrupar_por_modelo:
        group_by_columns.append("model_name")

    # 2. Strings de KQL para agrupamento diário
    daily_bin_extend_kql = ""  # Parte a ser adicionada no 'extend'
    daily_order_by_kql = ""    # Parte a ser adicionada no 'order by'

    if resultado_diario:
        daily_bin_column = "dia"
        # Adiciona a expressão de 'bin' para o 'extend'
        daily_bin_extend_kql = f", {daily_bin_column} = bin(timestamp, 1d)"
        # Adiciona a coluna 'dia' ao agrupamento
        group_by_columns.append(daily_bin_column)
        # Adiciona o 'dia' à ordenação
        daily_order_by_kql = f"{daily_bin_column} asc, "

    # 3. Cláusula de agrupamento final (Estágio 2)
    group_by_clause = ", ".join(group_by_columns)
    
    # 4. Cláusula de ordenação final
    order_by_clause = f"projeto asc, {daily_order_by_kql}{nome_coluna} desc"
    
    kql_query = ""
    
    if not analisar_por_job:
        # --- LÓGICA PADRÃO: Agregação por evento ---
        kql_query = f"""
        AppTraces 
        | extend msg_data = parse_json(Message)
        | extend
            projeto = tostring(msg_data.projeto),
            usuario_executor = tostring(msg_data.usuario_executor),
            model_name = tostring(msg_data.model_name),
            {coluna_token} = todouble(msg_data.{coluna_token})
            {daily_bin_extend_kql} 
        | where isnotnull({coluna_token})
        | summarize
            {nome_coluna} = {operacao}({coluna_token})
            by {group_by_clause}
        | order by {order_by_clause}
        """
    else:
        # --- NOVA LÓGICA: Agregação por Job (2 Estágios) ---
        if operacao == "sum":
             raise HTTPException(status_code=400, 
                detail="A operação 'sum' não é permitida com 'analisar_por_job=True', pois a agregação primária já é uma soma.")
        
        # O agrupamento do Estágio 1 precisa do job_id
        stage_1_groupby_columns = group_by_columns + ["job_id"]
        stage_1_groupby_clause = ", ".join(stage_1_groupby_columns)
        
        kql_query = f"""
        AppTraces 
        | extend msg_data = parse_json(Message)
        | extend
            projeto = tostring(msg_data.projeto),
            usuario_executor = tostring(msg_data.usuario_executor),
            model_name = tostring(msg_data.model_name),
            job_id = tostring(msg_data.job_id),
            {coluna_token} = todouble(msg_data.{coluna_token})
            {daily_bin_extend_kql} 
        | where isnotnull({coluna_token}) and isnotnull(job_id)
        
        // Estágio 1: "soma do job_id" (Soma os tokens para cada job E dia)
        | summarize 
            job_token_total = sum({coluna_token}) 
            by {stage_1_groupby_clause}
            
        // Estágio 2: Aplica a operação principal (ex: avg) sobre os totais de cada job, por dia
        | summarize
            {nome_coluna} = {operacao}(job_token_total)
            by {group_by_clause}
            
        | order by {order_by_clause}
        """

    if not app_insights_workspace_id:
        raise HTTPException(status_code=500, detail="LOG_ANALYTICS_WORKSPACE_ID não está configurado no servidor.")

    try:
        response = logs_client.query_workspace(
            workspace_id=app_insights_workspace_id,
            query=kql_query,
            timespan=timedelta(days=dias)
        )
        
        if response.tables:
            df = pd.DataFrame(data=response.tables[0].rows, columns=response.tables[0].columns)
            return df.to_dict('records')
        else:
            return [] 

    except Exception as e:
        print(f"Ocorreu um erro ao executar a query: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar o Log Analytics: {str(e)}")


# --- ENDPOINT ATUALIZADO ---
@app.get("/get_token_stats", response_model=List[Dict[str, Any]])
async def get_stats(
    # Validação automática de parâmetros de query:
    dias: int = Query(default=30, gt=0, description="Período de análise em dias."),
    
    token: Literal["tokens_entrada", "tokens_saida"] = Query(
        default="tokens_entrada",
        description="O tipo de token a ser analisado."
    ),
    
    op: Literal["avg", "sum", "count", "min", "max"] = Query(
        default="avg",
        description="A operação de agregação a ser executada."
    ),
    
    agrupar_por_modelo: bool = Query(
        default=False,
        description="Se True, agrupa os resultados também por 'model_name'."
    ),
    
    analisar_por_job: bool = Query(
        default=False,
        description="Se True, analisa por total de Job (ex: média de Jobs) em vez de por evento."
    ),
    
    # --- PARÂMETRO ADICIONADO ---
    resultado_diario: bool = Query(
        default=False,
        description="Se True, agrupa os resultados por dia (bin(timestamp, 1d))."
    )
):
    """
    Executa uma análise agregada dos tokens.
    - `analisar_por_job=False`: Calcula a agregação por evento.
    - `analisar_por_job=True`: Calcula a agregação por job (ex: avg(sum(job))).
    - `resultado_diario=True`: Adiciona o 'dia' ao agrupamento final.
    """
    
    coluna_saida = f"{op.capitalize()}_{token}"
    
    # --- ATUALIZADO: Passa o novo parâmetro ---
    results = run_analytics_query(
        dias, token, op, coluna_saida, 
        agrupar_por_modelo, analisar_por_job, resultado_diario
    )
    
    return results


# 6. Ponto de Entrada para Teste Local
if __name__ == "__main__":
    print("--- Rodando em modo de teste local ---")
    print("--- Certifique-se de ter rodado 'az login' no seu terminal ---")
    uvicorn.run(app, host="0.0.0.0", port=8000)
