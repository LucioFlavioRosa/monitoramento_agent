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
    coluna_alvo: str, 
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
        "model_name = tostring(msg_data.model_name)"
    ]
    
    daily_order_by_kql = ""

    if resultado_diario:
        daily_bin_column = "dia"
        extend_expressions.append(f"{daily_bin_column} = bin(todatetime(msg_data.data_hora), 1d)")
        group_by_columns.append(daily_bin_column)
        daily_order_by_kql = f"{daily_bin_column} asc, "

    # --- LÓGICA ATUALIZADA PARA TRATAR job_id ---
    
    # Se a coluna alvo for job_id, é uma contagem distinta (dcount), não uma operação matemática.
    if coluna_alvo == "job_id":
        if operacao not in ["count", "dcount"]:
             raise HTTPException(status_code=400, 
                detail="A operação para 'coluna_alvo=job_id' deve ser 'count' (para contar jobs únicos).")
        
        # Força a operação para 'dcount' (distinct count)
        operacao = "dcount" 
        nome_coluna = "Contagem_Jobs_Unicos" # Sobrescreve o nome da coluna de saída
        # A coluna é um 'tostring', não 'todouble'
        extend_expressions.append(f"{coluna_alvo} = tostring(msg_data.{coluna_alvo})")
        # A análise por job não faz sentido se já estamos contando jobs
        analisar_por_job = False 
    else:
        # Lógica original para colunas numéricas (tokens_entrada, tokens_saida)
        extend_expressions.append(f"{coluna_alvo} = todouble(msg_data.{coluna_alvo})")


    # 3. Cláusula de agrupamento final
    group_by_clause = ", ".join(group_by_columns)
    
    # 4. Cláusula de ordenação final
    order_by_clause = f"projeto asc, {daily_order_by_kql}{nome_coluna} desc"
    
    kql_query = ""
    
    if not analisar_por_job:
        # --- LÓGICA PADRÃO: Agregação por evento (ou dcount de job_id) ---
        extend_clause = ",\n            ".join(extend_expressions)
        
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
        # --- LÓGICA POR JOB (só roda se coluna_alvo NÃO for 'job_id') ---
        if operacao == "sum":
             raise HTTPException(status_code=400, 
                detail="A operação 'sum' não é permitida com 'analisar_por_job=True'.")
        
        extend_expressions.append("job_id = tostring(msg_data.job_id)")
        extend_clause = ",\n            ".join(extend_expressions)
        
        stage_1_groupby_columns = group_by_columns + ["job_id"]
        stage_1_groupby_clause = ", ".join(stage_1_groupby_columns)
        
        kql_query = f"""
        AppTraces 
        | extend msg_data = parse_json(Message)
        | extend
            {extend_clause}
        | where isnotnull({coluna_alvo}) and isnotnull(job_id)
        
        | summarize 
            job_token_total = sum({coluna_alvo}) 
            by {stage_1_groupby_clause}
            
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
    dias: int = Query(default=30, gt=0, description="Período de análise em dias."),
    
    # O Literal ainda aceita job_id, mas a lógica interna irá tratá-lo de forma especial
    coluna_alvo: Literal["tokens_entrada", "tokens_saida", "job_id"] = Query(
        default="tokens_entrada",
        description="A coluna de métrica a ser analisada. 'job_id' força uma contagem de jobs únicos."
    ),
    
    # Adicionei 'dcount' às opções, embora o código o force para 'job_id'
    op: Literal["avg", "sum", "count", "min", "max", "dcount"] = Query(
        default="avg",
        description="A operação de agregação. Se coluna_alvo='job_id', 'count' ou 'dcount' deve ser usado."
    ),
    
    agrupar_por_modelo: bool = Query(
        default=False,
        description="Se True, agrupa os resultados também por 'model_name'."
    ),
    
    analisar_por_job: bool = Query(
        default=False,
        description="Se True, analisa por total de Job (ex: média de Jobs). Ignorado se coluna_alvo='job_id'."
    ),
    
    resultado_diario: bool = Query(
        default=False,
        description="Se True, agrupa os resultados por dia (usando 'data_hora' do JSON)."
    )
):
    """
    Executa uma análise agregada.
    - Se coluna_alvo for 'tokens_entrada' ou 'tokens_saida', executa a operação (avg, sum, etc).
    - Se coluna_alvo for 'job_id', executa uma contagem de jobs únicos (dcount), ignorando 'analisar_por_job'.
    """
    
    coluna_saida = ""
    # --- LÓGICA ATUALIZADA PARA NOME DA COLUNA ---
    if coluna_alvo == "job_id":
        coluna_saida = "Contagem_Jobs_Unicos" # Define um nome de coluna fixo e claro
    else:
        coluna_saida = f"{op.capitalize()}_{coluna_alvo}"
    
    results = run_analytics_query(
        dias, coluna_alvo, op, coluna_saida, 
        agrupar_por_modelo, analisar_por_job, resultado_diario
    )
    
    return results


# 6. Ponto de Entrada para Teste Local
if __name__ == "__main__":
    print("--- Rodando em modo de teste local ---")
    print("--- Certifique-se de ter rodado 'az login' no seu terminal ---")
    uvicorn.run(app, host="0.0.0.0", port=8000)
