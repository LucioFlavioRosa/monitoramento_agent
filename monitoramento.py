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
    # Você pode definir um valor padrão para teste local se não quiser usar 'az login'
    # app_insights_workspace_id = "SEU_WORKSPACE_ID_PARA_TESTE_LOCAL"

credential = DefaultAzureCredential()
logs_client = LogsQueryClient(credential)


# --- FUNÇÃO ATUALIZADA ---
def run_analytics_query(dias: int, coluna_token: str, operacao: str, nome_coluna: str, agrupar_por_modelo: bool) -> List[Dict[str, Any]]:
    """
    Executa uma query parametrizada no Log Analytics Workspace.
    """
    
    # Constrói dinamicamente a cláusula de agrupamento (by)
    group_by_columns = ["projeto", "usuario_executor"]
    if agrupar_por_modelo:
        group_by_columns.append("model_name")
        
    # Converte a lista de colunas em uma string para o KQL
    group_by_clause = ", ".join(group_by_columns)
    
    
    # --- CORREÇÃO AQUI ---
    # Removidos os comentários Python (#) que estavam dentro da string KQL.
    kql_query = f"""
    AppTraces 
    | extend msg_data = parse_json(Message)
    | extend
        projeto = tostring(msg_data.projeto),
        usuario_executor = tostring(msg_data.usuario_executor),
        model_name = tostring(msg_data.model_name),
        {coluna_token} = todouble(msg_data.{coluna_token}) 
    | where isnotnull({coluna_token})
    | summarize
        {nome_coluna} = {operacao}({coluna_token})
        by {group_by_clause}
    | order by projeto asc, {nome_coluna} desc
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
            return df.to_dict('records')  # Retorna uma lista de dicionários
        else:
            return [] 

    except Exception as e:
        # Se a query falhar (ex: erro de sintaxe), retorna um erro 500
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
    )
):
    """
    Executa uma análise agregada dos tokens de entrada ou saída
    agrupados por projeto e usuário (e opcionalmente por modelo).
    """
    
    coluna_saida = f"{op.capitalize()}_{token}"  # Ex: "Avg_tokens_entrada"
    
    # Chama a função de lógica (FastAPI rodará isso em um thread pool)
    results = run_analytics_query(dias, token, op, coluna_saida, agrupar_por_modelo)
    
    # FastAPI converte o retorno (lista de dicts) em JSON automaticamente
    return results


# 6. Ponto de Entrada para Teste Local
if __name__ == "__main__":
    print("--- Rodando em modo de teste local ---")
    print("--- Certifique-se de ter rodado 'az login' no seu terminal ---")
    uvicorn.run(app, host="0.0.0.0", port=8000)
