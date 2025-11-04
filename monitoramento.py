import pandas as pd
import os
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware  # <<< 1. IMPORTAR O MIDDLEWARE
from azure.monitor.query import LogsQueryClient
from azure.identity import DefaultAzureCredential
from datetime import timedelta
from typing import Literal, List, Dict, Any
import uvicorn

# 2. Inicialização do FastAPI
app = FastAPI(
    title="Monitoramento",
    description="Um backend que consulta o Application Insights usando Identidade Gerenciada."
)

# 3. CONFIGURAÇÃO DO CORS (Adicionar este bloco) # <<< 2. ADICIONAR ESTE BLOCO
# -----------------------------------------------------------------
origins = [
    "http://localhost:8000",  # O seu servidor Python local
    "http://127.0.0.1:8000", # Outra forma de acessar o localhost
    # "https://seu-site-em-producao.com", # Adicione a URL do seu frontend quando for para produção
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,       # Quais origens podem fazer requisições
    allow_credentials=True,    # Permitir cookies (se necessário)
    allow_methods=["*"],       # Quais métodos são permitidos (GET, POST, etc.)
    allow_headers=["*"],       # Quais headers são permitidos
)
# ----------------------------------------------------------------- # <<< FIM DO BLOCO

# Pega o Workspace ID das "Configurações do Aplicativo" do App Service
app_insights_workspace_id = os.environ.get("LOG_ANALYTICS_WORKSPACE_ID")
if not app_insights_workspace_id:
    print("AVISO: Variável de ambiente LOG_ANALYTICS_WORKSPACE_ID não definida.")

credential = DefaultAzureCredential()
logs_client = LogsQueryClient(credential)


# --- FUNÇÃO ATUALIZADA (Lógica de query reescrita) ---
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
    
    # --- LÓGICA DE CONSTRUÇÃO DE QUERY ATUALIZADA ---

    # 1. Colunas de agrupamento base (Grupo para 'summarize')
    group_by_columns = ["projeto"] # 'projeto' é obrigatório

    # 2. Expressões de 'extend' (para criar as colunas)
    extend_expressions = ["projeto = tostring(msg_data.projeto)"]

    # 3. Condições de 'where' (Filtro 'not null')
    # REGRA NOVA: 'projeto' é sempre obrigatório
    where_conditions = ["isnotnull(msg_data.projeto)"]

    # ---
    # REGRA NOVA: Adiciona 'usuario_executor' ao 'extend' e 'group_by',
    # mas NÃO ao 'where'. É a única exceção.
    group_by_columns.append("usuario_executor")
    extend_expressions.append("usuario_executor = tostring(msg_data.usuario_executor)")
    # ---

    # 4. Lógica condicional para 'model_name'
    if agrupar_por_modelo:
        group_by_columns.append("model_name")
        extend_expressions.append("model_name = tostring(msg_data.model_name)")
        # REGRA NOVA: Se agrupar, deve ser not null
        where_conditions.append("isnotnull(msg_data.model_name)") 

    # 5. Lógica condicional para 'resultado_diario'
    daily_order_by_kql = ""
    if resultado_diario:
        daily_bin_column = "dia"
        group_by_columns.append(daily_bin_column)
        extend_expressions.append(f"{daily_bin_column} = bin(todatetime(msg_data.data_hora), 1d)")
        # REGRA NOVA: Se agrupar por dia, a data não pode ser nula
        where_conditions.append("isnotnull(msg_data.data_hora)")
        daily_order_by_kql = f"{daily_bin_column} asc, "

    # 6. Lógica condicional para 'coluna_alvo' (Métrica)
    if coluna_alvo == "job_id":
        # Se o alvo for 'job_id', a operação é uma contagem distinta
        if operacao not in ["count", "dcount"]:
                raise HTTPException(status_code=400, 
                    detail="A operação para 'coluna_alvo=job_id' deve ser 'count' ou 'dcount'.")
        
        operacao = "dcount" # Força a contagem de jobs únicos
        nome_coluna = "Contagem_Jobs_Unicos" # Sobrescreve o nome da coluna
        extend_expressions.append(f"{coluna_alvo} = tostring(msg_data.{coluna_alvo})")
        where_conditions.append(f"isnotnull(msg_data.{coluna_alvo})") # REGRA NOVA
        analisar_por_job = False # Desativa a lógica de 2 estágios
    else:
        # Lógica padrão para colunas numéricas (tokens)
        extend_expressions.append(f"{coluna_alvo} = todouble(msg_data.{coluna_alvo})")
        where_conditions.append(f"isnotnull(msg_data.{coluna_alvo})") # REGRA NOVA
        
    # 7. Cláusulas Finais
    group_by_clause = ", ".join(group_by_columns)
    order_by_clause = f"projeto asc, {daily_order_by_kql}{nome_coluna} desc"
    
    kql_query = ""
    
    if not analisar_por_job:
        # --- LÓGICA PADRÃO ---
        extend_clause = ",\n            ".join(extend_expressions)
        where_clause = " and ".join(where_conditions) # Cláusula 'where' dinâmica
        
        kql_query = f"""
        AppTraces 
        | extend msg_data = parse_json(Message)
        | extend
            {extend_clause}
        | where {where_clause}
        | summarize
            {nome_coluna} = {operacao}({coluna_alvo})
            by {group_by_clause}
        | order by {order_by_clause}
        """
    else:
        # --- LÓGICA POR JOB ---
        if operacao == "sum":
                raise HTTPException(status_code=400, 
                    detail="A operação 'sum' não é permitida com 'analisar_por_job=True'.")
        
        # Adiciona 'job_id' às cláusulas
        extend_expressions.append("job_id = tostring(msg_data.job_id)")
        where_conditions.append("isnotnull(msg_data.job_id)") # REGRA NOVA
        
        extend_clause = ",\n            ".join(extend_expressions)
        where_clause = " and ".join(where_conditions) # Cláusula 'where' dinâmica

        stage_1_groupby_columns = group_by_columns + ["job_id"]
        stage_1_groupby_clause = ", ".join(stage_1_groupby_columns)
        
        kql_query = f"""
        AppTraces 
        | extend msg_data = parse_json(Message)
        | extend
            {extend_clause}
        | where {where_clause}
        
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


# --- ENDPOINT ATUALIZADO (Corrigido o bug 'token' vs 'coluna_alvo') ---
@app.get("/get_token_stats", response_model=List[Dict[str, Any]])
async def get_stats(
    dias: int = Query(default=30, gt=0, description="Período de análise em dias."),
    
    coluna_alvo: Literal["tokens_entrada", "tokens_saida", "job_id"] = Query(
        default="tokens_entrada",
        description="A coluna de métrica a ser analisada. 'job_id' força uma contagem de jobs únicos."
    ),
    
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
    - `analisar_por_job=False`: Calcula a agregação por evento.
    - `analisar_por_job=True`: Calcula a agregação por job (ex: avg(sum(job))).
    - `resultado_diario=True`: Adiciona o 'dia' ao agrupamento final.
    """
    
    coluna_saida = ""
    # --- CORRIGIDO: Bug de nome de variável (usava 'token' que não existe mais) ---
    if coluna_alvo == "job_id":
        coluna_saida = "Contagem_Jobs_Unicos"
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
