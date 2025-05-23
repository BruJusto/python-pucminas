#  Aula Prática – Integração entre PostgreSQL (Pagila) e APIs com Python

# Nesta atividade, você irá:
# - Consultar dados da base Pagila com `psycopg2`
# - Integrar dados climáticos e populacionais usando APIs externas
# - Analisar, transformar e visualizar os dados
# - Praticar o uso de Pandas, APIs, SQL e operações avançadas como `lambda`, `groupby`, `merge`, entre outras


import os

import openpyxl
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import requests
import seaborn as sns
import time
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Carregar variáveis de ambiente
load_dotenv()

# String de conexão
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
db = os.getenv("PG_DB")
api_key = os.getenv("WEATHER_KEY")
airvisual_key = os.getenv("AIRVISUAL_KEY")


engine = psycopg2.connect(f"postgresql://{user}:{password}@{host}/{db}?sslmode=require")


def run_query(sql, coon=engine):
    """
    Executes a SQL query on the provided database connection and returns the result as a DataFrame.

    Args:
        sql (str): The SQL query to be executed.
        coon (sqlalchemy.engine.base.Connection or sqlite3.Connection): The database connection object.

    Returns:
        pandas.DataFrame: A DataFrame containing the results of the SQL query.
    """
    return pd.read_sql_query(sql, coon)

## https://github.com/devrimgunduz/pagila/tree/master 

# Dicionário de cache no formato {cidade: (timestamp, temperatura)}
cache_clima = {}
TEMPO_EXPIRACAO = 1800  # 30 minutos

# Cache no formato {(cidade, estado, pais): (timestamp, aqi)}
cache_aqi = {}
TEMPO_EXPIRACAO_AQI = 3600  # 1 hora

def buscar_clima(cidade):
    agora = time.time()
    
    # Verifica se cidade está em cache e ainda é válida
    if cidade in cache_clima:
        timestamp, temp = cache_clima[cidade]
        if agora - timestamp < TEMPO_EXPIRACAO:
            return temp  # Retorna do cache
    
    # Caso contrário, faz a chamada à API
    try:
        url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={cidade}"
        resposta = requests.get(url, timeout=10)
        temp = resposta.json()["current"]["temp_c"]
        
        # Salva no cache
        cache_clima[cidade] = (agora, temp)
        return temp
    
    except Exception as e:
        print(f"Erro ao buscar temperatura de {cidade}: {e}")
        return None
    

def buscar_qualidade_ar(cidade, estado, pais):
    chave = (cidade.lower(), estado.lower(), pais.lower())
    agora = time.time()

    # Verifica se já está no cache e se ainda é válido
    if chave in cache_aqi:
        timestamp, aqi = cache_aqi[chave]
        if agora - timestamp < TEMPO_EXPIRACAO_AQI:
            return aqi  # Retorna do cache

    try:
        url = (
            f"http://api.airvisual.com/v2/city?"
            f"city={requests.utils.quote(cidade)}&"
            f"state={requests.utils.quote(estado)}&"
            f"country={requests.utils.quote(pais)}&"
            f"key={airvisual_key}"
        )
        resposta = requests.get(url, timeout=15)
        resposta.raise_for_status()
        dados = resposta.json()

        if dados.get("status") == "success" and dados.get("data", {}).get("current", {}).get("pollution"):
            aqi = dados["data"]["current"]["pollution"]["aqius"]
            cache_aqi[chave] = (agora, aqi)  # Armazena no cache
            return aqi
        else:
            return None
    except Exception as e:
        print(f"Erro inesperado ao buscar AQI para {cidade}, {estado}, {pais}: {e}")
        return None

#   Exercício 1 – Temperatura Média das Capitais dos Clientes
# 	•	Recupere as cidades dos clientes com mais de 10 transações.
# 	•	Use a WeatherAPI para buscar a temperatura atual dessas cidades.
# 	•	Calcule a temperatura média ponderada por número de clientes.
# 	•	Insight esperado: quais cidades concentram clientes e temperaturas extremas?

# Cidades com mais transações

query_1 = '''
SELECT ci.city, COUNT(c.customer_id) AS num_clientes
FROM payment p
JOIN customer c ON p.customer_id = c.customer_id
JOIN address a ON c.address_id = a.address_id
JOIN city ci ON a.city_id = ci.city_id
GROUP BY ci.city
HAVING COUNT(p.payment_id) > 10
ORDER BY num_clientes DESC limit 100
'''
df_cidades = run_query(query_1)

# Adiciona coluna de temperatura
df_cidades["temperatura"] = df_cidades["city"].apply(buscar_clima)

# Remove cidades sem temperatura
df_cidades = df_cidades.dropna(subset=["temperatura"])

# Temperatura média ponderada
temperatura_ponderada = (df_cidades["temperatura"] * df_cidades["num_clientes"]).sum() / df_cidades["num_clientes"].sum()

print(f"\nTemperatura média ponderada: {temperatura_ponderada:.2f}°C")

# Exibir cidades com temperaturas mais extremas
print("\nCidades com maiores temperaturas:")
print(df_cidades.sort_values(by="temperatura", ascending=False).head(10))

input("Press enter to continue")

# ⸻

#   Exercício 2 – Receita Bruta em Cidades com Clima Ameno
# 	•	Calcule a receita bruta por cidade.
# 	•	Use a WeatherAPI para consultar a temperatura atual.
# 	•	Filtre apenas cidades com temperatura entre 18°C e 24°C.
# 	•	Resultado: qual o faturamento total vindo dessas cidades?

query = '''
SELECT ci.city, SUM(p.amount) AS receita_total
FROM payment p
JOIN customer c ON p.customer_id = c.customer_id
JOIN address a ON c.address_id = a.address_id
JOIN city ci ON a.city_id = ci.city_id
GROUP BY ci.city
ORDER BY receita_total DESC limit 100
'''
df_receita = run_query(query)

def ex2_sequencial(run_query, query, get_temperatura,df_receita):

# Adiciona coluna de temperatura
    df_receita["temperatura"] = df_receita["city"].apply(get_temperatura)

# Remove cidades sem temperatura
    df_receita = df_receita.dropna(subset=["temperatura"])

# Filtra cidades com clima ameno (18°C a 24°C)
    df_ameno = df_receita[(df_receita["temperatura"] >= 18) & (df_receita["temperatura"] <= 24)]

# Soma da receita total dessas cidades
    total_ameno = df_ameno["receita_total"].sum()

    print(f"Receita total em cidades com temperatura entre 18°C e 24°C: ${total_ameno:.2f}")
    print("\nCidades com clima ameno e suas receitas:")
    print(df_ameno.sort_values(by="receita_total", ascending=False))
    return df_receita


start = time.time()
df_receita = ex2_sequencial(run_query, query, buscar_clima,df_receita)
end = time.time()
print(f"Tempo de execução: {end - start:.2f} segundos")
#   Exercício 2 -- PARELELO – Receita Bruta em Cidades com Clima Ameno


def get_temperatura_paralelo(cidade):
    api_key = os.getenv("WEATHER_KEY")
    try:
        r = requests.get(f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={cidade}", timeout=5)
        return cidade, r.json()["current"]["temp_c"]
    except:
        return cidade, None

# Executa chamadas paralelas com ThreadPoolExecutor
def ex2_parealelo(get_temperatura, df_receita):
    cidades = df_receita["city"].unique()
    resultados = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(get_temperatura, cidade): cidade for cidade in cidades}
        for future in as_completed(futures):
            cidade, temp = future.result()
            resultados[cidade] = temp

# Mapeia resultados no DataFrame original
    df_receita["temperatura"] = df_receita["city"].map(resultados)

# Remove cidades sem temperatura
    df_receita = df_receita.dropna(subset=["temperatura"])

# Filtra cidades com clima ameno (18°C a 24°C)
    df_ameno = df_receita[(df_receita["temperatura"] >= 18) & (df_receita["temperatura"] <= 24)]

# Soma da receita total dessas cidades
    total_ameno = df_ameno["receita_total"].sum()

    print(f"Receita total em cidades com temperatura entre 18°C e 24°C: ${total_ameno:.2f}")
    print("\nCidades com clima ameno e suas receitas:")
    print(df_ameno.sort_values(by="receita_total", ascending=False))


start = time.time()
ex2_parealelo(get_temperatura_paralelo, df_receita)
end = time.time()
print(f"Tempo de execução: {end - start:.2f} segundos")

input("Press enter to continue")

# ⸻

#   Exercício 3 – Aluguel de Filmes por Região e População
# 	•	Identifique os países dos clientes com maior número de aluguéis.
# 	•	Use a REST Countries API para obter a população desses países.
# 	•	Calcule o número de aluguéis por 1.000 habitantes.
# 	•	Análise: quais países são mais “cinéfilos” proporcionalmente?

# Consulta ao número de aluguéis por país
query_3 = '''
SELECT co.country, COUNT(r.rental_id) AS total_alugueis
FROM rental r
JOIN customer c ON r.customer_id = c.customer_id
JOIN address a ON c.address_id = a.address_id
JOIN city ci ON a.city_id = ci.city_id
JOIN country co ON ci.country_id = co.country_id
GROUP BY co.country
ORDER BY total_alugueis DESC
'''

df_alugueis = run_query(query_3)

# Função para obter população
def buscar_populacao(pais):
    try:
        resposta = requests.get(f"https://restcountries.com/v3.1/name/{pais}?fullText=true", timeout=5)
        resposta.raise_for_status()
        data = resposta.json()
        return data[0]["population"]
    except Exception as e:
        print(f"Erro ao buscar população de {pais}: {e}")
        return None

# Adicionar coluna de população
df_alugueis["populacao"] = df_alugueis["country"].apply(buscar_populacao)

# Remover países sem dados de população
df_alugueis = df_alugueis.dropna(subset=["populacao"])

# Calcular aluguéis por 1000 habitantes
df_alugueis["alugueis_por_1000"] = (df_alugueis["total_alugueis"] / df_alugueis["populacao"]) * 1000

# Exibir os países mais "cinéfilos"
df_cinefilos = df_alugueis.sort_values(by="alugueis_por_1000", ascending=False)

print("\nTop países mais 'cinéfilos' proporcionalmente:")
print(df_cinefilos[["country", "total_alugueis", "populacao", "alugueis_por_1000"]].head(10))

input("Press enter to continue")
# ⸻

#   Exercício 4 – Filmes Mais Populares em Cidades Poluídas
# 	•	Liste as 10 cidades com maior número de clientes.
# 	•	Use a AirVisual API para consultar o AQI dessas cidades.
# 	•	Relacione os filmes mais alugados em cidades com AQI > 150.
# 	•	Discussão: poluição impacta preferências de filmes?

query_4 = '''
 SELECT
        ci.city,
        a.district,
        cy.country AS country,
        COUNT(DISTINCT c.customer_id) AS num_clientes
    FROM customer c
    JOIN address a ON c.address_id = a.address_id
    JOIN city ci ON a.city_id = ci.city_id
    JOIN country cy ON ci.country_id = cy.country_id
    GROUP BY ci.city, a.district, cy.country
    ORDER BY num_clientes DESC
    LIMIT 20;
'''
df_top_cidades = run_query(query_4)

# # Adicionar AQI ao DataFrame
df_top_cidades["aqi"] = df_top_cidades.apply(lambda row: buscar_qualidade_ar(row["city"],row["district"], row["country"]), axis=1)

print(df_top_cidades)

input("Press enter to continue")

# Observação 1: A lista das cidades com a soma dos clientes não resulta em valores maiores que 2
# Observação 2: A API "AIRVISUAL" utilizada no exercício não tem resposta para a maioria das cidades/estados/país listadas no banco, e reclama de "muitas requisições" em poucas consultas

# ⸻

#   Exercício 5 – Clientes em Áreas Críticas
# 	•	Recupere os clientes com endereço em cidades com AQI acima de 130.
# 	•	Combine nome do cliente, cidade, país, temperatura e AQI.
# 	•	Classifique os clientes em “zona de atenção” com base nos critérios ambientais.


# # Clientes com cidade, país
query_5 = '''
SELECT c.customer_id, c.first_name, c.last_name, ci.city,a.district, co.country
FROM customer c
JOIN address a ON c.address_id = a.address_id
JOIN city ci ON a.city_id = ci.city_id
JOIN country co ON ci.country_id = co.country_id limit 50
'''

df_clientes = run_query(query_5)

# Remove duplicatas de cidades para otimizar chamadas de API
df_cidades_unicas = df_clientes[["city","district","country"]].drop_duplicates().reset_index(drop=True)

# Obter AQI e Temperatura para cada cidade
def obter_info_ambiental(row):
    cidade, estado, pais = row["city"],row["district"], row["country"]
    aqi = buscar_qualidade_ar(cidade, estado, pais)
    temp = buscar_clima(cidade)
    return pd.Series({"aqi": aqi, "temperatura": temp})

df_cidades_unicas[["aqi", "temperatura"]] = df_cidades_unicas.apply(obter_info_ambiental, axis=1)

print(df_cidades_unicas.sort_values(by="aqi", ascending=False).head(10))

df_cidades_unicas = df_cidades_unicas.dropna(subset=["aqi"])

print(df_cidades_unicas.sort_values(by="aqi", ascending=False).head(10))

input("Press enter to continue")

# ⸻

#   Exercício 6 – Receita por Continente
# 	•	Use a REST Countries API para mapear o continente de cada país.
# 	•	Agrupe a receita total por continente.
# 	•	Exiba os resultados em um gráfico de pizza com matplotlib.

# Passo 1 – Receita por país
query_6 = '''
SELECT co.country, SUM(p.amount) AS receita_total
FROM payment p
JOIN customer c ON p.customer_id = c.customer_id
JOIN address a ON c.address_id = a.address_id
JOIN city ci ON a.city_id = ci.city_id
JOIN country co ON ci.country_id = co.country_id
GROUP BY co.country
ORDER BY receita_total DESC
'''
df_receita_pais = run_query(query_6)

# Passo 2 – Obter continente para cada país usando REST Countries
def get_continente(pais):
    try:
        resposta = requests.get(f"https://restcountries.com/v3.1/name/{pais}?fullText=true", timeout=5)
        data = resposta.json()
        return data[0]["region"]  # "Europe", "Asia", "Africa", etc.
    except Exception:
        return None

# Adiciona a coluna de continente
df_receita_pais["continente"] = df_receita_pais["country"].apply(get_continente)

# Remove países que não retornaram continente
df_receita_pais = df_receita_pais.dropna(subset=["continente"])

# Passo 3 – Agrupar receita por continente
df_receita_continente = df_receita_pais.groupby("continente")["receita_total"].sum().sort_values(ascending=False)

# Passo 4 – Gráfico de Pizza
plt.figure(figsize=(8, 8))
colors = sns.color_palette("Set3")
plt.pie(df_receita_continente, labels=df_receita_continente.index, autopct="%1.1f%%", colors=colors, startangle=140)
plt.title("Receita Total por Continente")
plt.axis("equal")
plt.show()

input("Press enter to continue")

# ⸻

#   Exercício 7 – Tempo Médio de Aluguel vs Clima
# 	•	Calcule o tempo médio de aluguel por cidade (entre rental_date e return_date).
# 	•	Combine com a temperatura atual dessas cidades.
# 	•	Visualize a correlação entre temperatura e tempo médio de aluguel (scatterplot + linha de tendência).

#Tempo médio de aluguel por cidade
query_7 = '''
SELECT ci.city, AVG(EXTRACT(EPOCH FROM (r.return_date - r.rental_date))/3600) AS duracao_media_horas
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN store s ON i.store_id = s.store_id
JOIN address a ON s.address_id = a.address_id
JOIN city ci ON a.city_id = ci.city_id
GROUP BY ci.city
ORDER BY duracao_media_horas DESC
'''
df_duracao = run_query(query_7)

# Obter temperatura para cada cidade
df_duracao["temperatura"] = df_duracao["city"].apply(buscar_clima)

# Remove cidades sem dados de temperatura
df_duracao = df_duracao.dropna(subset=["temperatura"])

# Scatterplot com linha de tendência
plt.figure(figsize=(10, 6))
sns.scatterplot(data=df_duracao, x="temperatura", y="duracao_media_horas", color="blue", alpha=0.6)
sns.regplot(data=df_duracao, x="temperatura", y="duracao_media_horas", scatter=False, color="red")

plt.title("Correlação entre Temperatura e Tempo Médio de Aluguel")
plt.xlabel("Temperatura Atual (°C)")
plt.ylabel("Duração Média do Aluguel (horas)")
plt.grid(True)
plt.tight_layout()
plt.show()

input("Press enter to continue")

# ⸻

#   Exercício 8 – Perfil de Clima por Cliente
# 	•	Para cada cliente, crie um perfil com:
# 	•	cidade, temperatura, AQI, total de aluguéis, gasto total.
# 	•	Agrupe os perfis por faixa etária (simulada ou fictícia) e avalie padrões.
# 	•	Objetivo: conectar comportamento de consumo e ambiente.

# base de clientes com cidade, país, total de aluguéis e gasto
query_8 = '''
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS nome,
    ci.city,
	a.district,
    co.country,
    COUNT(r.rental_id) AS total_alugueis,
    SUM(p.amount) AS gasto_total
FROM customer c
JOIN address a ON c.address_id = a.address_id
JOIN city ci ON a.city_id = ci.city_id
JOIN country co ON ci.country_id = co.country_id
LEFT JOIN rental r ON c.customer_id = r.customer_id
LEFT JOIN payment p ON c.customer_id = p.customer_id
GROUP BY c.customer_id, nome, ci.city,a.district, co.country
ORDER BY gasto_total DESC limit 20
'''
df_clientes = run_query(query_8)

# Buscar temperatura
df_clientes["temperatura"] = df_clientes["city"].apply(buscar_clima)

# Buscar AQI (usa cidade e país como chave)
df_clientes["aqi"] = df_clientes.apply(lambda row: buscar_qualidade_ar(row["city"],row["district"], row["country"]), axis=1)

# Remove dados inválidos
#df_clientes = df_clientes.dropna(subset=["temperatura", "aqi"])

# Simular Faixa Etária

np.random.seed(42)  # reprodutibilidade

faixas = ["18-25", "26-35", "36-45", "46-60", "60+"]
df_clientes["faixa_etaria"] = np.random.choice(faixas, size=len(df_clientes))

# Média de gastos e aluguéis por faixa etária
df_agrupado = df_clientes.groupby("faixa_etaria").agg({
    "gasto_total": "mean",
    "total_alugueis": "mean",
    "temperatura": "mean",
    "aqi": "mean"
}).round(2).sort_values(by="gasto_total", ascending=False)

print("\n Perfil médio por faixa etária:\n")
print(df_agrupado)

input("Press enter to continue")


# ⸻

#   Exercício 9 – Exportação Inteligente
# 	•	Gere um relatório Excel com os seguintes critérios:
# 	•	Clientes de países com temperatura < 15°C
# 	•	AQI acima de 100
# 	•	Receita individual > média geral
# 	•	Utilize OpenPyXL e organize em múltiplas abas: Clientes, Temperaturas, Alertas.

# Passo 1 – Calcular média geral de receita
media_receita = df_clientes["gasto_total"].mean()

# Passo 2 – Filtrar conforme os critérios
df_temp_baixa = df_clientes[df_clientes["temperatura"] < 15]
df_alertas = df_clientes[(df_clientes["aqi"] > 100) & (df_clientes["gasto_total"] > media_receita)]

# Passo 3 – Exportar para Excel com múltiplas abas
with pd.ExcelWriter("relatorio_clientes.xlsx", engine="openpyxl") as writer:
    df_clientes.to_excel(writer, sheet_name="Clientes", index=False)
    df_temp_baixa.to_excel(writer, sheet_name="Temperaturas", index=False)
    df_alertas.to_excel(writer, sheet_name="Alertas", index=False)

print("Arquivo 'relatorio_clientes.xlsx' gerado com sucesso!")
# ⸻

#   Exercício 10 – API Cache Inteligente (Desafio)
# 	•	Implemente uma lógica que salve os dados de clima e AQI localmente em CSV.
# 	•	Ao consultar novamente a mesma cidade, busque do CSV ao invés da API.
# 	•	Evite chamadas redundantes — bom para práticas de performance e economia de requisições.