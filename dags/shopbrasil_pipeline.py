
import logging
from datetime import timedelta

import pendulum
import requests

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context


# ==========================================================
# CALLBACKS
# ==========================================================

def callback_sucesso(context):
    task_instance = context["ti"]
    logging.info(
        "SUCESSO: Task %s executada corretamente.",
        task_instance.task_id
    )


def callback_falha(context):
    task_instance = context["ti"]
    logging.error(
        "FALHA: Task %s apresentou um erro.",
        task_instance.task_id
    )


def callback_retry(context):
    task_instance = context["ti"]
    logging.warning(
        "RETRY: Nova tentativa para a task %s.",
        task_instance.task_id
    )


# ==========================================================
# DAG
# ==========================================================

@dag(
    dag_id="shopbrasil_pipeline",
    description="Pipeline analítico de preços da ShopBrasil(Atividade01)",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(
        2026, 11, 18, tz="America/Sao_Paulo"
    ),
    catchup=False,
    tags=["shopbrasil", "atividade-pos-graduacao"]
)
def shopbrasil_pipeline():

    # ======================================================
    # TASKGROUP 1 - INGESTÃO
    # ======================================================

    @task_group(group_id="ingestao")
    def grupo_ingestao():

        @task(
            retries=3,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(minutes=5),
            on_failure_callback=callback_falha,
            on_retry_callback=callback_retry,
            on_success_callback=callback_sucesso
        )
        def buscar_produtos():

            url = "https://fakestoreapi.com/products"

            try:
                logging.info("Consultando FakeStore API.")

                response = requests.get(url, timeout=30)
                response.raise_for_status()

                produtos = response.json()

                if not isinstance(produtos, list) or not produtos:
                    raise ValueError("Resposta inválida ou vazia.")

                produtos_tratados = []

                for produto in produtos:

                    campos = ["id", "title", "price", "category"]

                    if not all(campo in produto for campo in campos):
                        raise ValueError(
                            "Produto com campos obrigatórios ausentes."
                        )

                    preco = float(produto["price"])

                    if preco < 0:
                        raise ValueError("Produto com preço negativo.")

                    produtos_tratados.append({
                        "id": produto["id"],
                        "title": produto["title"],
                        "price": preco,
                        "category": produto["category"]
                    })

                logging.info(
                    "Total de produtos encontrados: %s",
                    len(produtos_tratados)
                )

                return produtos_tratados

            except (requests.RequestException, ValueError) as erro:
                logging.error("Erro na ingestão: %s", erro)
                raise

        @task
        def extrair_categorias(produtos):

            categorias = sorted({
                produto["category"]
                for produto in produtos
            })

            logging.info(
                "Categorias identificadas: %s",
                categorias
            )

            return categorias

        produtos = buscar_produtos()
        categorias = extrair_categorias(produtos)

        return produtos, categorias

    # ======================================================
    # TASKGROUP 2 - ANÁLISE
    # ======================================================

    @task_group(group_id="analise")
    def grupo_analise(produtos, categorias):

        # --------------------------------------------------
        # CÁLCULO DAS MÉTRICAS
        # --------------------------------------------------

        @task(pool="ecommerce_pool")
        def calcular_metricas(categoria, produtos):

            produtos_categoria = [
                produto
                for produto in produtos
                if produto["category"] == categoria
            ]

            precos = [
                produto["price"]
                for produto in produtos_categoria
            ]

            resultado = {
                "categoria": categoria,
                "quantidade_produtos": len(precos),
                "preco_medio": round(sum(precos) / len(precos), 2),
                "preco_minimo": min(precos),
                "preco_maximo": max(precos)
            }

            logging.info(
                "Métricas calculadas: %s",
                resultado
            )

            return resultado

        # --------------------------------------------------
        # CONSOLIDAÇÃO (FAN-IN)
        # --------------------------------------------------

        @task
        def consolidar_resultados(resultados):

            logging.info("Consolidando métricas das categorias.")

            total_produtos = sum(
                item["quantidade_produtos"]
                for item in resultados
            )

            logging.info(
                "Categorias processadas: %s",
                len(resultados)
            )

            logging.info(
                "Total de produtos: %s",
                total_produtos
            )

            for item in resultados:
                logging.info("Resultado: %s", item)

            return resultados

        # --------------------------------------------------
        # CADASTRO NO POSTGRESQL
        # --------------------------------------------------

        @task
        def salvar_no_postgres(resultados):

            contexto = get_current_context()

            data_referencia = (
                contexto["logical_date"].date().isoformat()
            )

            hook = PostgresHook(
                postgres_conn_id="postgres_shopbrasil"
            )

            sql = """
                INSERT INTO metricas_categoria (
                    data_referencia,
                    categoria,
                    quantidade_produtos,
                    preco_medio,
                    preco_minimo,
                    preco_maximo,
                    atualizado_em
                )
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)

                ON CONFLICT (data_referencia, categoria)
                DO UPDATE SET
                    quantidade_produtos = EXCLUDED.quantidade_produtos,
                    preco_medio = EXCLUDED.preco_medio,
                    preco_minimo = EXCLUDED.preco_minimo,
                    preco_maximo = EXCLUDED.preco_maximo,
                    atualizado_em = CURRENT_TIMESTAMP;
            """

            for item in resultados:

                parametros = (
                    data_referencia,
                    item["categoria"],
                    item["quantidade_produtos"],
                    item["preco_medio"],
                    item["preco_minimo"],
                    item["preco_maximo"]
                )

                hook.run(
                    sql,
                    parameters=parametros
                )

                logging.info(
                    "Cadastro concluído no PostgreSQL: %s",
                    item["categoria"]
                )

            logging.info(
                "Total de categorias cadastradas: %s",
                len(resultados)
            )

        # --------------------------------------------------
        # DEPENDÊNCIAS: FAN-OUT -> FAN-IN -> POSTGRES
        # --------------------------------------------------

        metricas = calcular_metricas.partial(
            produtos=produtos
        ).expand(
            categoria=categorias
        )

        resultados = consolidar_resultados(metricas)

        salvar_no_postgres(resultados)

    # ======================================================
    # DEPENDÊNCIAS DA DAG
    # ======================================================

    produtos, categorias = grupo_ingestao()

    grupo_analise(produtos, categorias)


shopbrasil_pipeline()
