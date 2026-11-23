# ShopBrasil – Pipeline de Dados com Apache Airflow

## 1. Sobre o projeto

Este projeto foi desenvolvido como atividade da pós-graduação e tem como objetivo implementar um pipeline de dados utilizando **Apache Airflow**, **Python** e **PostgreSQL**.

O pipeline realiza a ingestão de produtos disponibilizados pela [FakeStore API](https://fakestoreapi.com/), processa os dados, calcula indicadores por categoria e armazena os resultados em um banco de dados PostgreSQL.

Toda a infraestrutura é executada utilizando Docker Compose.

## 2. Tecnologias utilizadas

- Python 3.12
- Apache Airflow 2.10.5
- Docker e Docker Compose
- PostgreSQL 16
- FakeStore API
- Airflow TaskFlow API
- PostgresHook

## 3. Arquitetura do projeto

O projeto possui dois bancos PostgreSQL:

- **PostgreSQL Metadata:** utilizado internamente pelo Airflow para armazenar informações sobre DAGs, tarefas e execuções.
- **PostgreSQL Analytics:** utilizado para armazenar as métricas calculadas pelo pipeline.

### Estrutura de diretórios

```text
Atividade01/
│
├── config/
├── dags/
│   └── shopbrasil_pipeline.py
│
├── logs/
├── plugins/
├── .env
├── .gitignore
├── docker-compose.yaml
└── README.md
```

## 4. Funcionamento do pipeline

A DAG `shopbrasil_pipeline` executa as seguintes etapas:

1. **Ingestão:** consulta a FakeStore API e recupera os produtos.
2. **Extração de categorias:** identifica as categorias existentes nos produtos.
3. **Processamento paralelo:** calcula quantidade de produtos, preço médio, preço mínimo e preço máximo para cada categoria.
4. **Consolidação:** reúne os resultados das categorias.
5. **Persistência:** grava as métricas no PostgreSQL Analytics.

### Recursos implementados no Airflow

- TaskFlow API com `@dag` e `@task`.
- Organização das etapas utilizando TaskGroups.
- Dependências entre tarefas.
- XCom para compartilhamento de resultados.
- Processamento paralelo com Dynamic Task Mapping (`expand`).
- Fluxo Fan-out e Fan-in.
- Pool `ecommerce_pool`, configurado com 2 slots.
- Retries com backoff exponencial.
- Callbacks de sucesso, falha e nova tentativa.
- Tratamento de exceções na ingestão.
- Persistência utilizando `PostgresHook`.
- UPSERT para garantir idempotência na gravação dos resultados.

## 5. Agendamento

A DAG está configurada para execução diária às **06:00**, considerando o fuso horário `America/Sao_Paulo`.

Configurações:

- DAG ID: `shopbrasil_pipeline`
- Schedule: `0 6 * * *`
- Timezone: `America/Sao_Paulo`
- Catchup: `False`

## 6. Como executar o projeto

### Pré-requisitos

- Docker Desktop instalado e em execução.
- Docker Compose disponível.
- Git instalado (para clonar o repositório).

### 6.1. Clonar o repositório

```bash
git clone URL_DO_REPOSITORIO
cd Atividade01
```

### 6.2. Configurar as variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto utilizando o `.env.example` como referência.

Preencha as senhas e chaves necessárias antes de iniciar os containers.

**Importante:** o arquivo `.env` não deve ser enviado ao GitHub.

### 6.3. Inicializar o ambiente

Execute:

```bash
docker compose up airflow-init
```

Após a inicialização, execute:

```bash
docker compose up -d
```

Verifique os containers:

```bash
docker compose ps
```

### 6.4. Acessar o Airflow

Abra o navegador no endereço:

http://localhost:8080

Utilize as credenciais administrativas definidas no arquivo `.env`.

### 6.5. Configurar a conexão PostgreSQL

Na interface do Airflow, acesse **Admin > Connections** e configure a conexão:

| Campo | Valor |
|---|---|
| Connection Id | `postgres_shopbrasil` |
| Connection Type | PostgreSQL |
| Host | `postgres-analytics` |
| Schema | Nome do banco analítico configurado |
| Login | Usuário do banco analítico |
| Password | Senha do banco analítico |
| Port | `5432` |

Os valores devem corresponder às variáveis configuradas no `.env`.

### 6.6. Configurar o Pool

Acesse **Admin > Pools** e crie:

| Campo | Valor |
|---|---|
| Pool Name | `ecommerce_pool` |
| Slots | `2` |

### 6.7. Executar a DAG

Na página principal do Airflow, localize a DAG `shopbrasil_pipeline` e habilite sua execução.

A execução também pode ser iniciada manualmente pela interface.

## 7. Banco de dados analítico

Os resultados são armazenados na tabela `metricas_categoria`.

A tabela contém:

- `data_referencia`
- `categoria`
- `quantidade_produtos`
- `preco_medio`
- `preco_minimo`
- `preco_maximo`
- `atualizado_em`

A chave primária composta por `data_referencia` e `categoria`, combinada com o comando `ON CONFLICT DO UPDATE`, evita a duplicação de registros quando o pipeline é executado novamente para a mesma data.

## 8. Encerramento do ambiente

Para parar os containers:

```bash
docker compose down
```

Os dados persistidos nos volumes são mantidos.

Para iniciar novamente:

```bash
docker compose up -d
```

**Atenção:** não utilize `docker compose down -v` caso queira preservar os dados dos bancos PostgreSQL.

## 9. Autor

Projeto acadêmico desenvolvido como atividade da pós-graduação, com foco em orquestração de workflows e engenharia de dados utilizando Apache Airflow.