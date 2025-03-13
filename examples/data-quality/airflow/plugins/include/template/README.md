# Utilização de Templates com Arquivos YAML

Este guia descreve como utilizar os templates de DAGs dinâmicas no Airflow a partir de arquivos YAML. A estrutura do arquivo YAML permite configurar os parâmetros do DAG, tarefas, e processos, facilitando a definição de fluxos de trabalho de maneira flexível e reutilizável.

## Templates de DAGs para Reutilização

Foram criados dois templates para facilitar a manutenção e padronizar a criação das DAGs:

- **`elt_data_pipeline`**: Template para pipelines de Extração, Carga e Transformação (ELT).
- **`external_data_pipeline`**: Template para pipelines que integram dags externas.

### Objetivo

Esses templates têm o intuito de padronizar a estrutura das DAGs e simplificar sua manutenção, garantindo consistência e eficiência na criação de novos pipelines.

### Padrão de Nomenclatura dos Arquivos

Para garantir a conformidade com o padrão definido no README principal do repositório Composer, siga estas diretrizes para a nomenclatura dos arquivos:

1. **Prefixo dos Arquivos**: 
   - Os arquivos de configuração das DAGs devem começar com o prefixo **`conf_`** em vez de **`dag_`**.
   - Por exemplo, se o padrão de nomenclatura é `dag_example_pipeline.py`, o arquivo de configuração correspondente deve ser nomeado como `conf_example_pipeline.py`.

2. **Substituição do Prefixo**:
   - Ao gerar as DAGs a partir dos arquivos de configuração, o prefixo **`conf_`** será substituído por **`dag_`**.
   - O nome final do arquivo de configuração será utilizado para criar o identificador da DAG (Dag ID).

### Exemplo

- **Arquivo de Configuração**: `conf_example_pipeline.py`
  - Este arquivo segue o padrão de nomenclatura para configurações e será utilizado para gerar a DAG com o identificador **`dag_example_pipeline`**.

## Documentação dos Parâmetros

### Template ``elt_data_pipeline``

#### Estrutura Geral do YAML

O arquivo YAML deve conter as seguintes seções:

```yaml
dag:
  owner: "string"
  max_active_runs: int
  schedule: "cron expression"
  event_dataset_schedule: List with the names of dataset events formed by "dataset_id.table_id" to trigger the dag.
  description: "string"
  retries: int
  retry_delay: int (minutes)
  timeout: int (minutes)
  tags:
    - "tag1"
    - "tag2"
  doc_md: "markdown content" (optional)

common:
  form_dag_compilation: "dag_id"
  form_repository_id: "repository_id"
  secret_db_url_conn: "secret_manager_id"
  secret_db_user: "secret_manager_id"
  secret_db_pwd: "secret_manager_id"

tasks:
  - process: "process_name"
    source: "source_name"
    deletion:
      deletion_partition_bronze: bool
      deletion_query: "SQL query string"
      deletion_file_query: "path_to_sql_file"
      use_query_params_dataflow: bool
      query_params:
        key1: value1
        key2: value2
    ingestion:
      service: "dataflow" or "cloudfunctions"
      job_name: "job_name"
      template_name: "SQLServer_to_BigQuery" or "Oracle_to_BigQuery"
      destination_dataset: "Destination Dataset"
      connection_properties: "property_name=property_value;..."
      use_column_alias: bool (optional, default: False)
      is_truncate: bool (optional, default: False)
      query_full: "SQL query for full extraction"
      query_incremental: "SQL query for incremental extraction"
      query_file_full: "path_to_sql_file"
      query_file_incremental: "path_to_sql_file"
      query_params:
        key1: value1
        date_init_rule: "rule"
        date_end_rule: "rule"
      fetch_size: int (optional, default: 50000)
      num_workers: int (optional, default: 2)
      max_workers: int (optional)
      function_name: "Function name"
      request_method: POST
      data_request: 
         key1: value1
      create_event_dataset: True or False
    transformation:
      service: "dataform"
      silver_is_incremental: bool
      exec_dependencies: bool
      exec_dependents: bool
    trigger_dag:
      - "dag_id1"
      - "dag_id2"
    dependencies:
      - "process_name1"
      - "process_name2"
```

#### Configuração dos Parâmetros

Aqui está a tradução das descrições para o português:

##### Configuração da DAG (dag(dict))

- **owner** (str, Obrigatório): O proprietário da DAG.
- **max_active_runs** (int, Obrigatório): O número máximo de execuções ativas.
- **schedule** (str, Opcional): A expressão de cronograma em formato cron para a DAG, se a dag for trigada externamente, basta não passar o scheduler.
Claro! Aqui está a tradução:
- **event_dataset_schedule** (list, Opcional): Lista com os nomes dos eventos de dataset formados por "dataset_id.table_id" para acionar a DAG. Pode ser usado como um substituto para scheduler.
- **description** (str, Obrigatório): A descrição da DAG.
- **retries** (int, Obrigatório): O número de tentativas para tarefas falhadas.
- **retry_delay** (int,Obrigatório): O intervalo em minutos entre tentativas.
- **timeout** (int, Obrigatório): O tempo limite em minutos para a execução da DAG.
- **tags** (list, Opcional): Lista de tags associadas à DAG.
- **doc_md** (str, Opcional): Documentação em formato markdown para a DAG.

#### Parâmetros Comuns (common)

- **form_dag_compilation** (str, Opcional): ID da DAG do resultado da compilação para Dataform. Só será usado se tiver o parâmetros de transformação no final do yaml.
- **form_repository_id** (str, Opcional): ID do repositório para Dataform. Só será usado se tiver o parâmetros de transformação no final do yaml.
- **secret_db_url_conn** (str, Opcional): ID do Secret Manager para a URL de conexão com o banco de dados. Só será usado se o serviço de ingestão for o Dataflow.
- **secret_db_user** (str, Opcional): ID do Secret Manager para o usuário do banco de dados. Só será usado se o serviço de ingestão for o Dataflow.
- **secret_db_pwd** (str, Opcional): ID do Secret Manager para a senha do banco de dados. Só será usado se o serviço de ingestão for o Dataflow.

#### Tarefas (tasks(list))

- **process** (str, Obrigatório): Nome que serve de base para o nome tabela bronze e silver, na silver é aplicado um prefixo "tbl".
- **source** (str, , Obrigatório): Origem dos dados composto pela 'aplicação' + 'sistema de origem'. Utilizaso para criar a nomenclatura dos datasets de origem e destino.
- **deletion** (dict, Opcional): Configuração de deleção para a tabela Bronze no BigQuery.
  - **deletion_partition_bronze** (bool, Obrigatório): Se deve deletar dados na tabela Bronze.
  - **deletion_query** (str, Opcional): String SQL com ou sem parâmetros que será usada para deletar dados, os parametros ``dataset_id`` e ``table_id`` estão definidos internamente.
  - **deletion_file_query** (str, Opcional): Caminho relativo do arquivo SQL com ou sem parâmetros.
  - **use_query_params_dataflow** (bool, Opcional): Indica se os mesmos parâmetros da consulta de ingestão do Dataflow serão usados para deleção, (EX: Quando definido ``date_init_role`` nos parâmetros do dataflow, ele será exposto como ``date_init`` nos parâmetros de deleção).
  - **query_params** (dict, Opcional): Parâmetros usados na consulta ou no arquivo de consulta. Será usado apenas se **use_query_params_dataflow** for igual a False.
    - **key**: valor
    - **date_init_rule** (str, Opcional): Regra para a data de início da extração.
    - **date_end_rule** (str, Opcional): Regra para a data de término da extração.
      O formato da **date_init_rule** ou **date_end_rule** deve ser:
      - Prefixo com 'd' (dia), 'm' (mês) ou 'y' (ano).
      - Seguido por um operador '-' (subtrair) ou '+' (adicionar).
      - Sufixo com um número indicando a quantidade de dias, meses ou anos para adicionar/subtrair.
      Exemplos:
      - 'd-5': 5 dias atrás
      - 'm+1': 1 mês a partir de agora
      - 'y-2': 2 anos atrás
- **ingestion** (dict): Dicionário com parâmetros para a camada de ingestão.
  - **service** (str, Obrigatório): Serviço de ingestão. Deve ser 'dataflow' ou 'cloudfunctions'.
    Configure os parâmetros abaixo quando o serviço for **dataflow**:
    - **job_name** (str, Opcional): Nome do job.
    - **template_name** (str, Obrigatório): Nome do template aceito, pode ser 'SQLServer_to_BigQuery' ou 'Oracle_to_BigQuery'.
    - **destination_dataset** (str, Opcional): Dataset de destino da ingestão, se não for definido o padrão será, ``bronze_{source}``.
    - **connection_properties** (str, Opcional): String de propriedades a ser usada para a conexão JDBC. O formato da string deve ser `[propertyName=property;]*`. (Exemplo: unicode=true;characterEncoding=UTF-8)
    - **use_column_alias** (bool, Opcional): Utilizar alias para colunas. O padrão é False.
    - **is_truncate** (bool, Opcional): Truncar a tabela, definido se a ingestão é completa ou incremental. O padrão é False.
    - **query_full** (str, Opcional): String SQL para extração completa. Se não definido, o parâmetro "query_file_full" será usado com o valor padrão.
    - **query_incremental** (str, Opcional): String SQL para extração incremental. Se não definido, o parâmetro "query_file_incremental" será usado com o valor padrão.
    - **query_file_full** (str, Opcional): Caminho do arquivo SQL .sql para ingestão completa. O padrão é "bronze/default_full_ingest.sql". Para o arquivo SQL padrão, os parâmetros "source_schema" e "source_table" devem ser definidos em "query_params".
    - **query_file_incremental** (str, Opcional): Caminho do arquivo SQL .sql para ingestão incremental. O padrão é "bronze/default_incremental_ingest.sql". Para o arquivo SQL padrão, os parâmetros "source_schema", "source_table", "column_ref_delta", "date_init_rule" e "date_end_rule" devem ser definidos em "query_params".
    - **query_params** (dict, Opcional): Dicionário com parâmetros a serem substituídos em "query" ou "query_file".
      - **key**: valor
      - **date_init_rule** (str, Opcional): Regra para a data de início da extração.
      - **date_end_rule** (str, Opcional): Regra para a data de término da extração.
        O formato da **date_init_rule** ou **date_end_rule** deve ser:
        - Prefixo com 'd' (dia), 'm' (mês) ou 'y' (ano).
        - Seguido por um operador '-' (subtrair) ou '+' (adicionar).
        - Sufixo com um número indicando a quantidade de dias, meses ou anos para adicionar/subtrair.
        Exemplos:
        - 'd-5': 5 dias atrás
        - 'm+1': 1 mês a partir de agora
        - 'y-2': 2 anos atrás
    - **fetch_size** (int, Opcional): Número de linhas a serem recuperadas do banco de dados de cada vez. Não usado para leituras particionadas. O padrão é: 50000.
    - **num_workers** (int, Opcional): Número de workers. O padrão é 2.
    - **max_workers** (int, Opcional): Número máximo de workers.
    - **create_event_dataset** (bool, Opcional): Ativa a criação de um evento de dataset para a task de ingestão, que pode ser utilizado como um gatilho para acionar outra DAG.
    Configure os parâmetros abaixo quando o serviço for **cloudfunctions**:
    - **function_name** (str): Nome da Cloud Function.
    - **request_method** (str): Método HTTP a ser utilizado (ex.: POST).
    - **data_request** (dict, Opcional): Dados a serem enviados na requisição.
    - **http_conn_id** (str, Opcional): ID da conexão HTTP para a requisição.
    - **trigger_rule** (str, Opcional): Regra de acionamento para a tarefa (padrão: 'all_success').
    - **create_event_dataset** (bool, Opcional): Ativa a criação de um evento de dataset para a task de ingestão, que pode ser utilizado como um gatilho para acionar outra DAG.
- **transformation** (dict, Opcional): Configuração para tarefas de transformação.
  - **service** (str): Serviço de transformação. Deve ser 'dataform'.
  - **silver_is_incremental** (bool): Se o carregamento é incremental. Se incremental será criado uma task branch para realiza a primeira carga full na primeira execução.
  - **exec_dependencies** (bool): Define se as dependências do processo serão executadas no Dataform.
  - **exec_dependents** (bool): Define se os dependentes do processo serão executados no Dataform.

- **trigger_dag** (list, Opcional): Lista de IDs de DAGs a serem acionados após a conclusão da tarefa.
- **dependencies** (list, Opcional): Lista de nomes de processos dos quais a tarefa atual depende.
#### Exemplo de Arquivo YAML

```yaml
dag:
  owner: "data_team"
  max_active_runs: 1
  schedule: "0 6 * * *"
  description: "DAG de ingestão e transformação de dados"
  retries: 3
  retry_delay: 10
  timeout: 120
  tags:
    - "ingestion"
    - "transformation"

common: # Parâmetros comuns entre os processos.
  form_repository_id: dataform-repo-default
  form_dag_compilation: compilation_dataform_repository_default 
  secret_db_url_conn: sm_url_connection
  secret_db_user: sm_user_connection
  secret_db_pwd: sm_password_connection

tasks:
  - process: "process_2"
    source: "ecommerce"
    deletion:
        deletion_partition_bronze: True
        deletion_query: >
            DELETE {dataset_id}.{table_id}
            WHERE {column_ref_delta} >= '{date_init}' 
            AND {column_ref_delta} <= '{date_end}'
        use_query_params_dataflow: True
    ingestion:
      service: "dataflow"
      job_name: "orders_ingestion"
      template_name: "SQLServer_to_BigQuery"
      is_truncate: False
      query_full: SELECT * FROM {source_schema}.{source_table}
      query_file_incremental: > 
        SELECT * FROM {source_schema}.{source_table} WHERE {column_ref_delta} >= '{date_init}' AND {column_ref_delta} <= '{date_end}'
      query_params:
        source_schema: "dbo"
        source_table: "orders"
        column_ref_delta: data_atualizacao
        date_init_rule: "d-5"
        date_end_rule: "d+0"
      fetch_size: 50000
      num_workers: 4
      max_workers: 8
    trigger_dag:
      - "external_dag"
    dependencies:
      - "process1"
```

### Conclusão

Com este template YAML, você pode criar DAGs dinâmicas no Airflow configurando facilmente parâmetros de ingestão, transformação, exclusão e dependências, adaptando o fluxo às suas necessidades específicas de pipeline de dados.