from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.timezone import pendulum
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.models.taskinstance import TaskInstanceKey
from airflow.operators.empty import EmptyOperator
from google.cloud.exceptions import NotFound
from dateutil.relativedelta import relativedelta
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token
from datetime import timedelta, datetime
from airflow.models.dagrun import DagRun
from airflow.models.xcom import XCom
from google.cloud import bigquery
import logging
import json
import pytz
import os
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.datasets import Dataset

# Defina o timezone para o Brasil (São Paulo)
brazil_timezone = pytz.timezone('America/Sao_Paulo')

ENV = os.getenv("ENV")
PROJECT_ID = os.getenv("PROJECT")
LOCATION = os.getenv("LOCATION")
URL_SUBNET_APPS = os.getenv("URL_SUBNET_APPS")
URL_SUBNET_DEFAULT = os.getenv("URL_SUBNET_DEFAULT")
SA_DATAFLOW = os.getenv("SA_DATAFLOW")
GCS_BUCKET_DATAFLOW = os.getenv("BUCKET_DATAFLOW")
PATH_DF_TEMPLATE = os.getenv("PATH_DF_TEMPLATE")
SQL_FILE_PATH = os.getenv("SQL_FILE_PATH")
KMS_KEY_RING = os.getenv("KMS_KEY_RING")
KMS_CRYPTO_KEY_DB = os.getenv("KMS_CRYPTO_KEY_DB")
URL_BASE_GCF = os.getenv("URL_BASE_GCF")

class TaskBaseFactoryConfig:
    @staticmethod
    def calculate_date_by_rule(date_rule: str = None) -> datetime.date:
        """
        Calculate date based on a given rule.

        param: date_rule (str, optional): A string representing the rule for date calculation.
                The format of the rule should be:
                - Prefix with 'd' (day), 'm' (month), or 'y' (year).
                - Followed by an operator '-' (subtract) or '+' (add).
                - Suffix with a number indicating the amount of days, months, or years to add/subtract.
                Examples:
                    - 'd-5' : 5 days ago
                    - 'm+1' : 1 month from now
                    - 'y-2' : 2 years ago

        Returns:
            datetime: The calculated date based on the provided rule in format - '%Y-%m-%d'
        """
        if not date_rule:
            return None
        # Separar o prefixo (d, m, y) e o sufix (+/- quantidade)
        prefix = date_rule[0].lower()
        operator = date_rule[1]
        sufix = date_rule[2:]
        
        # Converter a quantidade para um número inteiro
        try:
            quantity = int(sufix)
        except ValueError:
            return None  # Caso não seja um número válido

        today = datetime.now(brazil_timezone)
        operations = {
            'd': lambda x: today + relativedelta(days=x) if operator == '+' else today - relativedelta(days=x),
            'm': lambda x: today + relativedelta(months=x) if operator == '+' else today - relativedelta(months=x),
            'y': lambda x: today + relativedelta(years=x) if operator == '+' else today - relativedelta(years=x)
        }

        if prefix in operations:
            if prefix == 'm':
                return operations[prefix](quantity).strftime('%Y-%m-01')
            elif prefix == 'y':
                return operations[prefix](quantity).strftime('%Y-01-01')
            else:
                return operations[prefix](quantity).strftime('%Y-%m-%d')
        return None

    @staticmethod
    def load_sql_file_and_replace_params(query_relative_path: str, query_params: dict = {}) -> str:
        """ Load SQL File And Substituted Parameters
        param: query_relative_path (str): File path relative sql. Ex: bronze/file.sql
        param: query_params: (dict, optional): Dict with parameters to replace in "query" or "query_file"
        
        return: Sql with parameters substituted
        """
        # Ler o arquivo SQL
        with open(f"{SQL_FILE_PATH}{query_relative_path}", 'r') as file:
            sql = file.read()
        
        # Remove as quebras de linha
        sql = " ".join(sql.splitlines())

        # Substituir placeholders usando .format
        substituted_sql = sql.format(**query_params)
        return substituted_sql
    
    def get_flow_query_params(self, flow_param: dict):
        """Get dataflow query parameters and create key date_init and date_end.
        
        param: flow_param (dict): Dataflow Parameters.

        return: 
            dict: Query parameters with date_init and date_end replaced by calculated dates.
        """
        query_params = {}
        
        if flow_param.get('query_params'):
            query_params = flow_param['query_params']
            # Substitui a regra de inicio e fim pelo valor em data
            query_params['date_init'] = self.calculate_date_by_rule(query_params.get('date_init_rule'))
            query_params['date_end'] = self.calculate_date_by_rule(query_params.get('date_end_rule'))
        return query_params
    
    #get latest execution dag run which list the steps in xcom
    @staticmethod
    def get_latest_compilation_xcom(dag_id, task_id, key_xcom):

        dag_runs = DagRun.find(dag_id=dag_id)
        dag_sucess = list(filter(lambda x: x.state == 'success', dag_runs))
        dag_sucess.sort(key=lambda x: x.execution_date, reverse=True)
        logging.info(f"Last dag run: {dag_sucess[0].execution_date}")
        dag_run = None
        if dag_sucess:
            dag_run = dag_sucess[0]

        ti_key = TaskInstanceKey(dag_id=dag_id, task_id=task_id, run_id=f"{dag_run.run_id}")
        compilation = XCom.get_value(ti_key=ti_key, key=key_xcom)
        return compilation if compilation else {}

class TaskFactory(TaskBaseFactoryConfig):
    def __init__(self) -> None:
        super().__init__()
        
    @staticmethod
    def create_cloudfunction_task(task_id: str, gcf_params: dict):
        endpoint_gcf = gcf_params['function_name']
        url_gcf = f"{URL_BASE_GCF}/{endpoint_gcf}"
        token_gcf = fetch_id_token(Request(), url_gcf)
        
        return HttpOperator(
            task_id=task_id,
            method=gcf_params.get("request_method", "POST"),
            http_conn_id=gcf_params.get("http_conn_id", "gcf_conn_id"),
            endpoint=endpoint_gcf,
            headers={'Authorization': f"Bearer {token_gcf}", "Content-Type": "application/json"},
            data=json.dumps(gcf_params['data_request']) if gcf_params.get('data_request') else None,
            trigger_rule=gcf_params.get('trigger_rule', 'all_success')
        )
        
    def create_del_partition_task(self, task_id: str, del_param: dict, dataset_id: str,  table_id: str, flow_param: dict = None):
        """
        Creates a BigQuery task to delete partitions in the bronze table.

        param: task_id (str): Id of task
        param: partition_type (str): The type of partition (e.g., DAY, MONTH, YEAR).
        param: del_param (dict): BigQuery parameters.
        param: dataset_id (str): Dataset ID.
        param: table_id (str): Table ID.
        param: flow_param (dict, optional): Dataflow Parameters.

        return: BigQueryInsertJobOperator: The configured BigQuery task operator.
        """
        
        # Define se será usado os parâmetros do Dataflow quando forem comuns
        if del_param.get('use_query_params_dataflow'):
            query_params = self.get_flow_query_params(flow_param)
        else:
            query_params = del_param.get('query_params', {})
        
        # Adiciona o dataset e a tabela nos parametros da query 
        query_params['dataset_id'] = dataset_id
        query_params['table_id'] = table_id
        
        query = ""
        # Define se usar um String SQL ou um Arquivo SQL
        if del_param.get('deletion_query'):
            query = del_param['deletion_query'].format(**query_params)
            
        elif del_param.get('deletion_file_query'):
            query_path = del_param['deletion_file_query']
            query = self.load_sql_file_and_replace_params(query_path, query_params)
        
        return BigQueryInsertJobOperator(
            task_id=task_id,
            configuration={
                'query': {
                    'query': query,
                    'useLegacySql': False,
                }
            },
        )
        
    def create_dataflow_jdbc_to_bq_task(self, source, process, task_id, flow_param, common_params, dest_dataset, dest_table, task_type='full_load', event_dataset = None):
        """
        Create a Dataflow Flex Template operator for JDBC to BigQuery.
        
        param: task_id (str): ID of the Task.
        param: flow_param (dict): Task parameters.
        param: common_params (dict): Common parameters.
        param: dest_dataset (str): Destination dataset.
        param: dest_table (str): Destination table.
        param: task_type (str, optional): Type of task (full_load or incremental_load). Default is 'full_load'.
        
        return: DataflowStartFlexTemplateOperator: Configured DataflowStartFlexTemplateOperator.
        """
        
        job_name = f"flow-{source}-to-brz-{process}".replace("_", "-") if not flow_param.get('job_name') else flow_param['job_name']
        
        # Subtitui o prefixo "sm_" das credencias armazenadas no Secret Manager, para o Airflow acessar via backend.
        secret_db_url_conn = common_params['secret_db_url_conn'].replace("sm_", "")
        secret_db_user = common_params['secret_db_user'].replace("sm_", "")
        secret_db_pwd = common_params['secret_db_pwd'].replace("sm_", "")
        
        query_params = self.get_flow_query_params(flow_param)
        
        # Determinar a consulta com base no tipo de tarefa e nos parâmetros fornecidos
        if task_type == 'full_load':
            is_truncate = "true"
            if flow_param.get('query_full'):
                query = flow_param['query_full'].format(**query_params)
            else:
                query_path = flow_param.get('query_file_full', "bronze/default_full_ingest.sql")
                query = self.load_sql_file_and_replace_params(query_path, query_params)
        else:
            is_truncate = "false"
            if flow_param.get('query_incremental'):
                query = flow_param['query_incremental'].format(**query_params)
            else:
                query_path = flow_param.get('query_file_incremental', "bronze/default_incremental_ingest.sql")
                query = self.load_sql_file_and_replace_params(query_path, query_params)
        
        body={
            "launchParameter": {
                "jobName": job_name,
                "parameters": {
                    "connectionURL": "{{var.value." + f"{secret_db_url_conn}" + "}}",
                    "username": "{{var.value." + f"{secret_db_user}" + "}}",
                    "password": "{{var.value." + f"{secret_db_pwd}" + "}}",
                    "query": query,
                    "bigQueryLoadingTemporaryDirectory": f"{GCS_BUCKET_DATAFLOW}/temp/",
                    "useColumnAlias": str(flow_param.get('use_column_alias', 'false')).lower(),
                    "isTruncate": is_truncate,
                    "outputTable": f"{PROJECT_ID}:{dest_dataset}.{dest_table}",
                    "fetchSize": str(flow_param.get('fetch_size', '50000')),
                    "KMSEncryptionKey": f"projects/{PROJECT_ID}/locations/{LOCATION}/keyRings/{KMS_KEY_RING}/cryptoKeys/{KMS_CRYPTO_KEY_DB}"
                },
                "environment": {
                    "numWorkers": str(flow_param.get('num_workers', '2')),
                    "maxWorkers": str(flow_param.get('max_workers', '4')),
                    "ipConfiguration": "WORKER_IP_PRIVATE",
                    "serviceAccountEmail": SA_DATAFLOW,
                    "stagingLocation": f"{GCS_BUCKET_DATAFLOW}/staging/",
                    "tempLocation": f"{GCS_BUCKET_DATAFLOW}/temp/",
                    "subnetwork": URL_SUBNET_DEFAULT
                },
                "containerSpecGcsPath": f"{PATH_DF_TEMPLATE}/{flow_param['template_name']}",
            }
        }

        # Adiciona connectionProperties se connection_properties não for None
        if flow_param.get('connection_properties'):
            body['launchParameter']['parameters']['connectionProperties'] = flow_param['connection_properties']

        return DataflowStartFlexTemplateOperator(
            task_id=task_id,
            project_id=PROJECT_ID,
            body=body,
            location=LOCATION,
            deferrable=True,
            append_job_name=False,
            trigger_rule="all_done", # Executa essa task mesmo que a tarefa anterior, por padrão a "Compilation Dataform", termine com falha.
            outlets=[event_dataset] # Atualiza o dataset para essa task
        )

    @staticmethod
    def create_workflow_dataform_task(task_id, form_param, dataset_id,  table_id, repository_id, last_compilation, event_dataset, task_type='full_load'):
        
        fist_full_refresh = True if task_type == "full_load" else False
        
        return DataformCreateWorkflowInvocationOperator(
                task_id=task_id,
                project_id=PROJECT_ID,
                region=LOCATION,
                repository_id=repository_id,
                workflow_invocation={
                    # "compilation_result": "{{ ti.xcom_pull(dag_id='"+dag_compilation+"', task_ids='compilation_result', key='return_value', include_prior_dates=True)['name'] }}",
                    "compilation_result": last_compilation.get('name'),
                    "invocation_config": {
                        # "included_tags": tags,
                        "included_targets": [{"database": PROJECT_ID, "name": table_id, "schema": dataset_id}],
                        "transitive_dependencies_included": form_param.get('exec_dependencies', False),
                        "transitive_dependents_included": form_param.get('exec_dependents', False),
                        "fully_refresh_incremental_tables_enabled": fist_full_refresh
                    },
                },
                outlets=[event_dataset] # Atualiza o dataset para essa task
            )
        
    @staticmethod
    def check_empty_or_table_exists(task_name, dataset_id, table_name, task_id_true, task_id_false, group_id=None):
        """
        Checks if a BigQuery table exists and returns a branching task based on the result.

        param: process (str): The name of the process.
        param: dataset_id (str): The name of the dataset.
        param: table_name (str): The name of the table.
        param: task_id_true (str): Task ID to execute if table exists and has records.
        param: task_id_false (str): Task ID to execute if table does not exist or is empty.
        
        return: function: A branching task that checks the existence of the table.
        """
        if group_id:
            task_id_true = f"{group_id}.{task_id_true}"
            task_id_false = f"{group_id}.{task_id_false}"

        @task.branch(task_id=task_name)
        def select_load_type():
            client = bigquery.Client(project=PROJECT_ID)
            table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
            record_count = 0

            try:
                sql = f"SELECT COUNT(*) AS count FROM {table_id}"
                query_bq = client.query(sql)
                record_count = [value['count'] for value in query_bq][0]
                logging.info(f"Table {table_id} already exists.")
            except NotFound:
                logging.info(f"Table {table_id} is not found.")

            if record_count > 0:
                return task_id_true
            return task_id_false
        return select_load_type()

    @staticmethod
    def create_bq_load_task(source, bq_param, task_id, dataset_id,  table_id, task_type='full_load'):
        """
        Creates a BigQuery task for loading data into the silver table (full or incremental).

        param: process (str): The name of the process.
        param: source (str): The source of the data.
        param: bq_param (dict): BigQuery parameters.
        param: task_type (str): The type of the task ('full_load' or 'incremental_load'). Default is 'full_load'.

        return: BigQueryInsertJobOperator: The configured BigQuery task operator.
        """
        
        if task_type == 'full_load':
            query_path = f"silver/{source}/full/{table_id}.sql"
            write_disposition = bq_param.get('silver_write_disposition', 'WRITE_TRUNCATE')
            time_partitioning = {
                "type": bq_param.get("silver_type_partition"),
                "field": bq_param.get("silver_field_partition")
            }

            configuration = {
                "query": {
                    "query": "{% include '" + query_path + "' %}",
                    "allowLargeResults": True,
                    "useLegacySql": False,
                    "writeDisposition": write_disposition,
                    "timePartitioning": time_partitioning,
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": dataset_id,
                        "tableId": table_id
                    }
                }
            }
        elif task_type == 'incremental_load':
            query_path = f"silver/{source}/incremental/{table_id}.sql"
            configuration = {
                "query": {
                    "query": "{% include '" + query_path + "' %}",
                    "allowLargeResults": True,
                    "useLegacySql": False,
                }
            }
            
        else:
            raise ValueError(f"Unsupported task_type: {task_type}. Supported values are 'full_load' or 'incremental_load'.")
        
        return BigQueryInsertJobOperator(
            task_id=task_id,
            location=LOCATION,
            project_id=PROJECT_ID,
            deferrable=True,
            configuration=configuration
        )
        
class DagBaseConfig:
    def __init__(self, config):
        self.config = config

    def get_default_args(self):
        return {
            'owner': self.config["dag"]["owner"],
            'depends_on_past': False,
            'start_date': pendulum.today("America/Sao_Paulo").add(days=-1),
            'retries': self.config["dag"]["retries"],
            'retry_delay': timedelta(minutes=self.config["dag"]["retry_delay"]),
        }

    @staticmethod
    def get_event_dataset(events: list):
        return [Dataset(event) for event in events]

    def build_scheduler(self, dag_config: dict):
        time_schedule = dag_config.get("schedule")
        event_dataset_schedule = dag_config.get("event_dataset_schedule", [])

        if time_schedule and event_dataset_schedule:
            return DatasetOrTimeSchedule(
                timetable=CronTriggerTimetable(time_schedule, timezone="America/Sao_Paulo"),
                datasets=event_dataset_schedule,
            )
        elif event_dataset_schedule:
            return self.get_event_dataset(event_dataset_schedule)
        else:
            return time_schedule


class ELTDataPipeline(DagBaseConfig):
    """
    A class used to create Airflow DAGs dynamically based on a provided configuration of CloudFunctions and BigQuery.
    
    param: config (dict): A dictionary containing the configuration for the DAG and tasks.

    
    Config parameters:
    ----------
    OBS: The dag_id is inferred based on the conf yaml file name.
    dag(dict): The DAG level configuration.
        - owner (str): The owner of the DAG.
        - max_active_runs (int): The maximum number of active runs.
        - schedule (str): The cron schedule expression for the DAG.
        - description (str): The description of the DAG.
        - retries (int): The number of retries for failed tasks.
        - retry_delay (int): The delay in minutes between retries.
        - timeout (int): The timeout in minutes for the DAG run.
        - tags (list): List of tags associated with the DAG.
        - doc_md (str, optional): Documentation in markdown format for the DAG.

    common: Params common between tasks
        - form_dag_compilation (str): Dag ID of compilation result for Dataform
        - form_repository_id (str): Repository ID for Dataform
        - secret_db_url_conn (str): Secret Manager ID for DB connection URL.
        - secret_db_user (str): Secret Manager ID for DB user.
        - secret_db_pwd (str): Secret Manager ID for DB password.
  
    tasks(list): The list of tasks to be created in the DAG.
        - process (str): The name of the process.
        - source (str): The source of the data. Used for path of the sql files.
        - deletion (dict, optional): Configuration deletetion for Bronze table in BigQuery.
            - deletion_partition_bronze (bool): Whether to delete the partition day in the bronze table.
            - deletion_query (str): SQL string with or without parameters that will be used to delete data
            - deletion_file_query (str): Relative path the of file SQL with or without parameters.
            - use_query_params_dataflow (bool): Indicates whether the same parameters as the dataflow ingestion query will be used for deletion.
            - query_params: (dict): Parameters used in query or query file. Will only be used if use_query_params_dataflow is equal to False
                - key: value
        - ingestion (dict): Dict with params for ingestion layer
            - service (str): Ingestion Service. Must be 'dataflow' or 'cloudfunctions'
            Set the parameters below when the service is [dataflow]:
            - job_name (str): Job name.
            - template_name (str): Template name accepted 'SQLServer_to_BigQuery' or 'Oracle_to_BigQuery'.
            - connection_properties (str, optional): The properties string to use for the JDBC connection. The format of the string must be `[propertyName=property;]*`. (Example: unicode=true;characterEncoding=UTF-8)
            - use_column_alias (bool, optional): Use column alias. Default is False.
            - is_truncate (bool, optional): Truncate table, definded if ingest is full or incremental. Default is False.
            - query_full (str, optional): String SQL for full extraction. If not defined the "query_file_full" parameter will be used with of the default value.
            - query_incremental (str, optional): String SQL for incremental extraction, If not defined the "query_file_incremental" parameter will be used with of the default value.
            - query_file_full (str, optional): SQL file path .sql for ingest full. Default is "bronze/default_full_ingest.sql".
                For the default sql file it should be defined the parameters "source_schema" and "source_table" in "query_params".
            - query_file_incremental (str, optional): SQL file path .sql for ingest incremental. Default is "bronze/default_incremental_ingest.sql".
                For the default sql file it should be defined the parameters "source_schema", "source_table", "column_ref_delta", "date_init_rule" and "date_end_rule" in "query_params".
            - query_params (dict, optional): Dict with parameters to be substituted in "query" or "query_file"
                - key: value
                - date_init_rule: (str, optional) Rule for start date of extration.
                - date_end_rule: (str, optional) Rule for end date of extration.
                    The format of the date_init_rule or date_end_rule should be:
                        - Prefix with 'd' (day), 'm' (month), or 'y' (year).
                        - Followed by an operator '-' (subtract) or '+' (add).
                        - Suffix with a number indicating the amount of days, months, or years to add/subtract.
                        Examples:
                            - 'd-5' : 5 days ago
                            - 'm+1' : 1 month from now
                            - 'y-2' : 2 years ago
            - fetch_size (int, optional): The number of rows to be fetched from database at a time. Not used for partitioned reads. Defaults to: 50000.
            - num_workers (int, optional): Number of workers. Default is 2.
            - max_workers (int, optional): Maximum number of workers.
            
            Set the parameters below when the service is [cloudfunctions]:
            - function_name (str): The name of the Cloud Function.
            - request_method (str): The HTTP method to use (e.g., POST).
            - data_request (dict, optional): The data to be sent in the request.
            - http_conn_id (str, optional): The connection ID for the HTTP request.
            - trigger_rule (str, optional): The trigger rule for the task (default: 'all_success').
            
        - transformation (dict, optional): Configuration for Transformation tasks.
            - service (str): Ingestion Service. Must be 'dataform'.
            - silver_is_incremental (bool): Whether the load is incremental.
            - exec_dependencies(bool): Defines whether process dependencies will be executed in Dataform.
            - exec_dependents(bool): Defines whether process dependents will be executed in Dataform.
            
        - trigger_dag (list, optional): List of DAG IDs to trigger upon task completion.
        - dependencies (list, optional): List of process names that the current task depends on.
    """        
    def __init__(self, config):
        super().__init__(config)
        self.task_factory = TaskFactory()
        
    def create_dag(self):
        """
        Create Dag with a configuration.
        """
        try:
            default_args = self.get_default_args()
            dag_config = self.config["dag"]
            
            @dag(
                dag_id=dag_config["dag_id"],
                max_active_runs=dag_config["max_active_runs"],
                schedule=self.build_scheduler(dag_config),
                catchup=False,
                description=dag_config["description"],
                default_args=default_args,
                dagrun_timeout=timedelta(minutes=dag_config["timeout"]),
                tags=dag_config["tags"],
                template_searchpath=[SQL_FILE_PATH],
                doc_md=dag_config.get("doc_md")
            )
            def dag_elt_default():
                groups = {}
                tasks = {}
            
                start = EmptyOperator(task_id="start", task_display_name="Start 🏁")
                finish = EmptyOperator(task_id="finish", task_display_name="Finish 🏆")

                # Define variaveis comuns a todos os processos
                common_params = self.config.get("common",{})
                form_repository_id = common_params.get('form_repository_id', 'ghm-dataform-repo-default')
                
                # Obtém a dag de compilação para buscar a compilação via xcom.
                dag_compilation = common_params.get('form_dag_compilation')
                if dag_compilation:
                    last_compilation = self.task_factory.get_latest_compilation_xcom(dag_id=dag_compilation, task_id='compilation_result', key_xcom='return_value')
                
                # Cria tasks com base em cada processo
                for task_config in self.config['tasks']:
                    process = task_config['process']
                    source = task_config['source']
                
                    deletion = task_config.get('deletion', {})
                    ingestion = task_config.get('ingestion', {})
                    transformation = task_config.get('transformation', {})
                    dependencies = task_config.get('dependencies', [])
                    trigger_dags = task_config.get('trigger_dag', [])
            
                    # Cria a nomenclatura de dataset de acordo com os padrões estabelecidos
                    dataset_bronze = f"bronze_{source}" if not ingestion.get('destination_dataset') else ingestion['destination_dataset']
                    dataset_silver = f"silver_{source}"
                    layer_bronze = dataset_bronze.split("_")[0] 
                    layer_silver = dataset_silver.split("_")[0]
                    table_bronze = process
                    table_silver = f"tbl_{process}"
                    # Cria um evento de dataset para utilizar como trigger em outras dags.
                    event_dataset_table_silver = Dataset(f"{dataset_silver}.{table_silver}")
                    event_dataset_table_bronze = Dataset(f"bronze_{source}.{table_bronze}") if ingestion.get('create_event_dataset') else None
                    
                    group_id=f"group_{process}"
                    with TaskGroup(group_id=group_id) as task_group_process:
                        
                        # Cria tasks para seleção de carga incremental e a primeira carga completa.
                        if ingestion:
                                
                            if ingestion['service'].lower() == "dataflow":
                                
                                if not ingestion.get('is_truncate'):
                                    df_task_id_load_type = f"set_load_{layer_bronze}_{process}"
                                    df_task_id_incremental = f"flow_incremental_ingest_{layer_bronze}_{process}"
                                    df_task_id_full = f"flow_first_full_ingest_{layer_bronze}_{process}"
                                    
                                    flow_init_load_type_task = self.task_factory.check_empty_or_table_exists(
                                                                        task_name=df_task_id_load_type,
                                                                        dataset_id=dataset_bronze,
                                                                        table_name=table_bronze,
                                                                        group_id=group_id,
                                                                        task_id_true=df_task_id_incremental,
                                                                        task_id_false=df_task_id_full
                                                                    )
                                    tasks[f'flow_init_load_type'] = flow_init_load_type_task
                                    
                                    flow_first_full_task = self.task_factory.create_dataflow_jdbc_to_bq_task(
                                                                        process=process,
                                                                        source=source,
                                                                        flow_param=ingestion,
                                                                        common_params=common_params,
                                                                        task_id=df_task_id_full,
                                                                        dest_dataset=dataset_bronze,
                                                                        dest_table=table_bronze,
                                                                        task_type='full_load',
                                                                        event_dataset=event_dataset_table_bronze
                                                                    )
                                    
                                    flow_incremental_task = self.task_factory.create_dataflow_jdbc_to_bq_task(
                                                                        process=process,
                                                                        source=source,
                                                                        flow_param=ingestion,
                                                                        common_params=common_params,
                                                                        task_id=df_task_id_incremental,
                                                                        dest_dataset=dataset_bronze,
                                                                        dest_table=table_bronze,
                                                                        task_type='incremental_load',
                                                                        event_dataset=event_dataset_table_bronze
                                                                    )
                                    
                                    tasks[f'flow_end_load_type'] = EmptyOperator(task_id=f"end_load_type_brz_{process}", trigger_rule="none_failed_min_one_success")
                                    
                                    flow_init_load_type_task >> [flow_first_full_task, flow_incremental_task] >> tasks[f'flow_end_load_type']
                                else:
                                    flow_full_load_task = self.task_factory.create_dataflow_jdbc_to_bq_task(
                                                                        process=process,
                                                                        source=source,
                                                                        flow_param=ingestion,
                                                                        common_params=common_params,
                                                                        task_id=f"flow_full_ingest_{layer_bronze}_{process}",
                                                                        dest_dataset=dataset_bronze,
                                                                        dest_table=table_bronze,
                                                                        task_type='full_load',
                                                                        event_dataset=event_dataset_table_bronze
                                                                    )
                                    tasks[f'flow_full_load'] = flow_full_load_task
                                    
                            elif ingestion['service'].lower() == "cloudfunctions":
                                
                                task_id = f"gcf_ingest_{layer_bronze}_{process}"
                                tasks[f'trigger_cloudfunction'] = self.task_factory.create_cloudfunction_task(
                                                                        task_id=task_id, 
                                                                        gcf_params=ingestion
                                                                    )
                            
                            else:
                                raise ValueError("Ingestion service not found. Must be 'dataflow' or 'cloudfunction'!")
                            
                        # Task para deletar partição no BigQuery, se especificado
                        if deletion and deletion.get('deletion_partition_bronze'):

                            task_id = f'del_partition_{layer_bronze}_{process}'
                            del_partition_task = self.task_factory.create_del_partition_task(
                                                                task_id=task_id, 
                                                                del_param=deletion,
                                                                dataset_id=dataset_bronze, 
                                                                table_id=table_bronze,
                                                                flow_param=ingestion
                                                            )
                            # Configura sucessores da task de deleção de partição
                            ingestion_task_keys = ['trigger_cloudfunction', 'flow_full_load', 'flow_init_load_type']
                            for key in ingestion_task_keys:
                                if key in tasks:
                                    del_partition_task >> tasks[key]
                                    break

                        # Task para carga incremental no Dataform. Se existir a chave incremental com True no parâmetros do Dataform.
                        if transformation:
                            if transformation['service'] == 'dataform':
                                
                                if transformation.get("silver_is_incremental"):
                                    form_task_id_load_type = f"set_load_{layer_silver}_{process}"
                                    form_task_id_incremental = f"form_incremental_load_{layer_silver}_{process}"
                                    form_task_id_full = f"form_first_full_load_{layer_silver}_{process}"
                                    
                                    form_init_load_type_task = self.task_factory.check_empty_or_table_exists(
                                                                    task_name=form_task_id_load_type,
                                                                    dataset_id=dataset_silver, 
                                                                    table_name=table_silver,
                                                                    group_id=group_id,
                                                                    task_id_true=form_task_id_incremental,
                                                                    task_id_false=form_task_id_full
                                                                )
                                    form_full_task = self.task_factory.create_workflow_dataform_task(
                                                                    task_id=form_task_id_full,
                                                                    form_param=transformation,
                                                                    dataset_id=dataset_silver, 
                                                                    table_id=table_silver,
                                                                    repository_id=form_repository_id,
                                                                    last_compilation=last_compilation,
                                                                    event_dataset=event_dataset_table_silver,
                                                                    task_type='full_load'
                                                                )
                                    form_incremental_task = self.task_factory.create_workflow_dataform_task(
                                                                    task_id=form_task_id_incremental,
                                                                    form_param=transformation,
                                                                    dataset_id=dataset_silver, 
                                                                    table_id=table_silver,
                                                                    repository_id=form_repository_id,
                                                                    last_compilation=last_compilation,
                                                                    event_dataset=event_dataset_table_silver,
                                                                    task_type='incremental_load'
                                                                )
                                    form_end_load_type_task = EmptyOperator(task_id=f"end_load_type_slv_{process}", trigger_rule="none_failed_min_one_success")
                                    
                                    form_init_load_type_task >> [form_full_task, form_incremental_task] >> form_end_load_type_task
                                else:
                                    form_full_task = self.task_factory.create_workflow_dataform_task(
                                                                    task_id=f'form_full_load_{layer_silver}_{process}',
                                                                    form_param=transformation,
                                                                    dataset_id=dataset_silver, 
                                                                    table_id=table_silver,
                                                                    repository_id=form_repository_id,
                                                                    last_compilation=last_compilation,
                                                                    event_dataset=event_dataset_table_silver,
                                                                    task_type='full_load'
                                                                )

                                # Configura o predecessor das tasks de dataform
                                ingestion_task_keys = ['trigger_cloudfunction', 'flow_full_load', 'flow_end_load_type']
                                for key in ingestion_task_keys:
                                    if key in tasks:
                                        if transformation.get("silver_is_incremental"):
                                            tasks[key] >> form_init_load_type_task
                                        
                                        else:
                                            tasks[key] >> form_full_task
                             
                                        break
                            else:
                                raise ValueError("Transformation service not fount. Must be 'dataform'!!")   
                                     
                    groups[f"group_{process}"] = task_group_process

                    # Task para acionar outras DAGs
                    if trigger_dags:
                        for trigger_dag in trigger_dags:
                            trigger_dag_task = TriggerDagRunOperator(
                                task_id=f'trigger_{trigger_dag}',
                                trigger_dag_id=trigger_dag,
                            )
                        
                            task_group_process >> trigger_dag_task >> finish
                            
                    # Configurar dependências
                    if dependencies:
                        for dependency in dependencies:
                            dependency_group = groups.get(f'group_{dependency}')
                            if not dependency_group:
                                continue
                            else:
                                start >> dependency_group

                            if trigger_dags:
                                dependency_group >> task_group_process
                            else:
                                dependency_group >> task_group_process >> finish

                # Lógica para lidar com processos sem dependências e sem trigger_dags
                dependent_dependencies = [(task.get('process'), dependency) for task in self.config['tasks'] for dependency in task.get('dependencies', [])]
                for process in [task.get('process') for task in self.config['tasks']]:
                    if not any(process in dependency for dependency in dependent_dependencies):
                        start >> groups[f"group_{process}"] >> finish
                                                    

            return dag_elt_default()
        except Exception as e:
                raise RuntimeError(f"Error creation DAG {self.config['dag']['dag_id']}: {e}")