from airflow.decorators import dag, task
from airflow.utils.timezone import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartSqlJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from datetime import timedelta
import logging
import json
import os

PROJECT_ID = os.getenv("PROJECT")
LOCATION = os.getenv("LOCATION")
URL_SUBNET = os.getenv("URL_SUBNET")
URL_BASE_GCF = os.getenv("URL_BASE_GCF")
SA_DATAFLOW = os.getenv("SA_DATAFLOW")
GCS_BUCKET_DATAFLOW = os.getenv("BUCKET_DATAFLOW")
PATH_DF_TEMPLATE = os.getenv("PATH_DF_TEMPLATE")
SQL_FILE_PATH = os.getenv("SQL_FILE_PATH")
KMS_KEY_RING = os.getenv("KMS_KEY_RING")
KMS_CRYPTO_KEY_DB = os.getenv("KMS_CRYPTO_KEY_DB")

class FlexDAGBuilder:
    """
    Class to more flexibly build and create Airflow DAGs based on a provided configuration.
    
    param: config (dict): A dictionary containing the configuration for the DAG and tasks.

    Config parameters:
    ----------
    dag(dict): The DAG level configuration.
        - owner (str): The owner of the DAG.
        - dag_id (str): The ID of the DAG.
        - max_active_runs (int): The maximum number of active runs.
        - schedule (str): The cron schedule expression for the DAG.
        - description (str): The description of the DAG.
        - retries (int): The number of retries for failed tasks.
        - retry_delay (int): The delay in minutes between retries.
        - timeout (int): The timeout in minutes for the DAG run.
        - tags (list): List of tags associated with the DAG.
        - doc_md (str, optional): Documentation in markdown format for the DAG.

    tasks(list): The list of tasks to be created in the DAG.
        - task_id (str): Complete task id.
        - operator (str): Custom operator defined in the "create_task operator" method.
        - params (dict, optional): Necessary parameters for operators.
        - dependencies (list, optional): List of task ID names that the current task depends on.

    """

    def __init__(self, config):
        self.config = config

    def create_dag(self):
        """
        Creates an Airflow DAG dynamically based on the provided configuration.

        returns: dag: The created DAG.
        """
        default_args = {
            'owner': self.config["dag"]["owner"],
            'depends_on_past': False,
            'start_date': pendulum.today("America/Sao_Paulo").add(days=-1),
            'retries': self.config["dag"]["retries"],
            'retry_delay': timedelta(minutes=self.config["dag"]["retry_delay"]),
        }

        @dag(
            dag_id=self.config["dag"]["dag_id"],
            max_active_runs=self.config["dag"]["max_active_runs"],
            schedule=self.config["dag"].get("schedule"),
            catchup=False,
            description=self.config["dag"]["description"],
            default_args=default_args,
            dagrun_timeout=timedelta(minutes=self.config["dag"]["timeout"]),
            tags=self.config["dag"]["tags"],
            template_searchpath=[SQL_FILE_PATH],
            doc_md=self.config["dag"].get("doc_md")
        )
        def dynamic_dag():
            tasks = {}
            for task_config in self.config['tasks']:
                task = self.create_task_operator(task_config)
                tasks[task_config['task_id']] = task
            
            # Definindo dependências entre tarefas
            for task_config in self.config['tasks']:
                task_id = task_config['task_id']
                if 'dependencies' in task_config:
                    for dependency in task_config['dependencies']:
                        tasks[dependency] >> tasks[task_id]

        return dynamic_dag()

    def create_task_operator(self, task_config):
        """
        Creates a task worker based on the given configuration.

        param: task_config (dict): Task configuration, including task ID, operator, and parameters.

        return: Operator: A configured Airflow operator.
        """
        task_id = task_config['task_id']
        operator = task_config['operator']
        param = task_config.get('params', {})

        if operator == 'Empty':
            return self._create_empty_operator(task_id, param)
        elif operator == 'GCFHttp':
            return self._create_gcf_http_operator(task_id, param)
        elif operator == 'BQQueryJob':
            return self._create_bq_query_job_operator(task_id, param)
        elif operator == 'BQQueryJobDestination':
            return self._create_bq_query_job_destination_operator(task_id, param)
        elif operator == 'DFJdbcToBQ':
            return self._create_df_jdbc_to_bq_operator(task_id, param)
        elif operator == 'DFSqlJob':
            return self._create_df_sql_job_operator(task_id, param)
        elif operator == 'TriggerDag':
            return self._create_trigger_dag_operator(task_id, param)
        elif operator == 'SelectLoadType':
            return self._create_select_load_type_operator(task_id, param)
        else:
            raise ValueError(f"Unsupported operator: {operator}")

    def _create_empty_operator(self, task_id, param):
        """
        Create an empty operator.
        
        param: task_id (str): ID of the Task.
        param: params (dict): Task parameters.
                - trigger_rule (str, optional): Trigger rule for the operator. Default is 'all_success'.
        
        return: EmptyOperator: Configured EmptyOperator.
        """
        return EmptyOperator(task_id=task_id, trigger_rule=param.get('trigger_rule', 'all_success'))

    def _create_gcf_http_operator(self, task_id, param):
        """
        Create an HttpOperator for invoking a Google Cloud Function.
        
        param: task_id (str): ID of the Task.
        param: params (dict): Task parameters.
                - function_name (str): Name of the Google Cloud Function.
                - request_method (str, optional): HTTP method for the request. Default is 'POST'.
                - http_conn_id (str, optional): HTTP connection ID. Default is 'gcf_conn_id'.
                - data_request (dict, optional): Request data.
                - trigger_rule (str, optional): Trigger rule for the operator. Default is 'all_success'.
        
        return: 
            HttpOperator: Configured HttpOperator.
        """
        endpoint_gcf = param['function_name']
        url_gcf = f"{URL_BASE_GCF}/{endpoint_gcf}"
        TOKEN_GCF = fetch_id_token(Request(), url_gcf)
        return HttpOperator(
            task_id=task_id,
            method=param.get("request_method", "POST"),
            http_conn_id=param.get("http_conn_id", "gcf_conn_id"),
            endpoint=endpoint_gcf,
            headers={'Authorization': f"Bearer {TOKEN_GCF}", "Content-Type": "application/json"},
            data=json.dumps(param['data_request']) if param.get('data_request') else None,
            deferrable=False,
            trigger_rule=param.get('trigger_rule', 'all_success')
        )

    def _create_bq_query_job_operator(self, task_id, param):
        """
        Create a BigQuery Insert Job operator.
        
        param: task_id (str): ID of the Task.
        param: params (dict): Task parameters.
                - query (str): The query to be executed.
                - trigger_rule (str, optional): Trigger rule for the operator. Default is 'all_success'.
        
        return: BigQueryInsertJobOperator: Configured BigQueryInsertJobOperator.
        """
        return BigQueryInsertJobOperator(
            task_id=task_id,
            location=LOCATION,
            project_id=PROJECT_ID,
            deferrable=True,
            configuration={
                "query": {
                    "query": "{% include '" + param['query'] + "' %}" if ".sql" in param['query'] else param['query'],
                    "allowLargeResults": True,
                    "useLegacySql": False,
                }    
            },
            trigger_rule=param.get('trigger_rule', 'all_success')
        )

    def _create_bq_query_job_destination_operator(self, task_id, param):
        """
        Create a BigQuery Insert Job operator with a destination table.
        
        param: task_id (str): ID of the Task.
        param: params (dict): Task parameters.
                - query (str): The query to be executed.
                - type_partition (str, optional): Type of partitioning for the table.
                - field_partition (str, optional): Field used for partitioning.
                - write_disposition (str, optional): Write disposition. Default is 'WRITE_TRUNCATE'.
                - dataset_id (str): Dataset ID for the destination table.
                - table_id (str): Table ID for the destination table.
                - trigger_rule (str, optional): Trigger rule for the operator. Default is 'all_success'.
        
        return: BigQueryInsertJobOperator: Configured BigQueryInsertJobOperator.
        """
        return BigQueryInsertJobOperator(
            task_id=task_id,
            location=LOCATION,
            project_id=PROJECT_ID,
            deferrable=True,
            configuration={
                "query": {
                    "query": "{% include '" + param['query'] + "' %}" if ".sql" in param['query'] else param['query'],
                    "allowLargeResults": True,
                    "useLegacySql": False,
                    "timePartitioning": {
                        "type": param.get("type_partition"),
                        "field": param.get("field_partition")
                    },
                    "writeDisposition": param.get('write_disposition', 'WRITE_TRUNCATE'),
                    "destinationTable": {
                        "projectId": PROJECT_ID,
                        "datasetId": param["dataset_id"],
                        "tableId": param["table_id"]
                    }
                }
            },
            trigger_rule=param.get('trigger_rule', 'all_success')
        )

    def _create_df_jdbc_to_bq_operator(self, task_id, param):
        """
        Create a Dataflow Flex Template operator for JDBC to BigQuery.
        
        param: task_id (str): ID of the Task.
        param: param (dict): Task parameters.
                - job_name (str): Job name.
                - secret_db_url_conn (str): Secret Manager ID for DB connection URL.
                - secret_db_user (str): Secret Manager ID for DB user.
                - secret_db_pwd (str): Secret Manager ID for DB password.
                - query (str): SQL query.
                - dataset_destino (str): Destination dataset ID.
                - table_destino (str): Destination table ID.
                - use_column_alias (bool, optional): Use column alias. Default is False.
                - is_truncate (bool, optional): Truncate table. Default is False.
                - fetch_size (int, optional): Fetch size. Default is 50000.
                - num_workers (int, optional): Number of workers. Default is 2.
                - max_workers (int, optional): Maximum number of workers.
                - template_name (str): Template name.
                - trigger_rule (str, optional): Trigger rule for the operator. Default is 'all_success'.
        
        return: DataflowStartFlexTemplateOperator: Configured DataflowStartFlexTemplateOperator.
        """
        secret_db_url_conn = param['secret_db_url_conn'].replace("sm_", "")
        secret_db_user = param['secret_db_user'].replace("sm_", "")
        secret_db_pwd = param['secret_db_pwd'].replace("sm_", "")
        return DataflowStartFlexTemplateOperator(
            task_id=task_id,
            project_id=PROJECT_ID,
            body={
                "launchParameter": {
                    "jobName": param['job_name'].replace("_", "-"),
                    "parameters": {
                        "connectionURL": "{{var.value." + f"{secret_db_url_conn}" + "}}",
                        "username": "{{var.value." + f"{secret_db_user}" + "}}",
                        "password": "{{var.value." + f"{secret_db_pwd}" + "}}",
                        "query": "{% include '" + param['query'] + "' %}" if ".sql" in param['query'] else param['query'],
                        "bigQueryLoadingTemporaryDirectory": f"{GCS_BUCKET_DATAFLOW}/temp/",
                        "useColumnAlias": str(param.get('use_column_alias', 'false')).lower(),
                        "isTruncate": str(param.get('is_truncate', 'false')).lower(),
                        "outputTable": f"{PROJECT_ID}:{param['dataset_destino']}.{param['table_destino']}",
                        "fetchSize": param.get('fetch_size', '50000'),
                        "KMSEncryptionKey": f"projects/{PROJECT_ID}/locations/{LOCATION}/keyRings/{KMS_KEY_RING}/cryptoKeys/{KMS_CRYPTO_KEY_DB}"
                    },
                    "environment": {
                        "numWorkers": param.get('num_workers', '2'),
                        "maxWorkers": param.get('max_workers'),
                        "ipConfiguration": "WORKER_IP_PRIVATE",
                        "serviceAccountEmail": SA_DATAFLOW,
                        "stagingLocation": f"{GCS_BUCKET_DATAFLOW}/staging/",
                        "tempLocation": f"{GCS_BUCKET_DATAFLOW}/temp/",
                        "subnetwork": URL_SUBNET,
                    },
                    "containerSpecGcsPath": f"{PATH_DF_TEMPLATE}/{param['template_name']}",
                }
            },
            location=LOCATION,
            deferrable=True,
            trigger_rule=param.get('trigger_rule', 'all_success')
        )

    def _create_df_sql_job_operator(self, task_id, param):
        """
        Create a Dataflow SQL Job operator.
        
        param: task_id (str): ID of the Task.
        param: params (dict): Task parameters.
                - job_name (str): Job name.
                - query (str): SQL query.
                - options (dict): Options for the Dataflow SQL job.
                - trigger_rule (str, optional): Trigger rule for the operator. Default is 'all_success'.
        
        return: DataflowStartSqlJobOperator: Configured DataflowStartSqlJobOperator.
        """
        return DataflowStartSqlJobOperator(
            task_id=task_id,
            job_name=param['job_name'],
            query=param['query'],
            project_id=PROJECT_ID,
            location=LOCATION,
            options={
                "bigquery_write_disposition": param["options"]["write_disposition"],
                "service_account_email": SA_DATAFLOW,
                "subnetwork": "",
            },
            trigger_rule=param.get('trigger_rule', 'all_success')
        )
    
    def _create_trigger_dag_operator(self, task_id, param):
        """
        Create a TriggerDagRunOperator to trigger another DAG.

        param: task_id (str): ID of the Task.
        param: params (dict): Task parameters.
                - trigger_dag_id (str): ID of the DAG to trigger.
                - wait_for_completion (bool, optional): Whether to wait for the triggered DAG to complete. Default is False.
                - trigger_rule (str, optional): Trigger rule for the operator. Default is 'all_success'.

        return: TriggerDagRunOperator: Configured TriggerDagRunOperator.
        """
        return TriggerDagRunOperator(
            task_id=task_id,
            trigger_dag_id=param['trigger_dag_id'],
            wait_for_completion=param.get('wait_for_completion', False),
            deferrable=True,
            trigger_rule=param.get('trigger_rule', 'all_success')
        )

    def _create_select_load_type_operator(self, task_id, param):
        """
        Create a branch operator to select the loading type based on BigQuery table existence and emptiness.

        param: task_id (str): ID of the Task.
        param: params (dict): Task parameters.
                - dataset_id (str): Dataset ID of the BigQuery table.
                - table_id (str): Table ID of the BigQuery table.
                - task_ids_incremental (str): Task ID to execute if the table exists and is not empty.
                - task_ids_full (str): Task ID to execute if the table does not exist or is empty.

        return: task.branch: Branch function to select the appropriate next task based on table existence and record count.
        """
        @task.branch(task_id=task_id)
        def check_empty_or_table_exists():
            """
            Check if a BigQuery table exists and if it is empty or not.

            return: str: The task ID to be executed next based on table existence and record count.
            """
            client = bigquery.Client(project=PROJECT_ID)
            table_id = f"{PROJECT_ID}.{param['dataset_id']}.{param['table_id']}"
            record_count = 0
            
            try:
                sql = f"SELECT COUNT(*) AS count FROM `{param['dataset_id']}.{param['table_id']}`"
                query_bq = client.query(sql)
                record_count = [value['count'] for value in query_bq][0]
                logging.info(f"Table {table_id} already exists.")
            except NotFound:
                logging.info(f"Table {table_id} is not found.")
                
            if record_count > 0:
                return param['task_ids_incremental']
            return param['task_ids_full']
        
        return check_empty_or_table_exists()