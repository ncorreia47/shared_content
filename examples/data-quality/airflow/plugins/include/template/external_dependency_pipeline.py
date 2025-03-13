from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.operators.dataform import DataformCreateWorkflowInvocationOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.models.taskinstance import TaskInstanceKey
from airflow.operators.empty import EmptyOperator
from google.cloud.exceptions import NotFound
from airflow.utils.timezone import pendulum
from airflow.models.dagrun import DagRun
from airflow.datasets import Dataset
from airflow.models.xcom import XCom
from google.cloud import bigquery
from datetime import timedelta
import logging
import os

ENV = os.getenv("ENV")
PROJECT_ID = os.getenv("PROJECT")
LOCATION = os.getenv("LOCATION")
PATH_DF_TEMPLATE = os.getenv("PATH_DF_TEMPLATE")
SQL_FILE_PATH = os.getenv("SQL_FILE_PATH")

class ExternalDependencyPipeline:
    """
    The `ExternalDependencyPipelineTemplate` class is designed to dynamically create Airflow DAGs that manage external dependencies and trigger workflows based on context-aware processing. This template is ideal for scenarios where integration with external systems, dataset events, and business processes is required.

    Key Features:

    - **External DAG Integration**: Supports triggering and waiting for external DAGs. Can handle both `trigger` and `sensor` types, allowing for orchestration and synchronization with other workflows.
    - **Context-Aware Transformation**: Facilitates the creation and execution of Dataform tables, with support for defining dependencies on other tables and external DAGs.
    - **Dynamic Task Creation**: Generates tasks based on a provided configuration, including handling first full loads and incremental updates with Dataform.
    - **Event-Based Scheduling**: Can be configured to trigger DAGs based on dataset events or time schedules, offering flexibility in scheduling and execution.
    - **Exception Handling**: Includes mechanisms to handle failures and retries, ensuring robustness in data pipeline execution.

    Parameters:
    -----------
    config (dict): A dictionary containing the configuration for the DAG and tasks.

    Config parameters:
    ------------------
    OBS: The dag_id is inferred based on the conf yaml file name.
    dag (dict): The DAG level configuration.
        - owner (str): The owner of the DAG.
        - dag_id (str): The ID of the DAG.
        - max_active_runs (int): The maximum number of active runs.
        - schedule (str): The cron time schedule expression for the DAG.
        - event_dataset_schedule (list): List with the names of dataset events formed by "dataset_id.table_id" to trigger the dag. Can be used as a replacement for external sensors.
            For more information on how to use datasets and data-aware scheduling in airflow, access the official document link: https://www.astronomer.io/docs/learn/airflow-datasets
        - description (str): The description of the DAG.
        - retries (int): The number of retries for failed tasks.
        - retry_delay (int): The delay in minutes between retries.
        - timeout (int): The timeout in minutes for the DAG run.
        - tags (list): List of tags associated with the DAG.
        - doc_md (str, optional): Documentation in markdown format for the DAG.
    common: Params common between tasks
        - form_repository_id (str): Repository ID for Dataform
        - form_dag_compilation (str): Dag ID of compilation result for Dataform
    pipeline (dict): The dictionary of tasks to be created in the DAG.
        external_dags (list): List of external DAGs to trigger or sensor before executing the main DAG tasks. Can be used dataset data-aware scheduling as a replacement.
            - dag_id (str): The ID of the external DAG.
            - type (str): The type of external task. 'trigger' to trigger the DAG, 'sensor' to wait for the completion of a specific task in the DAG.
            - task_ids (list, optional): List of task IDs to wait for in the external DAG if type is 'sensor'. Defaults to empty list.
            - execution_delay_minutes (int, optional): The time difference, in minutes, between scheduling the external DAG and the current DAG if type is 'sensor'. Defaults to 0.
            - poke_interval_seconds (int, optional): The interval in seconds to wait between each check for the external task completion if type is 'sensor'. Defaults to 60 seconds.
            - timeout_minutes (int, optional): The maximum time in minutes to wait for the external task completion if type is 'sensor'. Defaults to 240 minutes.

        dataform_tables (list): List of Dataform tasks to create and execute tables with dependencies.
            - table_id (str): The ID of the Dataform table to create.
            - dataset_id (str): The dataset ID where the Dataform table resides.
            - exec_dependencies(bool): Defines whether process dependencies will be executed in Dataform.
            - exec_dependents(bool): Defines whether process dependents will be executed in Dataform.
            - fist_full_load(bool): Enables the first full load.
            - external_dependencies (list, optional): List of external DAG IDs that need to be completed before executing the Dataform task.
            - dependencies (list, optional): List of Dataform table task IDs that need to be completed before executing the current Dataform task.
    """
    def __init__(self, config):
        self.config = config

    def create_sensor_task(self, dag_id: str, task_ids: str, poke_interval: int, timeout: int, execution_delta: int):
        return ExternalTaskSensor(
            task_id=f"wait_{dag_id}",
            external_dag_id=dag_id,
            # Descomentar caso deseje que a task falhe imediatamente quando as tasks externas falhar.
            # failed_states=[TaskInstanceState.FAILED.value],
            external_task_ids=task_ids,
            execution_delta=timedelta(minutes=execution_delta),
            poke_interval=poke_interval,
            # Caso atinja o tempo limite a tarefa irá falhar.
            timeout=timeout*60,
            mode='reschedule',
            trigger_rule="all_done"
        )

    def create_trigger_task(self, trigger_dag_id: str, wait_for_completion: bool):
        return TriggerDagRunOperator(
            task_id=f"trigger_{trigger_dag_id}",
            trigger_dag_id=trigger_dag_id,
            wait_for_completion=wait_for_completion,
            deferrable=False,
            trigger_rule='all_success'
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
    
    def create_workflow_dataform_task(self, task_id, form_param, dataset_id,  table_id, last_compilation, event_dataset):
        
        # Create a task to invoke the Dataform workflow for the table.
        return DataformCreateWorkflowInvocationOperator(
                task_id=task_id,
                project_id=PROJECT_ID,
                region=LOCATION,
                repository_id=self.config['common'].get('form_repository_id', 'ghm-dataform-repo-default'),
                workflow_invocation={
                    "compilation_result": last_compilation.get('name'),
                    "invocation_config": {
                        # "included_tags": tags,
                        "included_targets": [{"database": PROJECT_ID, "name": table_id, "schema": dataset_id}],
                        "transitive_dependencies_included": form_param.get('exec_dependencies', False),
                        "transitive_dependents_included": form_param.get('exec_dependents', False),
                        "fully_refresh_incremental_tables_enabled": form_param.get('fist_full_load', False)
                    },
                },
                outlets=[event_dataset] # Atualiza o dataset para essa task
            )
    
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
            def dag_external_pipeline():
                
                # Define variaveis comuns a todos os processos
                common_params = self.config.get("common",{})
                
                # Obtém a dag de compilação para buscar a compilação via xcom.
                dag_compilation = common_params.get('form_dag_compilation')
                if dag_compilation:
                    last_compilation = self.get_latest_compilation_xcom(dag_id=dag_compilation, task_id='compilation_result', key_xcom='return_value')
                
                external_dags = self.config['pipeline'].get("external_dags", [])
                dataform_tables = self.config['pipeline'].get("dataform_tables", [])
                
                start = EmptyOperator(task_id="start", task_display_name="Start 🏁")
                finish = EmptyOperator(task_id="finish", task_display_name="Finish 🏆")
                
                tasks = {}
                if external_dags:
                    with TaskGroup(group_id=f"group_external_dags") as external_dags_group:
                        for step in external_dags:
                            dag_to_process = step['dag_id']
                            task_type = step['type']

                            if task_type == 'sensor':
                                poke_interval = step.get('poke_interval_seconds', 60)
                                task_ids = step.get("task_ids", [])
                                timeout = step.get('timeout_minutes', 240)
                                execution_delay = step.get('execution_delay_minutes', 0)
                                task = self.create_sensor_task(dag_to_process, task_ids, poke_interval, timeout, execution_delay)
                            elif task_type == 'trigger':
                                wait_for_completion = step.get("wait_for_completion", False)
                                task = self.create_trigger_task(dag_to_process, wait_for_completion)

                            tasks[dag_to_process] = task

                            if 'depends_on' in step:
                                for dependency in step['depends_on']:
                                    tasks[dependency] >> task
                            else:
                                task
                else:
                    external_dags_group = None
                    
                if dataform_tables:
                    # Create Dataform tasks for tables
                    dataform_tasks = {}
                    group_id = "group_workflow_dataform"
                    with TaskGroup(group_id=group_id) as dataform_group:
                        
                        for form_task_config in dataform_tables:
                            table_id = form_task_config['table_id']
                            dataset_id = form_task_config['dataset_id']
                            dataset_zone = form_task_config['dataset_id'].split("_")[0]
                            
                            # Cria um evento de dataset para utilizar como trigger em outras dags.
                            event_dataset_table = Dataset(f"{dataset_id}.{table_id}")
                            
                            task_id_full = f"full_load_{dataset_zone}_{table_id}"
                            fist_full_load = form_task_config.get('fist_full_load')
                            if fist_full_load:
                        
                                task_id_load_type = f"set_load_{dataset_zone}_{table_id}"
                                task_id_incremental = f"incremental_load_{dataset_zone}_{table_id}"
                                
                                dataform_tasks[f'default_{table_id}'] = self.check_empty_or_table_exists(
                                                                    task_name=task_id_load_type,
                                                                    dataset_id=dataset_id,
                                                                    table_name=table_id,
                                                                    group_id=group_id,
                                                                    task_id_true=task_id_incremental,
                                                                    task_id_false=task_id_full
                                                                )
                                form_incremental_task = self.create_workflow_dataform_task(task_id_incremental, form_task_config, dataset_id,
                                                                    table_id, last_compilation, event_dataset_table
                                                                )
                                form_full_task = self.create_workflow_dataform_task(task_id_full, form_task_config, dataset_id,
                                                                    table_id, last_compilation, event_dataset_table
                                                                )
                                
                                dataform_tasks[f'end_load_type_{table_id}'] = EmptyOperator(task_id=f"end_load_{dataset_zone}_{table_id}", trigger_rule="none_failed_min_one_success")
                                
                                dataform_tasks[f'default_{table_id}'] >> [form_full_task, form_incremental_task] >> dataform_tasks[f'end_load_type_{table_id}']
                            else:
                                form_full_task = self.create_workflow_dataform_task(task_id_full, form_task_config, dataset_id, table_id, last_compilation, event_dataset_table)
                                dataform_tasks[f'default_{table_id}'] = form_full_task

                            # Set external dependencies for Dataform tasks
                            for external_dependency in form_task_config.get('external_dependencies', []):
                                if external_dependency in tasks:
                                    tasks[external_dependency] >> dataform_tasks[f'default_{table_id}']
                                else:
                                    raise ValueError(f"External dependency {external_dependency} not found in pipeline tasks.")

                            # Set internal dependencies for Dataform tasks
                            for dependency in form_task_config.get('dependencies', []):
                                if f"end_load_type_{dependency}" in dataform_tasks:
                                    dataform_tasks[f'end_load_type_{dependency}'] >> dataform_tasks[f'default_{table_id}']
                                    
                                elif f"default_{dependency}" in dataform_tasks:
                                    dataform_tasks[f'default_{dependency}'] >> dataform_tasks[f'default_{table_id}'] 
                                else:
                                    raise ValueError(f"Internal dependency {dependency} not found in dataform tasks.")
                else:
                    dataform_group = None
                    
                if external_dags_group and dataform_group:
                    start >> external_dags_group
                    dataform_group >> finish
                elif external_dags_group and not dataform_group:
                    start >> external_dags_group >> finish
                else:
                    start >> dataform_group >> finish
                    
            return dag_external_pipeline()
        except Exception as e:
                raise RuntimeError(f"Error creation DAG {dag_config['dag_id']}: {e}")