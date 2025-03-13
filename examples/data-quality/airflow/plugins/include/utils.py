import os

def load_config_yaml(path_params: str, file_name: str):
    import yaml
    import os

    file_path = os.path.join(path_params, file_name)
    
    try:
        with open(file_path, 'r') as y:
            config = yaml.load(y.read(), Loader=yaml.FullLoader)
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {file_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error loading YAML file: {file_path}. Error: {e}")

def load_sql_file_and_replace_params(query_relative_path: str, query_params: dict = {}):
    sql_path = os.path.join(os.getcwd(), "plugins", "include", "sql")

    with open(f"{sql_path}/{query_relative_path}", 'r') as file:
        sql = file.read()

    substituted_sql = sql.format(**query_params)
    return substituted_sql

def create_dags_from_yaml(template_type: str, folder_config: str=None, file_prefix: str=None):
    from include.template.external_dependency_pipeline import ExternalDependencyPipeline
    from include.template.elt_data_pipeline import ELTDataPipeline
    
    """
    Cria DAGs dinamicamente com base nos arquivos YAML encontrados no diretório especificado.

    Args:
        folder_config (str): O caminho do diretório onde os arquivos YAML estão localizados, se não for passado o dir padrão é o configs
        file_prefix (str): O prefixo dos arquivos YAML a serem processados, se não for passado todos os arquivos dentro do diretório serão processados.
        template_type (str): Tipo de template para determinar qual classe de DAG usar. Suporta "elt" e "external".

    Returns:
        dict: Um dicionário de DAGs criados, onde a chave é o ID do DAG e o valor é o objeto DAG.
    """
    base_path = os.path.join(os.getcwd(), "plugins", "include", "configs")
    path_args = os.path.join(base_path, folder_config) if folder_config else base_path
    
    yaml_files = [
        file for file in os.listdir(path_args)
        if file.endswith('.yaml') and (not file_prefix or file.startswith(file_prefix))
    ]
    

    for yaml_file in yaml_files:
        dag_id = yaml_file.replace('.yaml', '')
        
        if "controller" in yaml_file:
            dag_id = dag_id.replace('conf_', '')
            current_template = 'external'
        else:
            dag_id = dag_id.replace('conf', 'dag')
            current_template = template_type
        
        config = load_config_yaml(path_args, yaml_file)
        config["dag"]["dag_id"] = dag_id
        
        
        if current_template == 'external':
            dag_creator_class = ExternalDependencyPipeline
        elif current_template == 'elt':
            dag_creator_class = ELTDataPipeline
        else:
            raise ValueError(f"Template type '{template_type}' is not recognized.")
        
        globals()[dag_id] = dag_creator_class(config).create_dag()
