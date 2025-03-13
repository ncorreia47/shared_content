from include.utils import create_dags_from_yaml   

folder_config = "receita_operacional_bruta"
file_prefix = None
template_type = "elt"

create_dags_from_yaml(folder_config=folder_config, file_prefix=file_prefix, template_type=template_type)