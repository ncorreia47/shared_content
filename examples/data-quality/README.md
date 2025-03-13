# Template para aplicação de regras de qualidade de dados

Esse é um exemplo de implementação de regras de qualidade de dados utilizando Airflow e Dataform -- similar ao dbt

Na pasta **`airflow/dags`**, você vai encontrar o código que representa a dag **`[dag_receita_operacional_bruta.py]`**.
Essa dag é construída dinamicamente, através da função **`utils.py`**, disponível na pasta **`plugins/include`**. A função utils.py realizará as chamadas para construção das dags no template 'elt' ou 'external'.

Na mesma pasta plugins/include, você encontrará a pasta de configs, sql e template.

Essa organização pode ser representada da seguinte maneira.

```
├── configs 
  └── conf_controller_context.py [DAG controladora de DAGs do mesmo contexto]
  └── conf_context.py            [DAG do contexto]

├── sql
  ├── bronze
  ├── silver
  ├── gold  
  [Você pode separar seus SQLs em zonas, podendo existir subdivisões]

├── template
  └── elt_template.py
  └── external_template.py
  [Você pode criar templates para reutilizá-los na sua arquitetura de dados]
```

Em relação ao dataform a organização pode ser representada da seguinte maneira:

```
├── assertions [você pode definir suas validações, separando-as em subdivisões]
├── bronze [todas as configurações para tabelas da zona bronze, podendo conter subdivisões]
├── silver [todas as configurações para tabelas da zona silver, podendo conter subdivisões]
├── gold [todas as configurações para tabelas da zona gold, podendo conter subdivisões]
```
