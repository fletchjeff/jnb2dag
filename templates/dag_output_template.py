from datetime import datetime, timedelta
from airflow.decorators import dag, task

{% for import in import_list -%}
{{ import }}
{% endfor -%}

{% for non_task_code in non_task_code_list -%}
{{ non_task_code }}
{% endfor -%}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

@dag(default_args=default_args, schedule_interval=None, start_date=datetime(2022, 1, 24), catchup=False)
def n2d_dag():
    """
    Notebook to DAG Demo
    """
    
    {% set count = namespace(value=0) %}
    {% for task in task_list_code -%}
        {% if count.value == 0 -%}
    @task
    def {{ tasks_list[count.value] }}():
        {% elif count.value > 0 -%}
    @task        
    def {{ tasks_list[count.value] }}({{ tasks_list[count.value-1] }}):
        {% endif -%}
            {% for task_code in task -%}
            {{ task_code }}
            {% endfor -%}
        
        {% set count.value = count.value + 1 %}
    {% endfor -%}
  
    
    {% set count = namespace(value=0) %}
    {% for task_name in tasks_list -%}
        {% if count.value == 0 -%}    
    {{ task_name }}_return = {{ tasks_list[count.value] }}()
        {% elif count.value > 0 -%}
    {{ task_name }}_return = {{ tasks_list[count.value] }}({{ tasks_list[count.value-1] }}_return)
        {% endif -%}
        {% set count.value = count.value + 1 %}
    {% endfor -%}

    return {{tasks_list[-1]}}_return

n2d_dag_run = n2d_dag()