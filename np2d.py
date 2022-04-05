from nbformat import read, NO_CONVERT
import re
import os
from jinja2 import Environment, FileSystemLoader

notebook_filename = "test_notebook.ipynb"
import_tag = "#@EXPORT imports"
non_task_code_tag = "#@EXPORT non_task_code"
tasks_list = ["#@EXPORT data_ingest_task", "#@EXPORT model_training_task", "#@EXPORT model_deployment_task"]

with open(notebook_filename, "r") as ipynb_file:
    ipynb = read(ipynb_file, NO_CONVERT)

#Doing this with tags
#tagged_cells = [cell for cell in cells if cell["cell_type"] == "code" if 'tags' in cell["metadata"]] 
#import_cells = [import_cells for import_cells in tagged_cells if 'imports' in import_cells['metadata']['tags']]

cells = ipynb["cells"]

import_cells = [cell for cell in cells if cell["cell_type"] == "code" if import_tag in cell["source"] if "#@EXCLUDE@" not in cell["source"]]

import_list = [re.sub(import_tag,"",imports_list["source"]) for imports_list in import_cells]

non_task_code_cells = [cell for cell in cells if cell["cell_type"] == "code" if non_task_code_tag in cell["source"] if "#@EXCLUDE@" not in cell["source"] ]
non_task_code_list = [re.sub(non_task_code_tag,"",non_task_code_list["source"]) for non_task_code_list in non_task_code_cells]

task_list_code = []
for task in tasks_list:
    task_cells = [cell for cell in cells if cell["cell_type"] == "code" if task in cell["source"] if "#@EXCLUDE@" not in cell["source"] ]
    task_list = [re.sub("\n","\n        ",re.sub(task,"",task_list["source"])) for task_list in task_cells]
    task_list_code.append(task_list)
    
env = Environment(
    autoescape=False,
    loader=FileSystemLoader('templates'),
    trim_blocks=False)

fname = "output_dag.py"
context = {
    'import_list': import_list,
    'non_task_code_list' : non_task_code_list,
    'task_list_code' : task_list_code,
    'tasks_list' : [re.sub("#@EXPORT ","",task_names) for task_names in tasks_list]
}
with open(fname, 'w') as f:
    dag_code = env.get_template('dag_output_template.py').render(context)
    f.write(dag_code)