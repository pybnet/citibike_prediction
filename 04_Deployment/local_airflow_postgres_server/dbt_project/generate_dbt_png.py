import json
from graphviz import Digraph

with open('target/manifest.json') as f:
    manifest = json.load(f)

dot = Digraph(comment='DBT DAG')
for node_name, node in manifest['nodes'].items():
    dot.node(node_name, node['name'])
for node_name, node in manifest['nodes'].items():
    for dep in node['depends_on']['nodes']:
        dot.edge(dep, node_name)

dot.render('target/dbt_dag', format='png', cleanup=True)