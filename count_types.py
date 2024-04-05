import argparse
from SPARQLWrapper import SPARQLWrapper

parser = argparse.ArgumentParser(description="Indexes all uris which have a rdf#type and rdfs#label.")
parser.add_argument("--sparql", default="http://localhost:3030/jvmg/update", help="Address to sparql endpoint (default: http://localhost:3030/jvmg/update)")
parser.add_argument("--graph", default="http://mediagraph.link/graph/jvmg", help="Graph in which the counts are stored (default: http://mediagraph.link/graph/jvmg)")
args = parser.parse_args()

count_query = f"""
PREFIX jvmg: <http://mediagraph.link/jvmg/ont/>

DELETE {{
  graph <{args.graph}> {{ ?type jvmg:count ?count }}
}}
INSERT {{
  graph <{args.graph}> {{ ?type jvmg:count ?new_count }}
}} WHERE {{
  SELECT DISTINCT ?type (COUNT (distinct ?entity) AS ?new_count) WHERE {{
    ?type jvmg:order ?order .
    ?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type
  }} group by ?type
}}
"""

sparql = SPARQLWrapper(args.sparql)
sparql.method = 'POST'
sparql.setQuery(count_query)
result = sparql.query()
