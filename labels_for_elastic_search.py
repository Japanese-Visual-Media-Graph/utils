import elasticsearch
from elasticsearch.helpers import bulk
from SPARQLWrapper import SPARQLWrapper, JSONLD
from rdflib import URIRef
from tqdm import tqdm

sparql = SPARQLWrapper("http://localhost:3030/public")
sparql.setReturnFormat(JSONLD)
index_name = "default"


es = elasticsearch.Elasticsearch("http://localhost:9200")
es.indices.delete(index=index_name, ignore=[400, 404])
settings =  {
        "number_of_shards": 6,
        "number_of_replicas": 1
    }
mappings = {'properties': {
        'uri': {'type': 'keyword'},
        'label': {'type': 'wildcard'},
        'graph': {'type': 'wildcard'},
        'type': {'type': 'wildcard'}}}

es.indices.create(index=index_name, ignore=400, settings=settings, mappings=mappings)

query = """
prefix label: <http://www.w3.org/2000/01/rdf-schema#label>
prefix type: <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
prefix graph_label: <http://mediagraph.link/jvmg/ont/shortLabel>
construct {
  graph ?graph_uri {
    ?uri label: ?label .
    ?uri type: ?type_uri .
    ?graph_uri graph_label: ?graph .
    ?type_uri label: ?type
  }
} WHERE {
  graph ?graph_uri {
    ?uri label: ?label .
    ?uri type: ?type_uri
    optional {
      ?graph_uri graph_label: ?graph
    }
    optional {
        ?type_uri label: ?type
    }
  }
}"""

sparql.setQuery(query)
sparql_data = sparql.queryAndConvert()

uri_query = """
prefix label: <http://www.w3.org/2000/01/rdf-schema#label>
prefix type: <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>
select distinct ?uri ?graph
where {
  graph ?graph{
    ?uri label: ?label .
    ?uri type: ?type_uri
  }
}
"""

LABEL = URIRef("http://www.w3.org/2000/01/rdf-schema#label")
TYPE = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
GRAPH_LABEL = URIRef("http://mediagraph.link/jvmg/ont/shortLabel")

uris = list(sparql_data.query(uri_query))

def gen_docs():
    for uri, graph in uris:
        labels = [str(item) for item in sparql_data.objects(subject=uri, predicate=LABEL)]
        types = [str(item) for item in sparql_data.objects(subject=uri, predicate=TYPE)]
        graph_labels = [str(item) for item in sparql_data.objects(subject=graph, predicate=GRAPH_LABEL)]
        doc = {
            "uri": str(uri),
            "label": labels,
            "type": types,
            "graph": graph_labels
        }
        yield {"_index": index_name, "_source": doc}

bulk(es, gen_docs())
