import argparse
import elasticsearch
from elasticsearch.helpers import bulk
from SPARQLWrapper import SPARQLWrapper, JSONLD
from rdflib.term import URIRef
from tqdm import tqdm

parser = argparse.ArgumentParser(description="Indexes all uris which have a rdf#type and rdfs#label.")

parser.add_argument("--sparql", default="http://localhost:3030/jvmg/sparql", help="Address to sparql endpoint (default: http://localhost:3030/jvmg/sparql)")
parser.add_argument("--elasticsearch", default="http://localhost:9200", help="Address to elasticsearch (default: http://localhost:9200)")
parser.add_argument("--index", required=True, help="Index name for elasticsearch.")
args = parser.parse_args()

sparql = SPARQLWrapper(args.sparql)
sparql.setReturnFormat(JSONLD)

es = elasticsearch.Elasticsearch(args.elasticsearch)
es.options(ignore_status=[400, 404]).indices.delete(index=args.index)

mappings = {
    'properties': {
        'uri':   {'type': 'text'},
        'label': {'type': 'text'},
        'graph': {'type': 'keyword'},
        'type':  {'type': 'keyword'}
    }
}

es.options(ignore_status=400).indices.create(index=args.index, mappings=mappings)


query = """
    CONSTRUCT {
       ?uri <http://www.w3.org/2000/01/rdf-schema#label> ?label .
       ?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type .
       ?type <http://www.w3.org/2000/01/rdf-schema#label> ?type_label .
       ?uri <http://mediagraph.link/jvmg/ont/hasGraph> ?graph .
       ?graph <http://mediagraph.link/jvmg/ont/shortLabel> ?graph_label
    } WHERE {
    graph ?graph{
      ?uri <http://www.w3.org/2000/01/rdf-schema#label> ?label .
    }
    ?uri <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?type .
    OPTIONAL {
      ?type <http://www.w3.org/2000/01/rdf-schema#label> ?type_label .
    }
    ?graph <http://mediagraph.link/jvmg/ont/shortLabel> ?graph_label
    }"""

print("executing sparql query...")
sparql.setQuery(query)
sparql_data = sparql.query().convert()

LABEL = URIRef("http://www.w3.org/2000/01/rdf-schema#label")
TYPE = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
GRAPH = URIRef("http://mediagraph.link/jvmg/ont/hasGraph")
GRAPH_LABEL = URIRef("http://mediagraph.link/jvmg/ont/shortLabel")

def gen_docs():
    for uri in tqdm(set(sparql_data.subjects(predicate=TYPE))):
        labels  = [str(label) for label in sparql_data.objects(subject=uri, predicate=LABEL)]
        type_labels = []
        graph_labels = []

        for type in set(sparql_data.objects(subject=uri, predicate=TYPE)):
            new_labels = [str(label) for label in sparql_data.objects(subject=type, predicate=LABEL)]
            if len(new_labels):
                type_labels.extend(new_labels)
            else:
                type_labels.append(str(type))

        for graph in set(sparql_data.objects(subject=uri, predicate=GRAPH)):
            new_labels = [str(label) for label in sparql_data.objects(subject=graph, predicate=GRAPH_LABEL)]
            if len(new_labels):
                graph_labels.extend(new_labels)
            else:
                graph_labels.append(str(graph))

        doc = {
            "uri": str(uri),
            "label": labels,
            "type": type_labels,
            "graph": graph_labels,
        }

        yield {"_index": args.index, "_source": doc}

bulk(es, gen_docs())
