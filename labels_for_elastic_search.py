import elasticsearch
from SPARQLWrapper import SPARQLWrapper, JSONLD

sparql = SPARQLWrapper("http://localhost:3030/public")
sparql.setReturnFormat(JSONLD)


es = elasticsearch.Elasticsearch()
es.indices.delete(index="jvmg_search", ignore=[400, 404])
settings = {
    "settings": {
        "number_of_shards": 6,
        "number_of_replicas": 0
    },
    'mappings':
    {'properties':
     {'object': {'type': 'wildcard'},
      'predicate': {'type': 'wildcard'},
      'subject': {'type': 'wildcard'}}}}

es.indices.create(index="jvmg_search", ignore=400, body=settings)

step_size = 20_000

for i in range(0, 60_000_000, step_size):
    print(i)
    query = f"""
    CONSTRUCT {{?s ?p ?o}}
    WHERE {{
    ?s ?p ?o . filter isLiteral(?o)
    }} LIMIT {step_size} OFFSET {i}"""

    sparql.setQuery(query)
    sparql_data = sparql.query().convert()
    if len(sparql_data) == 0:
        break

    bulk_data = []
    for id, item in enumerate(sparql_data):
        bulk_data.append({"index": {"_id": str(i+id),
                                    "_index": "jvmg_search"}})
        bulk_data.append({"subject": str(item[0]),
                          "predicate": str(item[1]),
                          "object": str(item[2])})

    res = es.bulk(body=bulk_data)
    if res["errors"]:
        print(res)
