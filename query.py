import rdflib
from rdflib import Graph, URIRef, BNode, Literal, Namespace
from rdflib.namespace import RDF, RDFS, OWL, XSD, FOAF, DCTERMS, SDO, SKOS

g = Graph()
g.parse('pressure.ttl', format='ttl')

qres = g.query('''
SELECT ?resultTime ?hasSimpleResult
WHERE {
    ?a sosa:resultTime ?resultTime.
    ?a sosa:hasSimpleResult ?hasSimpleResult.
}
ORDER BY ?resultTime
''')
for label in qres:
    print("%s | %s" % label)