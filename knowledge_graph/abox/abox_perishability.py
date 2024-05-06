import pandas as pd
from rdflib import Graph, URIRef, Literal, Namespace, RDF, XSD
from urllib.parse import quote

# Define namespaces
pub = Namespace("http://www.example.edu/spicybytes/")
xsd = Namespace("http://www.w3.org/2001/XMLSchema#")

# Create an RDF graph
g = Graph()



def expected_avg_expiry():
    expiry_df = pd.read_parquet('./data/formatted_zone/estimated_avg_expiry')
    expiry_df['product_name_quoted'] = expiry_df['product_name'].str.lower().apply(quote)

    for index, row in expiry_df.iterrows():
        subject = URIRef(pub + 'perishability/'+str(row['product_name_quoted']))

        product_literal = Literal(row['product_name'], datatype = XSD.string)
        category_literal = Literal(row['category'], datatype = XSD.string)
        subcategory_literal = Literal(row['sub_category'], datatype = XSD.string)
        avg_expiry_days_literal = Literal(row['avg_expiry_days'], datatype = XSD.integer)
        
        # Add triples to the RDF graph
        # g.add((subject, predicate, obj))
        g.add((subject, pub.product_name, product_literal))
        g.add((subject, pub.category, category_literal))
        g.add((subject, pub.subcategory, subcategory_literal))
        g.add((subject, pub.avg_expiry_days, avg_expiry_days_literal))

    return g



if __name__ == "__main__":
    expected_avg_expiry = expected_avg_expiry()

    turtle = expected_avg_expiry.serialize(format='turtle')
    expected_avg_expiry.serialize(destination='./knowledge_graph/output/kg_abox_perishability.rdf', format='xml')
