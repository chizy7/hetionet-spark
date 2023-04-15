from pyspark import SparkConf, SparkContext

def parse_edge(edge):
    parts = edge.split("\t")
    return parts[0], parts[1]

def parse_node(node):
    parts = node.split("\t")
    return parts[0], parts[1]

conf = SparkConf().setAppName("HetIONet_Q3")
sc = SparkContext(conf=conf)

edges = sc.textFile("data/edges.tsv").map(parse_edge)
nodes = sc.textFile("data/nodes.tsv").map(parse_node)

drug_gene = edges.filter(lambda x: x[1].startswith("G"))

drug_gene_count = drug_gene.countByKey()
drug_gene_count_sorted = sorted(drug_gene_count.items(), key=lambda x: x[1], reverse=True)

top_5 = drug_gene_count_sorted[:5]
drug_names = dict(nodes.collect())

for drug, gene_count in top_5:
    print(f"{drug_names[drug]} -> {gene_count}")

sc.stop()
