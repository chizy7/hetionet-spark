from pyspark import SparkConf, SparkContext

def parse_edge(edge):
    parts = edge.split("\t")
    return parts[0], parts[1]

def parse_node(node):
    parts = node.split("\t")
    return parts[0], parts[1]

conf = SparkConf().setAppName("HetIONet_Q1")
sc = SparkContext(conf=conf)

edges = sc.textFile("data/edges.tsv").map(parse_edge)
nodes = sc.textFile("data/nodes.tsv").map(parse_node)

drug_gene = edges.filter(lambda x: x[1].startswith("G"))
drug_disease = edges.filter(lambda x: x[1].startswith("Dis"))

drug_gene_count = drug_gene.countByKey()
drug_disease_count = drug_disease.countByKey()

drug_gene_disease = {
    k: (drug_gene_count.get(k, 0), drug_disease_count.get(k, 0))
    for k in set(drug_gene_count) | set(drug_disease_count)
}

drug_gene_disease_sorted = sorted(
    drug_gene_disease.items(), key=lambda x: x[1][0], reverse=True
)

top_5 = drug_gene_disease_sorted[:5]
drug_names = dict(nodes.collect())

for drug, (gene_count, disease_count) in top_5:
    print(f"{drug_names[drug]}\t{gene_count}\t{disease_count}")

sc.stop()
