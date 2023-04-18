from pyspark import SparkConf, SparkContext

# Functions to parse lines in edges and nodes files
def parse_edge(edge):
    parts = edge.split("\t")
    return parts[0], parts[1]

def parse_node(node):
    parts = node.split("\t")
    return parts[0], parts[1]

# Set up Spark configuration and context
conf = SparkConf().setAppName("HetIONet_Q3")
sc = SparkContext(conf=conf)

# Load and parse edges and nodes data
edges = sc.textFile("data/edges.tsv").map(parse_edge)
nodes = sc.textFile("data/nodes.tsv").map(parse_node)

# Filter edges to get drug-gene relationships
drug_gene = edges.filter(lambda x: x[1].startswith("G"))

# Compute gene counts for each drug
drug_gene_count = drug_gene.countByKey()

# Sort drugs by gene count in descending order
drug_gene_count_sorted = sorted(drug_gene_count.items(), key=lambda x: x[1], reverse=True)

# Get the top 5 drugs by gene count
top_5 = drug_gene_count_sorted[:5]

# Load drug names from nodes data
drug_names = dict(nodes.collect())

# Print the top 5 drugs with their gene counts
for drug, gene_count in top_5:
    print(f"{drug_names[drug]} -> {gene_count}")

# Stop the Spark context
sc.stop()
