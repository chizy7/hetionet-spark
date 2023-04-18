from pyspark import SparkConf, SparkContext

# Functions to parse lines in edges and nodes files
def parse_edge(line):
    parts = line.split("\t")
    return parts[0], parts[1]

def parse_node(line):
    parts = line.split("\t")
    return parts[0], parts[1]

# Functions to check if an edge is related to a gene or a disease
def is_gene(edge):
    return edge[1].startswith("G")

def is_disease(edge):
    return edge[1].startswith("Dis")

# Set up Spark configuration and context
conf = SparkConf().setAppName("HetIONet Analysis")
sc = SparkContext(conf=conf)

# Load and parse edges and nodes data
edges = sc.textFile("data/edges.tsv").map(parse_edge)
nodes = sc.textFile("data/nodes.tsv").map(parse_node)

# Q1: Compute gene and disease counts for each drug
genes_by_drug = edges.filter(is_gene).groupByKey().mapValues(len)
diseases_by_drug = edges.filter(is_disease).groupByKey().mapValues(len)

# Join results and sort by gene count
q1 = genes_by_drug.join(diseases_by_drug).join(nodes).map(lambda x: (x[1][1], x[1][0]))
q1_sorted = q1.sortBy(lambda x: x[1][0], ascending=False).take(5)
print("Q1 Results: ", q1_sorted)

# Q2: Compute disease counts for 1, 2, 3, ..., n drugs
diseases_by_drug_count = diseases_by_drug.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
q2_sorted = diseases_by_drug_count.sortBy(lambda x: x[1], ascending=False).take(5)
print("Q2 Results: ", q2_sorted)

# Q3: Get drug names with the top 5 gene counts
q3 = genes_by_drug.join(nodes).map(lambda x: (x[1][1], x[1][0]))
q3_sorted = q3.sortBy(lambda x: x[1], ascending=False).take(5)
print("Q3 Results: ", q3_sorted)

# Stop the Spark context
sc.stop()
