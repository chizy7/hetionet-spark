from pyspark import SparkConf, SparkContext
from collections import defaultdict

# Function to parse lines in edges file
def parse_edge(edge):
    parts = edge.split("\t")
    return parts[0], parts[1]

# Set up Spark configuration and context
conf = SparkConf().setAppName("HetIONet_Q2")
sc = SparkContext(conf=conf)

# Load and parse edges data
edges = sc.textFile("data/edges.tsv").map(parse_edge)

# Filter edges to get drug-disease relationships
drug_disease = edges.filter(lambda x: x[1].startswith("Dis"))

# Compute the count of drugs associated with each disease
disease_drug_count = drug_disease.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)

# Create a dictionary to store diseases associated with the same drug count
drug_count_disease = defaultdict(list)

# Populate the dictionary with diseases and their associated drug counts
for disease, count in disease_drug_count.collect():
    drug_count_disease[count].append(disease)

# Sort drug counts in descending order
sorted_counts = sorted(drug_count_disease.keys(), reverse=True)

# Print the top 5 drug counts with their associated disease counts
for count in sorted_counts[:5]:
    diseases = drug_count_disease[count]
    print(f"{count} drugs -> {len(diseases)} diseases")

# Stop the Spark context
sc.stop()
