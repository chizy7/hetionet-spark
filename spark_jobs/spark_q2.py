from pyspark import SparkConf, SparkContext
from collections import defaultdict

def parse_edge(edge):
    parts = edge.split("\t")
    return parts[0], parts[1]

conf = SparkConf().setAppName("HetIONet_Q2")
sc = SparkContext(conf=conf)

edges = sc.textFile("data/edges.tsv").map(parse_edge)
drug_disease = edges.filter(lambda x: x[1].startswith("Dis"))

disease_drug_count = drug_disease.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)
drug_count_disease = defaultdict(list)

for disease, count in disease_drug_count.collect():
            drug_count_disease[count].append(disease)

sorted_counts = sorted(drug_count_disease.keys(), reverse=True)

for count in sorted_counts[:5]:
    diseases = drug_count_disease[count]
    print(f"{count} drugs -> {len(diseases)} diseases")

sc.stop()
