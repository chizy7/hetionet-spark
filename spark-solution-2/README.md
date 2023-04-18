# HetIONet Analysis using Apache Spark

This project analyzes HetIONet data to answer the following questions:
1. For each drug, compute the number of genes and the number of diseases associated with the drug. Output results with the top 5 number of genes in a descending order.
2. Compute the number of diseases associated with 1, 2, 3, ..., n drugs. Output results with the top 5 number of diseases in a descending order.
3. Get the name of drugs that have the top 5 number of genes. Output the results.

## Requirements
* Python 3.6+
* Apache Spark 3.1.2+

## How to Run
1. Install Apache Spark on your system and set up the necessary environment variables.
2. Clone the repository and navigate to the `hetionet_project` directory.
3. Place the HetIONet data files `nodes.tsv` and `edges.tsv` inside the `data` folder.
4. Execute the following command to run the analysis using `spark-submit`:
```
spark-submit src/hetionet_analysis.py
```

The output of the analysis will be printed in the console.

## Analysis
The provided solution uses Apache Spark to analyze HetIONet data. The solution is designed with the Reduce-Side pattern in mind. It reads `edges.tsv` and `nodes.tsv`, filters the data based on genes and diseases, and groups them by drug to compute the required statistics. The results are then sorted and the top 5 records are printed for each question.