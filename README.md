# hetionet-spark

This project analyzes the HetIONet dataset using Apache Spark. The HetIONet dataset consists of nodes representing drugs and edges representing the relationships between drugs, genes, and diseases.

## Project Structure

```
big-data-project/
│
├── data/
│   ├── edges.tsv
│   └── nodes.tsv
│
├── spark_jobs/
│   ├── spark_q1.py
│   ├── spark_q2.py
│   └── spark_q3.py
│
└── run_spark_jobs.sh
```

* `data/`: This directory contains the input data files, `edges.tsv` and `nodes.tsv`.
* `spark_jobs/`: This directory contains the PySpark scripts for each question (Q1, Q2, and Q3).
* `run_spark_jobs.sh`: This shell script runs the Spark jobs for each question.

## Problem Statement

1. For each drug, compute the number of genes and the number of diseases associated with the drug. Output results with the top 5 number of genes in descending order.
2. Compute the number of diseases associated with 1, 2, 3, ..., n drugs. Output results with the top 5 number of diseases in descending order.
3. Get the name of the drugs that have the top 5 number of genes. Output the results.

## Prerequisites

* Apache Spark 3.x
* Python 3.x
* JDK 8

## Running the Project

1. Ensure that Apache Spark is installed and configured on your system.
2. Download the HetIONet dataset and place the `edges.tsv` and `nodes.tsv` files in the `data/` directory.
3. Open a terminal and navigate to the project root directory.
4. Run the Spark jobs by executing the `run_spark_jobs.sh` script:

```
./run_spark_jobs.sh
```
The script will run the PySpark jobs for each question and display the results in the terminal.

## License
This project is licensed under the MIT License. See the `LICENSE` file for details.