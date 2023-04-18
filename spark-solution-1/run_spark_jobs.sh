#!/bin/bash

# Run Spark job for Question 1
echo "Running Spark job for Question 1"
spark-submit --master local spark_jobs/spark_q1.py

# Run Spark job for Question 2
echo "Running Spark job for Question 2"
spark-submit --master local spark_jobs/spark_q2.py

# Run Spark job for Question 3
echo "Running Spark job for Question 3"
spark-submit --master local spark_jobs/spark_q3.py
