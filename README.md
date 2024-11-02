
# Spark-Based Document Search with Bloom Filter

## Description

This project implements a Spark application that efficiently checks whether a collection of documents contains a specific word using a Bloom Filter. The application processes multiple documents, converting them into a list of words stored as Resilient Distributed Datasets (RDDs).

## Key Features

### Word Mapping
- Reads a set of documents and maps their content into a list of words as RDDs.

### Bloom Filter Implementation
- Utilizes the `breeze.util` package to create a Bloom Filter, adding the words from the RDD to the filter for fast membership checking.

### Empirical Error Rate Calculation
- Compares the results from the Bloom Filter with the actual content of the RDDs to calculate the empirical error rate, providing insights into the filter's accuracy.

## Conclusion
This project demonstrates the integration of Spark and Bloom Filters for efficient searching, showcasing the trade-offs between speed and accuracy in large-scale data processing.
