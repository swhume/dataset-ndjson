# dataset-ndjson

### Introduction
These programs generate NDJSON file from existing example Dataset-JSON datasets to create the dataset-ndjson datasets.

The purpose of dataset-ndjson, or new-line delimited JSON, is to simplify streaming large datasets so that the dataset
can easily be read or written one row at a time. Most languages have libraries that can read a large
JSON dataset as a stream, but in cases where such a library is not available the ndjson format makes
it easy for the program read and write a row at a time.

The programs are examples to demonstrate a proof-of-concept dataset-ndjson format. The 
json2ndjson.py program retrieves a set of the Dataset-JSON SDTM example datasets from the
cdisc-org/DataExchange-DatasetJson GitHub repository and converts them to an ndjson format.
The ndjson2csv.py program converts the example dataset-ndjson files to csv files as a
simple test.

### The dataset-ndjson format

The dataset-ndjson format is created from the Dataset-JSON standard by:
* Row 1. Create 1 JSON object from the ODM attributes
* Row 2. Create 1 JSON object from the dataset attributes (everything after the ODM attributes and before the variable metadata)
* Row 3. Create 1 JSON object that contains an array of variable metadata definitions
* Row 4 - n. Create 1 array per data row for each itemData array

### Examples

Several of the Dataset-JSON example datasets have been converted into Dataset-ndjson for review. 
They are available in the data folder and have the .ndjson extension.


