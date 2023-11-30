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

The dataset attributes in row 2 have a flattened structure that will also be included in
a Dataset-JSON that is updated after the pilot. Two examples of this are listed below:
* "datasetType": "clinicalData", 
* "OID": "IG.FA"

#### Row 2 - Dataset Attributes
Row 2 provides a flatter structure for the dataset attributes than the current Dataset-JSON standard, but
we anticipate also flattening this content in Dataset-JSON as part of an update at the conclusion of the pilot.
Flattening the JSON structure makes Dataset-JSON easier to read and write.

Instead of an object named clinicalData or referenceData these values will be captured as an attribtute
with a proposed name of datasetType. The datasetType attribute can have a value of clinicalData
or referenceData. Differentiating between clinicalData and referenceData improves Dataset-JSON's alignment with ODM.

The second case replaces the use of the dataset OID, or ItemGroupDef OID, as the name of an object and makes it an 
attribute. Now the dataset metadata includes an OID attribute that holds a value that must match the OID
used in the Define-XML. 

![NDJSON Proposed Changes](https://github.com/swhume/dataset-ndjson/blob/master/docs/ndjson-example.png?raw=true)

### Examples

The Dataset-JSON example datasets have been converted into Dataset-ndjson for review. 
They are available in the data folder and have the .ndjson extension.

### Limitations

This is just a quick a dirty demonstration of a proposed format for ndjson support in Dataset-JSON. The
programs and outputs have not been well tested. The programs use the json package so this is not suited for large 
datasets, but it works fine for the example datasets.


