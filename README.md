# DataSF Profiler

## Aspects of Data Quality -> Open data programs should monitor the following data quality metrics on their open data portals:
* Completeness
* Validity
* Timeliness
* Uniqueness
* Consistency
* Accuracy


## Why do data profiling?
* Data profiling helps us make an assessment of data quality
* It assists in the discovery of anomalies within the data
* It helps users understand content, structure, relationship, etc about the data we are working with
* It surfaces issues/problems that we might run into before we start working with the data
* It helps us assess and validate metadata


## Types of Data Profiling Analysis That DataSF Profiler performs
Data can be profiled both the dataset level and at the column level. Below outlines the type of analysis that can be conducted.

### Dataset Level Analysis
* Total number of records- the total number of records in a dataset
* Total Number of Fields - the total number of fields in a dataset
* Field Type Counts -  for each field type the number of fields that are of that type
* Number of Duplicates - The number of row level duplicates- aka rows that are exact copies
* Percentage of Duplicates in the Dataset - Indicates what percentage of the dataset is row level duplicates
* Global Fields - the number of global fields in a dataset
* Global Fields Percentage - Shows the percentage that global fields make up the dataset; A dataset with a high percetnge of global fields may indicate that its a reference dataset
* The day the dataset was last updated - shows how recently the dataset was updated
* The number of days since the dataset was last updated- a dataset with a high number of days suggest that the dataset is stale
* Other metadata information that can be used for more indepth analysis


### Field Level Analysis
* Total Count - Count of the number of records given field
* Null Count - Count of the number of records with a NULL value
* Missing Count - Count of the number of records with a missing value (i.e. non-NULL absence of data e.g. character spaces)
* Actual Count - Count of the number of records with an actual value (i.e. non-NULL and non-missing)
* Cardinality - Count of the number of distinct values in a field
* Completeness - Percentage calculated as Actual divided by the total number of records
* Distinctness - Percentage calculated as Cardinality divided by the total number of records
* Uniqueness - Percentage calculated as Cardinality divided by Actual
* IsPrimaryKeyCandidate - Looks to see if a column is a 100% unique and 100% complete; if both are true, the column is a good candidate to become a primary key
* Min Field Length - the min number of characters/digits in a field
* Min Field Length - the max number of characters/digits in a field
* Avg Field Lenghth - the average number of characters/digits in a field
* Min Value - The mininum value found in a field
* Max Value - The maximum value found in a field
* Mean - The average value found in a field
* Median - The middle value found in a field
* Mode - The most frequently occuring value in a field
* Range - The value between the min and the max
* Sum - The sum of all the values in the field
* Many other summary stats such as standard deviation, variance, quartiles, iqr, kurtosis, and skewness

### How this works + tech specs
This repo builds on other work that Datasf has done around metadata.
It uses the collect metadata to run a bunch of queries to profile the data.
Support is built into the tool to kill processes and queries that take too long....
Can be configured to send email notifications when various tasks are done...

### Related Repos
* [the metadata management tool ] (https://github.com/DataSF/metadata-mgmt-tool)
* [fetch-metadata-fields](https://github.com/DataSF/fetch-metadata-fields)
