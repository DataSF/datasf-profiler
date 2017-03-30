# DataSF Profiler

## Why Do Data Profiling?
* Data profiling helps us make an assessment of data quality
* It assists in the discovery of anomalies within the data
* It helps users understand content, structure, relationship, etc about the data we are working with
* It surfaces issues/problems that we might run into before we start working with the data
* It helps us assess and validate metadata

## Using this tool, DataSF can monitor the following data quality metrics on their open data portal:
* Completeness
* Validity
* Timeliness
* Uniqueness
* Consistency
* Accuracy

## Types of Data Profiling Analysis That DataSF Profiler performs
Data can be profiled both the dataset level and at the field level. Below outlines the types of analysis that the profiling tool conducts

### Dataset Level Analysis
* Total number of records- The total number of records in a dataset
* Total Number of Fields - The total number of fields in a dataset
* Field Type Counts -  For each field type the number of fields that are of that type
* Number of Duplicates - The number of row level duplicates- aka rows that are exact copies
* Percentage of Duplicates in the Dataset - Indicates what percentage of the dataset is compromised of level duplicates
* Documented Count and Percentage - Count/Percentage of the number of fields that have been documented and have completed field definitions
* Global Field Count - The number of global fields in a dataset
* Global Field Percentage - Shows the percentage that global fields make up the dataset; A dataset with a high percetnge of global fields may indicate that its a reference dataset
* The day the dataset was last updated - Shows how recently the dataset was updated
* Days Since Last Updated - The number of days between today and the date when the dataset was last updated; Can be used to indetify stale datasets
* Days Since First Created - The number of days between today and the date when the dataset was first created
* Plus other metadata information that can be used for more indepth analysis/analytics....


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
* Min Field Length - The minimum number of characters/digits in a field
* Min Field Length - The maximum number of characters/digits in a field
* Avg Field Lenghth - The average number of characters/digits in a field
* Min Value - The minimum value found in a field
* Max Value - The maximum value found in a field
* Mean - The average value found in a field
* Median - The middle value found in a field
* Mode - The most frequently occurring value in a field
* Range - The value between the min and the max
* Sum - The sum of all the values in the field
* Standard Deviation - A measure of how spread out numbers in the field are; a concrete measure of the exact distances from the mean
* Variance - A measure that gives a very general idea of the spread the values in a field. A value of zero means that there is no variation in values; All the numbers in the field set are the same
* Mean Absolute Deviation - Another measure that helps you get a sense of how spread out the values in a field are; Tells you how far, on average, all values in the field are from the middle
* 5th, 25th 50th, 75th, 95th Percentiles- A measure that indicates the value below which X% of values in the field fall
* IQR - The difference between the first quartile and third quartile of the values in a field. This another way to describe the spread of values within a field
* Kurtosis - A measure to describe the distribution, or skewness, of observed values of the field around the mean
* Skewness - A measure of the degree of asymmetry of the distribution of values in a field. If values in the lower tail of the field are more pronounced than the values in the larger tail of the field, the field will have negative skewness. If the reverse is true, the field will have positive skewness. If the two are equal, the field has zero skewness
* Days Since Last Updated - The number of days between today and the date when the data in the field was last updated

### How this works + tech specs
* This repo builds on other work that Datasf has done around metadata.
* It uses various metadata to run a bunch of queries that profile the data.
* Most of the heavy lifting is done by the portal's API, not in memory for quicker run time
* Support is built into the tool to kill processes and queries that take too long
* Timestamps identify when field data is refreshed so that fields are profiled when the data is updated
* This tool can be configured to send email notifications when various tasks are done

### Related Repos
* [the metadata management tool ](https://github.com/DataSF/metadata-mgmt-tool)
* [fetch-metadata-fields](https://github.com/DataSF/fetch-metadata-fields)
