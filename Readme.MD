### Tasks

#### Stage Operator
? Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.


#### Fact and Dimension Operators
* You can also define a target table that will contain the results of the transformation.
* Load dimensions with a subdag


#### Data Quality Operator
* For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. 
* We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.

