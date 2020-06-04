# Data Pipelines with Airflow

### Summary

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines with Apache Airflow. The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

We need to build high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Data Quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

### Project Description

Build a Data Pipeline with Apache Airflow using custom subdags & operators  to perform tasks such as staging the data from S3 to Redshift, fill the data warehouse (fact & dimension tables), and run data quality checks.

### Pre-Requisites
* AWS Redshift Cluster, KeyPair (Read Amazon S3)
* Apache Airflow

### How to Run & Test
* Start Airflow Webserver & Scheduler and add 'aws-credentials' & 'redshift' connection parameters
* Enable 'dend_dpl_dag' and Run ETL Pipeline. 
* Review GraphView for Data Flow and Monitor TreeView for Execution Results.
* Review logs of tasks from 'Run_data_quality_checks' subdag.

### Files
| Filename |  |
| ------ | ------ |
| dags/dend_dpl_dag.py | Primary  DAG to create subdags that Stages dataset from S3 to Redshift, Creates Fact & Dimension tables, Runs data quality checks. | 
| dags/stage_redshift_subdag.py | SubDAG that creates staging tables and copies data from S3 to Redshift. | 
| dags/load_fact_subdag.py | SubDAG that creates Fact table and inserts data from staging table. | 
| dags/load_dim_subdag.py | SubDAG that creates Dimension tables and inserts data from staging table. | 
| dags/data_quality_subdag.py | SubDAG that runs data quality checks. | 
| plugins/operators/stage_redshift.py | Operator that copies data from S3 to Redshift. | 
| plugins/operators/load_fact.py | Operator that inserts data to Fact table from staging tables. | 
| plugins/operators/load_dimension.py | Operator that inserts data to Dimension tables from staging tables. | 
| plugins/operators/data_quality.py | Operator that runs data quality checks on a specific table. | 
| plugins/helpers/sql_tables.py | Redshift queries to create & drop tables. | 
| plugins/helpers/sql_queries.py | Redshift queries to copy & insert data into the tables, etc. | 

### Table Design
##### Staging Tables  
- staging_events_table
- staging_songs_table

##### Dimension Tables
- songs
- artists
- users
- times

##### Facts Table
- songplays

##### DISTKEY, SORTKEY
- artist_id (or artist name) used for distribution
- song_id, artist_id, user_id, timestamp, songplay_id used for sorting

```sh
# store raw data from log_data JSON files on S3 onto staging_events_table 
CREATE TABLE IF NOT EXISTS staging_events_table (
    "artist" varchar,
    "auth" varchar, 
    "firstname" varchar, 
    "gender" varchar, 
    "iteminsession" int,
    "lastname" varchar, 
    "length" real, 
    "level" varchar, 
    "location" varchar, 
    "method" varchar, 
    "page" varchar, 
    "registration" bigint, 
    "sessionid" bigint, 
    "song" varchar, 
    "status" int, 
    "ts" bigint, 
    "useragent" varchar(512), 
    "userid" bigint
) DISTKEY(artist)

# store raw data from song_data JSON files on S3 onto staging_songs_table 
CREATE TABLE IF NOT EXISTS "staging_songs_table" (
    "song_id" varchar,
    "title" varchar(512), 
    "num_songs" int, 
    "year" int, 
    "duration" real,
    "artist_id" varchar, 
    "artist_name" varchar(512), 
    "artist_location" varchar(512), 
    "artist_latitude" real, 
    "artist_longitude" real
) DISTKEY(artist_id) SORTKEY(song_id)

# records in log data associated with song plays i.e. records with page 'NextSong'
# ensure that a user is using a single session at any particular time
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int IDENTITY(1,1) PRIMARY KEY, 
    start_time bigint NOT NULL, 
    user_id bigint NOT NULL, 
    song_id varchar, 
    artist_id varchar, 
    session_id bigint NOT NULL, 
    location varchar, 
    user_agent varchar(512),
    UNIQUE (start_time, user_id, session_id)
) DISTKEY(artist_id) SORTKEY(songplay_id)

# users in the app
CREATE TABLE IF NOT EXISTS users (
    user_id bigint PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
) SORTKEY(user_id)

# songs in music database
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar(512), 
    artist_id varchar, 
    year int, 
    duration real
) DISTKEY(artist_id) SORTKEY(song_id)

# artists in music database
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar(512), 
    location varchar(512), 
    latitude real, 
    longitude real
) SORTKEY(artist_id)

# timestamps of records in songplays broken down into specific units
CREATE TABLE IF NOT EXISTS times (
    start_time bigint PRIMARY KEY, 
    dt DATE NOT NULL, 
    hour smallint NOT NULL, 
    day smallint NOT NULL, 
    week smallint NOT NULL, 
    month smallint NOT NULL, 
    year smallint NOT NULL, 
    weekday boolean NOT NULL
) SORTKEY(start_time)
```

### Acknowledgement
Author: Hari Raja
Framework: Udacity
Date: Jun 3 2020
