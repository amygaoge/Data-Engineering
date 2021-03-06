# Sparkify AWS Cloud Data Warehouse and ETL

## Context
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They'd like a data engineer to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional analytics tables for their team to continue finding insights in what songs their users are listening to.


## ETL Pipeline
This project uses Cloud Data Warehouse Redshift to give the Sparkify analytics team a flexible architecture that can scale in seconds to meet changing storage demands. Redshift also achieves high performance when handling analytic workloads on big data sets. Using its Massively Parallel Processing (MPP) architecture, Redshift can parallelize data loading, backup, and restore operations on multiple machines.

There are two ETL processes. The first ETL extracts the data from S3 and stages them in Redshift raw/staging tables. The second ETL is a sql to sql ETL which transform data from raw table into a set of dimensional/analytics tables.


## Database Schema and Optimizing Table Design
Star Schema is used on analytics tables to give users the ability to perform simple query with less joins and fast aggregations. The Fact Table records in log data associated with song plays. Using this table, the company can relate and analyze four dimensions users, songs, artists and time.

Using different Distribution strategy and Sorting Keys, analytics tables are optimized for fast execution of complex analytic queries against large data sets.

* Fact Table: songplays (KEY distribution + SORTKEY)
* Dimension Tables: users (ALL distribution + SORTKEY), songs (KEY distribution + SORTKEY), artists (ALL distribution + SORTKEY) and time (ALL distribution + SORTKEY).


## Project file structure
* **[create_AWS_cluster.ipynb]**: Python notebook to create a new AWS cluster
* **[create_tables.py]**: Python script to perform SQL-Statements for (re-)creating database and tables
* **[delete_AWS_cluster.ipynb]**: Python notebook to delete the AWS cluster
* **[dwh.cfg]**: Config file which contains AWS, Redshift and S3 credentials
* **[sql_queries.py]**: Python script containing SQL-Statements used by create_tables.py and etl.py
* **[etl.py]**: Python script to extracts raw data from S3, stages them in Redshift staging tables, and transforms data into a set of dimensional analytics tables


## Running the ETL

First create the Redshift database tables by doing:

```
python create_tables.py
```

Then extract raw dataset from S3, stage the data in Redshift raw tables & transform data into the analytics tables:

```
python etl.py
```


### Dataset
The Song dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song.

The Log dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations.

- **Song datasets**: all json files are nested in subdirectories under */data/song_data*. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: all json files are nested in subdirectories under */data/log_data*. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```