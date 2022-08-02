## Summary

* [Objective] 
* [System] 
* [Schema] 
* [ETL Pipeline] 
* [How to Test Queries] 
* [Project files] 

==============================================================================

#### Objective
Assist Sparkify to move databases onto the cloud by building ETL pipeline that extract data from AWS S3, stages them in Redshift and transform into a set of dimensional tables for their analytics team to use. 


#### System
This project is using AMAZON S3 as a data storage for the Sparkify's songs & event data. <br>
AMAZON REDSHIFT is used for DATA WAREHOUSE with 'columnar storage'. <br>

``S3``: is where data gets extracted <br>
``REDSHIFT``: is where the data is ingested and transformed using ``postgresql``.


#### Schema
- Staging Table 
1. song (song_id , num_songs, artist_id, artist_lattitude, artist_longitude, artist_location, artist_name, title, duration, year) <br>
2. event (event_id, artist, auth, first_name, gender, item_in_session, last_name, length, level, location, method, page, registration, session_id, song, status, ts, user_agent, user_id)

- Fact Table : <br> songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
- Dimension Tables: 
1. users (user_id, first_name, last_name, gender, level)
2. songs (song_id, title, artist_id, year, duration)
3. artists (artist_id, name, location, lattitude, longitude)
4. time ( start_time, hour, day, week, month, year, weekday)

![Star_schema.png](attachment:Star_schema.png)
                          

#### ETL Pipeline <br>
In this project, python is used as connecting language and PostgreSQL is main language used. <br>
- Data Transformation & normalization is done through query (sql_queries.py)
- Creating DATABASE through (create_tables.py)
- ETL process through (etl.py)
- configuration set_up through (dwh.cfg)



```python

```
