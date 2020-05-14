import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                        artist VARCHAR(500) DISTKEY,
                                        auth VARCHAR(100),
                                        firstName VARCHAR(100),
                                        gender VARCHAR(1),
                                        itemInSession INT,
                                        lastName VARCHAR(100),
                                        length DECIMAL,
                                        level VARCHAR(100),
                                        location VARCHAR(100),
                                        method VARCHAR(100),
                                        page VARCHAR(100),
                                        registration DECIMAL,
                                        sessionId INT,
                                        song VARCHAR(500),
                                        status INT,
                                        ts BIGINT,
                                        userAgent VARCHAR(200),
                                        userId int
                                    ) DISTSTYLE KEY
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                        num_songs INTEGER,
                                        artist_id VARCHAR(100),
                                        artist_latitude DECIMAL,
                                        artist_longitude DECIMAL,
                                        artist_location VARCHAR(500),
                                        artist_name VARCHAR(500) DISTKEY,
                                        song_id VARCHAR(50),
                                        title VARCHAR(500),
                                        duration DECIMAL,
                                        year INTEGER
                                    )DISTSTYLE KEY
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                             songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
                             start_time TIMESTAMP not null,
                             user_id INTEGER not null,
                             level VARCHAR(10) not null,
                             song_id VARCHAR(50) DISTKEY,
                             artist_id VARCHAR(100),
                             session_id INTEGER,
                             location VARCHAR(50),
                             user_agent VARCHAR(200) not null
                             )DISTSTYLE KEY
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                            user_id INTEGER PRIMARY KEY,
                            first_name VARCHAR(50) not null,
                            last_name VARCHAR(50) not null,
                            gender VARCHAR(1) not null,
                            level VARCHAR(10) not null
                            )
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                        song_id VARCHAR(50) PRIMARY KEY DISTKEY,
                        title VARCHAR(500) not null,
                        artist_id VARCHAR(100) not null,
                        year INTEGER not null,
                        duration DECIMAL not null
                        )DISTSTYLE KEY
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
                        artist_id VARCHAR(100) PRIMARY KEY,
                        name VARCHAR(500) not null,
                        location VARCHAR(500),
                        latitude DECIMAL,
                        longitude DECIMAL
                        )
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                        start_time TIMESTAMP PRIMARY KEY,
                        hour INTEGER not  null,
                        day INTEGER not null,
                        week INTEGER not null,
                        month INTEGER not null,
                        year INTEGER not null,
                        weekday INTEGER not null
                        )
                        DISTSTYLE ALL
""")

LOG_DATA     = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA    = config.get('S3','SONG_DATA')
ARN          = config.get('IAM_ROLE','ARN')

staging_events_copy = (""" COPY staging_events from {}
                           iam_role {}
                           FORMAT AS JSON {} 
                           REGION 'us-west-2'
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""  COPY staging_songs from {}
                           iam_role {}
                           FORMAT AS JSON 'auto'
                           REGION 'us-west-2'
""").format(SONG_DATA,ARN)

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            (
                                select
                                       distinct '1970-01-01'::date + e.ts/1000 * interval '1 second',
                                       e.userid,
                                       e.level,
                                       s.song_id,
                                       a.artist_id,
                                       e.sessionid,
                                       e.location,
                                       e.useragent
                                from staging_events e
                                left join artists a on e.artist=a.name
                                left join songs s on e.song=s.title and a.artist_id=s.artist_id
                                where page='NextSong'
                            )
""")

user_table_insert = ("""INSERT INTO users(user_id,first_name,last_name,gender,level)
                            (select distinct userid,firstname,lastname,gender,level from
                            (
                                SELECT userid,firstname,lastname,gender,level,
                                row_number() OVER (PARTITION BY userid order by '1970-01-01'::date + ts/1000 * interval '1 second' desc) as row_number
                                FROM staging_events
                                WHERE page='NextSong'
                            ) t
                            where t.row_number=1
                            and userid is not null)
""")

song_table_insert = ("""INSERT INTO songs(song_id,title,artist_id,year,duration)
                            (SELECT distinct song_id, title,artist_id, year,duration FROM staging_songs)
""")

artist_table_insert = ("""INSERT INTO artists(artist_id,name,location,latitude,longitude)
                            (
                                SELECT distinct t.artist_id,t.artist_name,t.artist_location,t.artist_latitude,t.artist_longitude FROM
                                (
                                    SELECT artist_id,artist_name,artist_location,artist_latitude,artist_longitude,
                                    row_number() OVER (PARTITION BY artist_id ORDER BY year DESC) as row_number
                                    FROM staging_songs
                                ) t
                                WHERE row_number=1
);
""")

time_table_insert = ("""INSERT INTO time(start_time,hour,day,week,month,year,weekday)
                            (SELECT 
                            start_time,
                            EXTRACT(HOUR FROM start_time) As hour,
                            EXTRACT(DAY FROM start_time) As day,
                            EXTRACT(WEEK FROM start_time) As week,
                            EXTRACT(MONTH FROM start_time) As month,
                            EXTRACT(YEAR FROM start_time) As year
                            ,EXTRACT(DOW FROM start_time) As weekday
                            FROM (
                                SELECT distinct start_time
                                FROM songplays
                            ))
""")

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert,songplay_table_insert, time_table_insert]
