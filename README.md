# Data2bots Task
This was a data engineering project for a company.

First thing I did was to attempt to extract the data from the s3 bucket and inspect it. I was only able to download 2 csv files, I kept getting a name error for the last one, since the bucket was public I was able to inspect it using my own aws account and figured out that there was a spelling error in the pdf file sent to us, I corrected it and was able to download the last file.
After that I initialized a connection to the postgres database, I created a configuration file which held the necessary credentials for connection, this was read with a parser.Then I used a python driver for postgresql (psycopg2) to complete the connection. A little message shows if the connection is successful. I then created tables in postgres via sql queries, due to limited permissions on the if_common schema, I created tables like the if_common tables in my staging schema amongst other tables. The queries were executed using a cursor object obtain from psycopg2 library. After the tables were created the next thing I did was to load the csv files I extracted earlier into appropriate tables. After this I began to write the transformation queries to extract the information needed. After the required answers were obtained, I created the necessary tables in the analytics schema and exported the csv files to the appropriate s3 bucket.
I would've used apache airflow for the orchestration of the pipeline but I didn't have enough permissions on the schema to use it properly.
N/B: 
1. I only included the configuration file in git so it can be seen by the person assessing the task, I normally wouldn't push config files.
2. For most_ordered_day in the best_performing_product table, after my transformations I had multiple days with same amount of highest ordered, so I just selected the LIMIT 1 result.
3. I wanted to download the data from the s3 bucket directly to my postgres tables using an extension called aws_s3 but I don't have superuser privileges to donwload the extension so I had to download it to my local machine first and then use copy_expert to load it into my tables.
4. I copied the transformed tables to a csv file in my local machine using psql and then accessed the s3 bucket to upload the data in my created folder.
