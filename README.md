# ETL Project
**Simple ETL project in CLI that inserts data from a CSV into a single, flat table.**

Number of rows in my table: 29889

A few sentences what I would change if I knew it would be used for a 10GB CSV input file:
For handling large files:  
    -Stream Processing: Process the CSV file in chunks rather than loading everything into memory.  
    -Parallel Processing: Consider parallelizing the bulk insert or CSV processing.  
    -Indexes: Ensure proper indexing in the SQL database, particularly on PULocationID and DOLocationID, for fast query results.  

CSV files are in "data-csv" folder.  
SQL scripts for DB are in file "SQLScripts.sql"
