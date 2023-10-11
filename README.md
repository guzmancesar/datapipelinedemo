- main.py needs to be modularized 
- subdirectories to be added
- see below for steps remaining

##Steps 


Requirements:
  - Homebrew ()
  - python (brew install python3)
  - pyspark (pip3 install python)
  - Java (brew install java)
  - AWS S3
  - Databricks

Steps 
1. Use pyspark to transform data before it hits storage (preprocessing)
2. create s3 bucket for data
3. assign permissions to an IAM user for the script to RWD s3 objects 
4. place data in s3 bucket <- we are here as MVP
5. dockerize script
6. switch out csv for a dynamic dataset
7. cron job
9. add unit tests
10. Demonstrate ETL capabilities (TBD)
11. Dashboarding (TBD)

