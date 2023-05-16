# Airflow
Dag reads from DB  and write in s3 (staging) and then join  with target in snowflack (DWH)
RDS__s3__snowflack 
Apache Airflow is an open-source platform for developing, scheduling, and monitoring batch-oriented workflows. Airflow's extensible Python framework enables you to build workflows connecting with virtually any technology. A web interface helps manage the state of your workflows.
components of pipelines:
1- source system __AWS(RDS)
2-postgres (transactional data base) s3 ,have two tables
-finance -hr
3-Snowflack (DWH) the target
___________________________________________________________
steps:
1- take all data to data lack (EL) into s3 (staging) 
2- ectracted the data and then made transformations 
1- join tables (finance - hr)
2- to get new rows make left join  with target in snowflack dwh 
3- updated rows check the salary to make updates in DWH 
4- make branching to specify if the table in empty make only insert 
and if it not empty make update then insert 









