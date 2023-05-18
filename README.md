# Airflow

Apache Airflow is an open-source platform for developing, scheduling, and monitoring batch-oriented workflows. Airflow's extensible Python framework enables you to build workflows connecting with virtually any technology. A web interface helps manage the state of your workflows.

___________________________________________________________
steps:

 Apache Airflow-based data pipeline that automates the extraction, transformation, and loading of employee salary data. With seamless integration between PostgreSQL and Snowflake, this project ensures accurate and up-to-date salary information. 


 Extraction: PostgresOperator retrieves salary data from PostgreSQL on AWS.
 
 Staging: S3UploadOperator securely stages data in Amazon S3.
 
 Transformation: PythonOperator applies custom transformations using Pandas.
 
- Update and Insert: SnowflakeOperator updates existing records and inserts new ones. (DWH)

1- join tables (finance - hr)
2- to get new rows make left join  with target in snowflack     DWH
3- updated rows check the salary to make updates in DWH 
4- make branching to specify if the table in empty make only insert 
and if it not empty make update then insert 


![air2](https://github.com/sara443/Airflow-Data-Pipeline/assets/63977435/d8edfb8d-0996-4ab6-872d-63b347e4f846)







