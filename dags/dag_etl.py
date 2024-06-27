from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator  # Correct import
from airflow.utils.dates import days_ago
from datetime import datetime
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

local_tz = pendulum.timezone('Asia/Jakarta')

def extract_data():
    # Implement your data extraction logic here
    pass

def transform_data():
    # Implement your data transformation logic here
    pass

with DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL DAG untuk memindahkan data dari PostgreSQL ke BigQuery',
    schedule_interval='0 0 * * *',
    start_date=datetime(2024, 6, 25, tzinfo=local_tz),
    catchup=False,
) as dag:

    # List of tables
    tables = ['categories', 'employees', 'order_details', 'orders', 'products', 'suppliers']
    gcs_bucket = 'project3_bucket1'
    project_id = 'lively-clover-427610'
    dataset_id = 'my_dataset'

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    join_query_task_1 = PostgresOperator(
        task_id='making_datamart_monthly_supplier_gr',
        postgres_conn_id='my_postgres_connection_id',
        sql="""
        CREATE OR REPLACE VIEW monthly_supplier_gr AS
        SELECT
        o."orderID",
        TO_CHAR(TO_DATE(o."orderDate", 'YYYY-MM-DD'), 'Mon-YYYY') AS "SalesMonthYear",
        p."productName",
        s."companyName",
        SUM(od."quantity") as total_quantity_order_supplier,
        ROUND(SUM((od."unitPrice" - (od."unitPrice" * od."discount")) * od."quantity")::numeric ,2) AS monthly_revenue_supplier
        FROM
        order_details od
        JOIN
        orders o ON od."orderID" = o."orderID"
        JOIN 
        products p  ON p."productID"  = od."productID"
        JOIN
        suppliers s  ON p."supplierID" = s."supplierID"
        GROUP BY 
        "SalesMonthYear",
        o."orderID",
        p."productName",
        s."companyName" 
        ORDER BY 
        o."orderID";
        """
    )
    join_query_task_2 = PostgresOperator(
        task_id='making_datamart_monthly_category_sold',
        postgres_conn_id='my_postgres_connection_id',
        sql="""
        CREATE OR REPLACE VIEW monthly_category_sold AS
        SELECT
        c."categoryName",
        TO_CHAR(TO_DATE(o."orderDate", 'YYYY-MM-DD'), 'YYYY-Mon') AS "SalesYear_Month",
        sum(od."quantity") AS monthly_quantity
        from
        order_details od
        join
        orders o on od."orderID" = o."orderID"
        join 
        products p  on p."productID"  = od."productID"
        join 
        categories c on p."categoryID" = c."categoryID"
        group by 
        c."categoryName",
        "SalesYear_Month"
        order by 
        monthly_quantity desc;
        """
    )

    join_query_task_3 = PostgresOperator(
        task_id='making_datamart_best_employee',
        postgres_conn_id='my_postgres_connection_id',
        sql="""
        CREATE OR REPLACE VIEW best_employee AS
        SELECT
        TO_CHAR(TO_DATE(o."orderDate", 'YYYY-MM-DD'), 'YYYY-Mon') AS "SalesYear_Month",
        e."employeeID",
        CONCAT(e."firstName", ' ', e."lastName") AS employee_name,
        ROUND(SUM((od."unitPrice" - (od."unitPrice" * od."discount")) * od."quantity")::numeric ,2) AS monthly_revenue,
        ROW_NUMBER() OVER(PARTITION BY EXTRACT(YEAR FROM TO_DATE(o."orderDate", 'YYYY-MM-DD')), EXTRACT(MONTH FROM TO_DATE(o."orderDate", 'YYYY-MM-DD')) ORDER BY ROUND(SUM((od."unitPrice" - (od."unitPrice" * od."discount")) * od."quantity")::numeric ,2) DESC) as rank
        from
        employees e
        join 
        orders o on e."employeeID"  = o."employeeID"
        join 
        order_details od on o."orderID" = od."orderID"
        group by 
        "SalesYear_Month",
        employee_name,
        e."employeeID" ,
        o."orderID",
        e."firstName",
        e."lastName",
        o."orderDate";
        """
    )

    extract_joined_data_task_1 = PostgresToGCSOperator(
        task_id='load_monthly_supplier_gr_to_gcs',
        postgres_conn_id='my_postgres_connection_id',
        sql='SELECT * FROM monthly_supplier_gr',
        bucket=gcs_bucket,
        filename='data/monthly_supplier_gr/{{ ds }}.json',
        export_format='json',
    )

    load_to_bq_task_1 = GCSToBigQueryOperator(
        task_id='load_monthly_supplier_gr_to_bigquery',
        bucket=gcs_bucket,
        source_objects=['data/monthly_supplier_gr/{{ ds }}.json'],
        destination_project_dataset_table=f'{project_id}.{dataset_id}.monthly_supplier_gr',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
    )

    extract_joined_data_task_2 = PostgresToGCSOperator(
        task_id='load_monthly_category_sold_to_gcs',
        postgres_conn_id='my_postgres_connection_id',
        sql='SELECT * FROM monthly_category_sold',
        bucket=gcs_bucket,
        filename='data/monthly_category_sold/{{ ds }}.json',
        export_format='json',
    )

    load_to_bq_task_2 = GCSToBigQueryOperator(
        task_id='load_monthly_category_sold_to_bigquery',
        bucket=gcs_bucket,
        source_objects=['data/monthly_category_sold/{{ ds }}.json'],
        destination_project_dataset_table=f'{project_id}.{dataset_id}.monthly_category_sold',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
    )

    extract_joined_data_task_3 = PostgresToGCSOperator(
        task_id='load_best_employee_to_gcs',
        postgres_conn_id='my_postgres_connection_id',
        sql='SELECT * FROM best_employee',
        bucket=gcs_bucket,
        filename='data/best_employee/{{ ds }}.json',
        export_format='json',
    )

    load_to_bq_task_3 = GCSToBigQueryOperator(
        task_id='load_best_employee_to_bigquery',
        bucket=gcs_bucket,
        source_objects=['data/best_employee/{{ ds }}.json'],
        destination_project_dataset_table=f'{project_id}.{dataset_id}.best_employee',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_TRUNCATE',
    )

    transform_task >> join_query_task_1 >> extract_joined_data_task_1 >> load_to_bq_task_1
    transform_task >> join_query_task_2 >> extract_joined_data_task_2 >> load_to_bq_task_2
    transform_task >> join_query_task_3 >> extract_joined_data_task_3 >> load_to_bq_task_3

    extract_task >> transform_task

    for table in tables:
        load_to_gcs_task = PostgresToGCSOperator(
            task_id=f'load_{table}_to_gcs',
            postgres_conn_id='my_postgres_connection_id',
            sql=f'SELECT * FROM {table}',
            bucket=gcs_bucket,
            filename=f'data/{table}/{{{{ ds }}}}.json',
            export_format='json',
        )

        load_to_bq_task = GCSToBigQueryOperator(
            task_id=f'load_{table}_to_bigquery',
            bucket=gcs_bucket,
            source_objects=[f'data/{table}/{{{{ ds }}}}.json'],
            destination_project_dataset_table=f'{project_id}.{dataset_id}.{table}',
            source_format='NEWLINE_DELIMITED_JSON',
            write_disposition='WRITE_TRUNCATE',
        )

        transform_task >> load_to_gcs_task >> load_to_bq_task




