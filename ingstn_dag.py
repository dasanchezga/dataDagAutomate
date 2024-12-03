from datetime import datetime
import logging
import sys
import time
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import boto3
import io
import csv

ENV = Variable.get("ENV", default_var = "dev")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

s3_bucket = f"co-std-intec-{ENV}"
s3_key = 'assets/campaign/tabla_parametrica_cmpgn_prd_trnsfrmtn_hm.csv'
s3 = boto3.resource('s3')
obj = s3.Object(s3_bucket, s3_key)


v_start_date=datetime(2024, 8, 3)
#v_end_date=datetime(2024, 6, 3)

#función para ejecutar los jobs
def glue_job_runner(job_name: str, ds: str, **kwargs: dict) -> None:
    ds_datetime = datetime.strptime(ds, '%Y-%m-%d')
    one_day_ago = (ds_datetime- timedelta(days=2)).strftime("%Y-%m-%d")
    #log del dia de ejecucion 
    logger.info(f"RUNNING FOR: {one_day_ago}")
    args = {"--FCT_DT": one_day_ago}

    #Inicializar y ejecutar el job
    glue_hook = GlueJobHook(job_name=job_name)
    job_run_id = glue_hook.initialize_job(script_arguments=args)["JobRunId"]
    glue_hook.job_completion(job_name=job_name, run_id=job_run_id, verbose=True)

#funcion para crear operadores de python para jobs
def glue_job(job_name: str) -> PythonOperator:
    return PythonOperator(
        task_id=job_name,
        python_callable=glue_job_runner,
        op_kwargs={"job_name": job_name},
        provide_context=True,
        dag=dag
    )

def glue_job_weekly(job_name: str) -> PythonOperator:
    return PythonOperator(
        task_id=job_name,
        python_callable=glue_job_runner,
        op_kwargs={"job_name": job_name},
        provide_context=True,
        dag=dag
    )

def glue_job_monthly(job_name: str) -> PythonOperator:
        return PythonOperator(
        task_id=job_name,
        python_callable=glue_job_runner,
        op_kwargs={"job_name": job_name},
        provide_context=True,
        dag=dag
    )

#Logica para decidir si es semanal o mensual o ambas 
def job_type(**kwargs):
    #today = datetime.now()
    execution_date = kwargs['ds']
    execution_date = datetime.strptime(execution_date, '%Y-%m-%d').date()
    execution_date = execution_date - timedelta(days=2)
    logger.info(f"Today is: {execution_date}")
    logger.info(f"Day of the month: {execution_date.day}")
    logger.info(f"Day of the week: {execution_date.weekday()}")
    if execution_date.day == 1 and execution_date.weekday() == 0: #Si el primer dia del mes es lunes
        return ['glue_7', 'glue_8']# Ejecutar
    elif execution_date.day == 1: #primer dia del mes
        return 'finish'
    elif execution_date.weekday() == 0: # Si es solo lunes, primer dia de la semana
        return  ['glue_7', 'glue_8']
    else:
        return 'finish'

#################################

with DAG(
    dag_id="airflow_co_cmpgn_hm_trnsfrm",
    schedule_interval='@daily',
    start_date=v_start_date,
    #end_date=v_end_date,
    tags=["co-hm-cmpgn"],
    catchup=True,
    max_active_runs=1,
    concurrency=30,
    default_args={
        "depends_on_past": True,
        "wait_for_downstream": True,
        "retries": 1,
        "retry_delay": 180
    }
) as dag:


    # Definir las tareas
    start = DummyOperator(task_id="start", dag=dag)
    finish = DummyOperator(task_id="finish", trigger_rule='all_done', dag=dag)
    batch_1 = DummyOperator(task_id="batch_1", dag=dag)
    batch_2 = DummyOperator(task_id="batch_2", dag=dag)
    batch_3 = DummyOperator(task_id="batch_3", dag=dag)

    # transformations
    ingstn_glue1 = glue_job("glue_1")
    ingstn_glue2  = glue_job("glue_2")
    ingstn_glue3  = glue_job("glue_3")
    ingstn_glue4  = glue_job("glue_4")
    ingstn_glue5  = glue_job("glue_5")
    ingstn_glue6 = glue_job("glue_6")
    ingstn_glue7 = glue_job_weekly("glue_7")
    ingstn_glue8 = glue_job_weekly("glue_8")
    
    

    #BranchOperator para decidir las frecuencias 
    choose_frecuency= BranchPythonOperator(
        task_id ='choose_frecuency',
        python_callable = job_type,
        provide_context = True,
    )


    start >> [ingstn_glue1, ingstn_glue2, ingstn_glue3] >> batch_1 
    batch_1 >> [ingstn_glue4, ingstn_glue5, ingstn_glue6] >> batch_2
    batch_2 >> choose_frecuency>>[ingstn_glue7, ingstn_glue8] >> finish
    