from __future__ import print_function

import time
import boto3
import logging
import airflow

from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.hooks.aws_glue_catalog_hook import AwsGlueCatalogHook

def aws_crawler_function(ds, **kwargs):
    """
    This can be any python code you want and is called from the python operator. The code is not executed until
    the task is run by the airflow scheduler.
    """
    crawler_name = kwargs['crawler_name']
    print('crawler_name is ', crawler_name)
    hook = AwsGlueCatalogHook(aws_conn_id='aws_default', region_name='us-east-1')
    client = hook.get_conn()
    return client.start_crawler(
        Name = crawler_name
    )

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 13),
    'provide_context': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'econect',
    schedule_interval='@daily',
    default_args=args,
    max_active_runs=1)
with dag:
    execution_date = '2020-03-30'
    start_date = '2020-03-29'
    end_date = '2020-03-31'
    dag_config = Variable.get("config", deserialize_json=True)
    log_url = dag_config['LogUri'] 
    
    # S3 buckets for econet_data_engineering.py
    extract_script = dag_config['econet_engineering_script_path']
    econet_engineering_destination_bucket = dag_config['econet_engineering_destination_bucket_path']
    econet_engineering_source_bucket = dag_config['econet_engineering_source_bucket_path']

    # S3 buckets for combine_polling_connection_data.py
    combine_polling_data_script_path = dag_config['combine_polling_data_script_path']
    combine_polling_data_destination_bucket = dag_config['combine_polling_data_destination_bucket_path']
    combine_polling_data_source_bucket = dag_config['combine_polling_data_source_bucket_path']

    crawler_config = Variable.get("crawler_config",deserialize_json = True)
    aws_access_key_id = crawler_config['aws_access_key_id']
    aws_secret_access_key = crawler_config['aws_secret_access_key']
    crawler_name = crawler_config['crawler_name']

    # https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html
    default_emr_settings = {"Name": "Data Engineering",
                            "LogUri": log_url,
                            "ReleaseLabel": "emr-5.29.0",
                            "Instances": {
                                "InstanceGroups": [
                                    {
                                        "Name": "Master nodes",
                                        # "Market": "SPOT",
                                        "Market": "ON_DEMAND",
                                        "EbsConfiguration":{
                                                 "EbsBlockDeviceConfigs":[
                                                    {
                                                       "VolumeSpecification":{
                                                          "SizeInGB":32,
                                                          "VolumeType":"gp2"
                                                       },
                                                       "VolumesPerInstance":2
                                                    }
                                                 ]
                                              },
                                        "InstanceRole": "MASTER",
                                        "InstanceType": "m5.2xlarge",
                                        "InstanceCount": 1
                                    },
                                    {
                                        "Name": "Slave nodes",
                                        "EbsConfiguration":{
                                                 "EbsBlockDeviceConfigs":[
                                                    {
                                                       "VolumeSpecification":{
                                                          "SizeInGB":32,
                                                          "VolumeType":"gp2"
                                                       },
                                                       "VolumesPerInstance":2
                                                    }
                                                 ]
                                              },
                                        # "Market": "SPOT",
                                        "Market": "ON_DEMAND",
                                        "InstanceRole": "CORE",
                                        "InstanceType": "m5.2xlarge",
                                        "InstanceCount": 2
                                    }
                                ],
                                "Ec2KeyName": "chandani-apache-airflow-training",
                                "KeepJobFlowAliveWhenNoSteps": True, # todo not sure
                                'EmrManagedMasterSecurityGroup': 'sg-0235feea873ec73b8',
                                'EmrManagedSlaveSecurityGroup': 'sg-08cb0237b40ef359c',
                                'Placement': {
                                    'AvailabilityZone': 'us-east-1a',
                                },

                            },
                            "Applications": [
                                {"Name": "Spark"}, #todo is it required?
                                {"Name": "Hadoop"},
                            ],
                            "VisibleToAllUsers": True, # todo not found
                            "JobFlowRole": "EMR_EC2_DefaultRole", # todo called InstanceProfile?
                            "ServiceRole": "EMR_DefaultRole",
                            }


    create_job_flow_task = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        aws_conn_id='aws_default',
        job_flow_overrides=default_emr_settings,
        dag=dag,
        region_name="us-east-1"
    )


    extract_step_task = EmrAddStepsOperator(
        task_id= 'extract_step', # 'add_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=[
            {
                "Name": "Step1 Preprocess",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar", # todo https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html
                    "Args": ["spark-submit", "--deploy-mode", "client",
                                        "--conf", "spark.default.parallelism=6",
                                        "--conf", "spark.driver.cores=4",
                                        "--conf","spark.dynamicAllocation.enabled=true",
                                        "--conf", "spark.shuffle.service.enabled=true",
                                        "--conf", "spark.maxResultSize=8G",
                                        "--conf", "spark.driver.memory=8G",
                                        "--conf", "spark.driver.memoryOverhead=2G",
                                        "--conf", "spark.dynamicAllocation.minExecutors=14",
                                        "--conf", "spark.dynamicAllocation.maxExecutors=40",
                                        "--conf", "spark.executor.memory=4G",
                                        "--conf", "spark.executor.memoryOverhead=1G",
                                        #"s3://econet-data-engineering-code/pyspark/econet_data_engineering.py",
                                        extract_script,
                                        "--source_bucket", econet_engineering_source_bucket, #"s3://rheemconnectrawdata/history/",
                                        "--destination", econet_engineering_destination_bucket, #"s3://weiyutest/",
                                        "--input_date", execution_date
                                        ]
                }
            }
        ],
    )

    watch_extract_step_task = EmrStepSensor(
        task_id= 'watch_extract_step', #'watch_prev_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('extract_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    connect_step_task = EmrAddStepsOperator(
        task_id='connect_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=[
            {
                "Name": "Step2 Merge",
                "ActionOnFailure": "CONTINUE",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar", # todo https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-commandrunner.html
                    "Args": ["spark-submit", "--deploy-mode", "client",
                                        "--conf", "spark.default.parallelism=6",
                                        "--conf", "spark.driver.cores=4",
                                        "--conf","spark.dynamicAllocation.enabled=true",
                                        "--conf", "spark.shuffle.service.enabled=true",
                                        "--conf", "spark.maxResultSize=8G",
                                        "--conf", "spark.driver.memory=8G",
                                        "--conf", "spark.driver.memoryOverhead=2G",
                                        "--conf", "spark.dynamicAllocation.minExecutors=14",
                                        "--conf", "spark.dynamicAllocation.maxExecutors=40",
                                        "--conf", "spark.executor.memory=4G",
                                        "--conf", "spark.executor.memoryOverhead=1G",
                                        combine_polling_data_script_path,
                                        #"s3://econet-data-engineering-code/pyspark/combine_polling_connection_data.py",
					                    "--destination", combine_polling_data_destination_bucket,
                                        "--source", combine_polling_data_source_bucket,
                                        "--start_date", start_date,
                                        "--end_date", end_date
                                        ]
                }
            }
        ],
    )

    watch_connect_step_task = EmrStepSensor(
        task_id='watch_connect_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('connect_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    terminate_job_flow_task = EmrTerminateJobFlowOperator(
        task_id='terminate_job_flow',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        trigger_rule="all_done",
    )


    aws_crawler = PythonOperator(
        task_id = 'aws_crawler',
        python_callable = aws_crawler_function,
        trigger_rule='all_success',
        op_kwargs = {
            'crawler_name': crawler_name
        },
    )


    create_job_flow_task >> extract_step_task
    extract_step_task >> watch_extract_step_task
    watch_extract_step_task >> connect_step_task
    connect_step_task >> watch_connect_step_task
    watch_connect_step_task >> terminate_job_flow_task
    watch_connect_step_task >> aws_crawler
