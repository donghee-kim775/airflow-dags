import os
from datetime import timedelta

class DEFAULT_SPARK:
    driver_config={
            "cores": 1,
            "coreLimit": "1200m",
            "memory": "1g",
            "serviceAccount": "spark-driver-sa",
        }

    executor_config={
            "cores": 1,
            "instances": 2,  # executor의 pod 개수
            "memory": "1g",
        }

    deps={
            "jars": [
                "local:///opt/spark/user-jars/hadoop-aws-3.3.1.jar",
                "local:///opt/spark/user-jars/aws-java-sdk-bundle-1.11.901.jar",
            ],
        }

    spark_conf={
            "spark.hadoop.fs.s3a.access.key": os.getev("aws_access_key_id"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("aws_secret_access_key"),
            "spark.hadoop.fs.s3a.endpoint": "s3.amazonaws.com",
            "spark.kubernetes.driver.deleteOnTermination": "true",
            "spark.kubernetes.executor.deleteOnTermination": "true",
        }
    
class DEFAULT_DAG:
    default_args = {
        'owner': 'ehdgml7755@cu.ac.kr',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 24,
        'retry_delay': timedelta(minutes=30),
    }
    