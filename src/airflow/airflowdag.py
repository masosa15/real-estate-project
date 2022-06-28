from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args={
    'owner':'airflow',
    'star_date': datetime(2022, 13, 6),
    'email': ['data.engineer.re@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_sucesss': False,
    'retries' : 1,
    'retries_delay': timedelta(minutes=5)
}

dag = DAG(task_id = 'Ingest_Dag',
          default_args = default_args,
          schedule_interval = '@daily' #'0 0 * * *'
          )

haya_task = BashOperator(task_id='haya_ingest',
                         bash_command='python3 ~/scripts/haya-ingest.py',
                         dag=dag)

ib_task = BashOperator(task_id='ib_ingest',
                       bash_command='python3 ~/scripts/IB-ingest.py',
                       dag=dag)

ov_task = BashOperator(task_id='ov_ingest',
                       bash_command='python3 ~/scripts/OV-ingest.py',
                       dag=dag)

idealista_task = BashOperator(task_id='idealista-ingest',
                         bash_command='python3 ~/scripts/idealista.py',
                         dag = dag)


haya_task >> ib_task >> ov_task >> idealista_task