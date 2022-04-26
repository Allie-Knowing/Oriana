from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from operators.mysql import MySqlReadOperator
from utils import days_ago

DAG_ID = "action_score"
MYSQL_CONN_ID = "knowing_rds"
REWARD_POOL = 1000
CATEGORY_POINT = [1, 1, 1, 2, 1, 5, 5, 3, 7, 7, -30]
NUM_CATEGORIES = len(CATEGORY_POINT)

default_args = {
    "owner": "knowing",
    "start_date": "2022-04-15T14:15:23Z",
}

GET_ACTION_SCORE_ALL_USERS_TASK_ID = "get_action_score_all_users"
GET_IQ_FROM_ACTION_SCORE_TASK_ID = "get_iq_from_action_score"
GET_IQ_POINT_TO_USER_TASK_ID = "get_iq_point_to_user"
PAY_IQ_POINT_TO_USER_TASK_ID = "pay_iq_point_to_user"
GENERATE_TEMPLATED_SQL_TASK_ID = "generate_templated_sql"


def iq_calc(**context):
    sum_all_categories = 0
    for i in range(NUM_CATEGORIES):
        sum_all_categories += (
            context["ti"].xcom_pull(
                task_ids=f"{GET_ACTION_SCORE_ALL_USERS_TASK_ID}_{i + 1}"
            )
            * CATEGORY_POINT[i]
        )
    if sum_all_categories == 0:
        return 0
    return int(REWARD_POOL / sum_all_categories)


def get_sql_template(**context):
    action_score_multiple = context["ti"].xcom_pull(
        task_ids=GET_IQ_FROM_ACTION_SCORE_TASK_ID
    )
    templated_sql = ""
    for i in range(NUM_CATEGORIES):
        for j in get_iq_point_to_user_task[i].output.resolve(context):
            templated_sql = (
                templated_sql
                + f"""UPDATE iq
            SET cur_cnt = cur_cnt + { CATEGORY_POINT[i] * action_score_multiple }, tot_cnt = tot_cnt + { CATEGORY_POINT[i] * action_score_multiple}
            WHERE user_id = {j[0]};
            """
            )
    return templated_sql


with DAG(
    dag_id=DAG_ID,
    description="Calculation user action score and pay points",
    concurrency=5,
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    user_defined_macros={"days_ago": days_ago},
) as dag:
    # Get action score of all users
    get_action_score_all_users_tasks = [
        MySqlReadOperator(
            task_id=f"{GET_ACTION_SCORE_ALL_USERS_TASK_ID}_{i + 1}",
            mysql_conn_id=MYSQL_CONN_ID,
            sql=f"""SELECT SUM(1)
            FROM action_point
            WHERE category_id = {i + 1}
            AND created_at BETWEEN '{{{{ days_ago(1) }}}}' AND '{{{{ days_ago(0) }}}}';""",
            first=True,
            records_filter=lambda record: int(record[0]) if record[0] != None else 0,
        )
        for i in range(NUM_CATEGORIES)
    ]

    # Get IQ from (reward pool / all users action score)
    get_iq_from_action_score_task = PythonOperator(
        task_id=GET_IQ_FROM_ACTION_SCORE_TASK_ID, python_callable=iq_calc
    )

    # Get how much pay IQ point to user
    get_iq_point_to_user_task = [
        MySqlReadOperator(
            task_id=f"{GET_IQ_POINT_TO_USER_TASK_ID}_{i + 1}",
            mysql_conn_id=MYSQL_CONN_ID,
            sql=f"""SELECT user_id
            FROM action_point
            WHERE category_id = {i + 1}
            AND created_at BETWEEN '{{{{ days_ago(1) }}}}' AND '{{{{ days_ago(0) }}}}';""",
        )
        for i in range(NUM_CATEGORIES)
    ]

    # Generate templated SQL
    generate_templated_sql = PythonOperator(
        task_id=GENERATE_TEMPLATED_SQL_TASK_ID, python_callable=get_sql_template
    )

    # Pay IQ point to user
    pay_iq_point_to_user_task = MySqlOperator(
        task_id=PAY_IQ_POINT_TO_USER_TASK_ID,
        mysql_conn_id=MYSQL_CONN_ID,
        sql=generate_templated_sql.output,
    )

    get_iq_point_to_user_task >> generate_templated_sql
    get_action_score_all_users_tasks >> get_iq_from_action_score_task
    [generate_templated_sql, get_iq_from_action_score_task] >> pay_iq_point_to_user_task
