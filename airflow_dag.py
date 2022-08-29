import os
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
from airflow.models import Variable
# Operators; we need this to operate!
from datetime import datetime, timedelta
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator

# Bash Command prefix to login interactive shell
prefix_comm     = Variable.get("PREFIX_COMMAND", default_var="bash --login -c")
runEnv_variable = Variable.get("RUN_ENV_RENEWAL", default_var="dev")
#clone the dev branch of ds_mlc_maintenance into the below branch


rootPath = '/data/lake/clds'
if runEnv_variable == 'dev':
   # Get the user from the environment variable
   user                 = Variable.get("MLCDS_HARI", default_var=os.getenv("USER"))
   BaseFldr             = Variable.get("S3_BASE_FLDR", default_var = os.getenv("BaseFldr"))
   app_path             = rootPath + '/mlc_repos/'+ user + '/fre'                                          # Application path   
   sql_path             = app_path + '/sql'                                                                # SRC path
   src_path             = app_path + '/src/data'                                                           # SRC path
   out_path             = app_path + '/output'                                                             # Output path
   ora_origin_schema    = user[:-1]                                                                        #Schea name in Oracle
   #hive_team_schema     = 'zn753627'                                                                       # Hive Team Schema
   #s3_team_path         = BaseFldr + '/user/'+ hive_team_schema[1:].upper()                                # S3 User toot path
   sftp_conn_id         = 'DS_TRIAGE_SFTP'                                                                 # Airflow variable for SFTP
   ssh_conn_id          = 'DS_TRIAGE_SSH'                                                                  # Airflow variable for SSH
   hive_team_schema    = 'lake_cl_team_mlcds'                                                             # Hive Team Schema
   s3_team_path        = BaseFldr + '/lake/data/cl/team/native/mlcds/database'              # S3 Team root path
   table_ext            = '_test'                                                                          # Table extention
   ssh_key              = f'deedatasciencesshkeynonprod'                                                   # SSH Key
   end_point            = f'vpce-0c05638aabc30de2e-gg15lv15.vpce-svc-07e03ba2e0eda5e2d.us-east-1.vpce.amazonaws.com' # End Point url



elif (runEnv_variable == 'int'):
   # Get the user from the environment variable
   user                 = Variable.get("MLCDS_AES_SRV", default_var=os.getenv("USER"))
   BaseFldr             = Variable.get("S3_BASE_FLDR", default_var = os.getenv("BaseFldr"))
   app_path             = rootPath + '/mlc_repos/'+ user + '/fre'                                          # Application path
   sql_path             = app_path + '/sql'                                                                # SQL path
   src_path             = app_path + '/src/data'                                                           # SRC path        
   out_path             = app_path + '/output'                                                             # Output path
   ora_origin_schema    = user                                                                             # Schema name in Oracle
   sftp_conn_id         = 'MLCDS_SFTP'                                                                     # Airflow variable for SFTP
   ssh_conn_id          = 'MLCDS_SSH'                                                                      # Airflow variable for SSH
   hive_team_schema     = 'lake_cl_team_mlcds'                                                             # Hive Team Schema
   s3_team_path         = BaseFldr + '/lake/data/cl/team/native/mlcds/database'                            # S3 Team root path
   table_ext            = '_prod'                                                                          # table extentions
   ssh_key              = f'deedatasciencesshkeynonprod'                                                   # SSH key
   end_point            = f'vpce-0f1682c5bf3579965-slbe7uk9.vpce-svc-0276185169646a642.us-east-1.vpce.amazonaws.com' # End Point url


# Dependent file paths
user_path           = f"/data/lake/clds/mlc_repos/"+user
py_renewals         = f"{src_path}/fre_renewal_inputs.py"
py_renewal_main     = f"cd {app_path}/src/batch && pyb fre_renewal_main"
output_json_path    = f"{app_path}/output/firstpartydata.json"
output_files        = f"{app_path}/output/*"
sftp_transfer_cmd   = f" echo 'put {output_files} /renewaltriage/dev/inbound/' | sftp -i {user_path}/{ssh_key} app-4717-DEE-DataScience@{end_point}"


# Print Variable Runtime Values
print('runEnv_variable          : ' + runEnv_variable           )
print('user                     : ' + user                      )
print('user_path                : ' + user_path                 )
print('rootPath                 : ' + rootPath                  )
print('app_path                 : ' + app_path                  )
print('sql_path                 : ' + sql_path                  )
print('src_path                 : ' + src_path                  )
print('ora_origin_schema        : ' + ora_origin_schema         )
print('table_ext                : ' + table_ext                 )
print('hive_team_schema         : ' + hive_team_schema          )
print('s3_team_path             : ' + s3_team_path              )
print('sftp_conn_id             : ' + sftp_conn_id              )
print('ssh_conn_id              : ' + ssh_conn_id               )
print('ssh_key                  : ' + ssh_key                   )
print('end_point                : ' + end_point                 )
print('output_json_path         : ' + output_json_path          ) 
print('sftp_transfer_cmd        : ' + sftp_transfer_cmd         )

# sqoop the data from all the source data (Auto, Gl, Prop, Wc)
process_dict = {
    "auto"  : f"ora2hive {ora_origin_schema} TEST {hive_team_schema} fre_rnwl_triage_auto{table_ext} BATCH_SUB=N SQL_PATH={sql_path}/_batch_execs/x_mlc_rnwl_triage_xtrct_auto.sql ORA_INST=CLDW_DEL_PRD HDFS_DIR={s3_team_path}",
    "gl"    : f"ora2hive {ora_origin_schema} TEST {hive_team_schema} fre_rnwl_triage_gl{table_ext} BATCH_SUB=N SQL_PATH={sql_path}/_batch_execs/x_mlc_rnwl_triage_xtrct_gl.sql ORA_INST=CLDW_DEL_PRD HDFS_DIR={s3_team_path}",
    "prop"  : f"ora2hive {ora_origin_schema} TEST {hive_team_schema} fre_rnwl_triage_prop{table_ext} BATCH_SUB=N SQL_PATH={sql_path}/_batch_execs/x_mlc_rnwl_triage_xtrct_prop.sql ORA_INST=CLDW_DEL_PRD HDFS_DIR={s3_team_path}",
    "wc"    : f"ora2hive {ora_origin_schema} TEST {hive_team_schema} fre_rnwl_triage_wc{table_ext} BATCH_SUB=N SQL_PATH={sql_path}/_batch_execs/x_mlc_rnwl_triage_xtrct_wc.sql ORA_INST=CLDW_DEL_PRD HDFS_DIR={s3_team_path}"
}
#    "bd"    : f"ora2hive {ora_origin_schema} TEST {hive_team_schema} bad_file_pm{table_ext} BATCH_SUB=N SQL_PATH={sql_path}/sql/_batch_execs/x_bad_file_pm.sql ORA_INST=ACTP HDFS_DIR=s3://edopddata-s3-clds-prd-001/lake/data/cl/team/native/mlcds/shared"


location_dict = {
    "auto"  : f"{sql_path}/_batch_execs/x_mlc_rnwl_triage_xtrct_auto.sql",
    "gl"    : f"{sql_path}/_batch_execs/x_mlc_rnwl_triage_xtrct_gl.sql",
    "prop"  : f"{sql_path}/_batch_execs/x_mlc_rnwl_triage_xtrct_prop.sql",
    "wc"    : f"{sql_path}/_batch_execs/x_mlc_rnwl_triage_xtrct_wc.sql"
    
}
#"bd"    : f"/data/lake/clds/mlc_repos/$USER/fre/sql/_batch_execs/x_bad_file_pm.sql"


default_args = {
        "owner": "MLCDS_Airflow_User",
        "email": ['harikrishna.tumpudi@thehartford.com'],
        "email_on_failure": True,
        "email_on_success": True,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    }

dag = DAG('fre_renewals',
            start_date=datetime(2022, 7, 17),
              #schedule_interval= 'None',
              schedule_interval = '0 20 5 1/3 *',
              catchup=False,
              default_args=default_args,
              tags=["fre"])


with dag:

    # renewal_main = SSHOperator(task_id = "main", ssh_conn_id = f"{ssh_conn_id}",
    #                 command = f'{prefix_comm} "{py_renewal_main}"')  
    dummy_operator = DummyOperator(task_id="dummy", trigger_rule="all_success")
    for key, items in process_dict.items():  
        check_file = SFTPSensor(task_id= f"check_file_{key}", path=f"{location_dict[key]}", sftp_conn_id=f"{sftp_conn_id}")
        load_table = SSHOperator(task_id=f"load_table_{key}", ssh_conn_id=f"{ssh_conn_id}",
                    command=f'{prefix_comm} "{items}"')
        check_file >>  load_table >> dummy_operator

    check_renewals_py_file = SFTPSensor(task_id= f"check_renewals_py_file", path=f"{py_renewals}", sftp_conn_id=f"{sftp_conn_id}")
    execute_renewals_py_file = SSHOperator(task_id=f"execute_renewals_py_file", ssh_conn_id=f"{ssh_conn_id}",
                    command=f'{prefix_comm} "python {py_renewals} {runEnv_variable}"')

    check_op_file = SFTPSensor(task_id=f"check_op_file", path = f"{output_json_path}", sftp_conn_id=f"{sftp_conn_id}")
    move_to_s3_bucket = SSHOperator(task_id = f"copy_op_file", ssh_conn_id = f"{ssh_conn_id}",
                        command = f'{prefix_comm} "{sftp_transfer_cmd}"')

    dummy_operator >> check_renewals_py_file >> execute_renewals_py_file >> check_op_file >> move_to_s3_bucket




