import boto3
import time
import os
import json
import logging
import traceback
import shutil
from airflow.decorators import dag
from airflow.decorators import task
from datetime import datetime
from datetime import timedelta
from datetime import date
from typing import Dict
from dotenv import load_dotenv

from prepData.prepData import PrepData
from train.train import Autoencoder_Model

load_dotenv()
YESTURDEY = date.today() - timedelta(days=1)


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}

@dag(
    dag_id='dag_with_preparation_training_validation_models',
    default_args=default_args,
    start_date=datetime(YESTURDEY.year, YESTURDEY.month, YESTURDEY.day),
    schedule_interval="@daily"
    )
def example_dag():

    @task(task_id="put_jsons_from_s3_to_local")
    def get_jsons_from_s3_to_local(**kwargs):
        DATA_WINDOW = 3
        DATE_TIME_TEST = datetime(2024, 7, 4) # 2024-07-04
        CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
        USE_DIR = os.path.join(os.path.split(CURRENT_DIR)[0],
                               'jsons')

        AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
        print(AWS_ACCESS_KEY_ID)
        logging.info(f"AWS_ACCESS_KEY_ID: {AWS_ACCESS_KEY_ID}")

        AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
        print(AWS_SECRET_ACCESS_KEY)
        logging.info(f"AWS_SECRET_ACCESS_KEY: {AWS_SECRET_ACCESS_KEY}")

        BUCKET_ID = os.environ.get('BUCKET_ID')
        print(BUCKET_ID)
        logging.info(f"BUCKET_ID: {BUCKET_ID}")

        BASE_DATA_DIR = os.environ.get('BASE_DATA_DIR')
        print(BASE_DATA_DIR)
        logging.info(f"BASE_DATA_DIR: {BASE_DATA_DIR}")

        session = boto3.session.Session()
        s3 = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id = AWS_ACCESS_KEY_ID,
            aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

        # s3 = kwargs["task_instance"].xcom_pull(task_ids="s3_connection", key="s3")

        if not os.path.isdir(USE_DIR):
            os.mkdir(USE_DIR)
        else:
            shutil.rmtree(USE_DIR)
            os.mkdir(USE_DIR)

        try:
            # cur_time: datetime = kwargs['logical_date'] # ds
            cur_time = DATE_TIME_TEST
            logging.info(f"CURRENT TIME: {cur_time}")
        except:
            cur_time: datetime = kwargs['ds'] # ds
            logging.info(f"CURRENT TIME: {cur_time}")

        cur_time_sec = cur_time.timestamp()
        struct_cur_time = time.localtime(cur_time_sec)
        str_cur_time = time.strftime('%Y-%m-%d %H:%M:%S', struct_cur_time)

        last_time_window: list = [str_cur_time]
        up_time_skip = timedelta(days=1).total_seconds()

        all_units_prefixes = []

        for i in range(DATA_WINDOW-1):
            upper_time = cur_time_sec - up_time_skip
            str_upper_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(upper_time))
            last_time_window.append(str_upper_time)
            up_time_skip += timedelta(days=1).total_seconds()

        create_date_dir: str = ''
        for index, date_id in enumerate(last_time_window[::-1]):
            date_prefix = f"{BASE_DATA_DIR}/{date_id}/"
            try:
                s3_obj = s3.list_objects_v2(Bucket=BUCKET_ID, Prefix=date_prefix, Delimiter = "/", MaxKeys=1000)
                print(f"s3_obj - {s3_obj}")
            except Exception as e:
                logging.error(traceback.format_exc())
                print(f"ERROR get s3_obj - {repr(e)}")
                raise
            if 'CommonPrefixes' not in s3_obj:
                continue  
            else:
                logging.info(f"s3_obj['CommonPrefixes'] - {s3_obj['CommonPrefixes']}")
                print(f"s3_obj['CommonPrefixes'] - {s3_obj['CommonPrefixes']}")

                all_units_prefixes.extend(s3_obj['CommonPrefixes'])
                only_date = date_id.split(' ')[0]
                if index == len(last_time_window) - 1:
                    create_date_dir += f"{only_date}"
                else:
                    create_date_dir += f"{only_date}_"

        date_dir_path = os.path.join(USE_DIR, create_date_dir)
        try:
            if not os.path.isdir(date_dir_path):
                os.mkdir(date_dir_path)
        except Exception as e:
            logging.error(f"ERROR: make date_dir_path - {traceback.format_exc()}")
            print(f"ERROR: make date_dir_path - {traceback.format_exc()}")
            print(f"ERROR: make date_dir_path repr(e) - {repr(e)}")
            raise

        logging.info(f"all_units_prefixes - {all_units_prefixes}")
        print(f"all_units_prefixes - {all_units_prefixes}")

        for unit_prefix in all_units_prefixes:
            current_unit = os.path.split(unit_prefix['Prefix'].rstrip('/'))[-1]

            current_unit_dir = os.path.join(date_dir_path, current_unit)
            try:
                if not os.path.isdir(current_unit_dir):
                    os.mkdir(current_unit_dir)
            except Exception as e:
                logging.error(f"ERROR: make current_unit_dir - {traceback.format_exc()}")
                raise

            try:
                get_all_one_unit_jsons = s3.list_objects_v2(Bucket=BUCKET_ID, Prefix=unit_prefix['Prefix'], Delimiter = "/", MaxKeys=1000)
                
                logging.info(f"get_all_one_unit_jsons - {get_all_one_unit_jsons}")
                print(f"get_all_one_unit_jsons - {get_all_one_unit_jsons}")
            except Exception as e:
                logging.error(f"ERROR: all_one_unit_jsons - {traceback.format_exc()}")
                raise
            if 'Contents' in get_all_one_unit_jsons:
                for unit_json in get_all_one_unit_jsons['Contents']:
                    try:
                        get_json_response = s3.get_object(Bucket=BUCKET_ID, Key=unit_json['Key'])
                    except Exception as e:
                        logging.error(f"ERROR: get_json_response - {traceback.format_exc()}")
                        raise
                    json_name = os.path.split(unit_json['Key'].rstrip('/'))[-1]
                    json_dir = os.path.join(current_unit_dir, json_name)

                    json_obj = json.loads(get_json_response['Body'].read())
                    with open(json_dir, 'w') as json_write:
                        json_obj = json.dump(json_obj, json_write)
            else:
                logging.info(f"get_all_one_unit_jsons - No Contents")
                print(f"get_all_one_unit_jsons - No Contents")
        logging.info(f"Path to date dir with units: {date_dir_path}")
        return date_dir_path

    @task(multiple_outputs=True, task_id="organization_of_preprocessing_data")
    def preprocess_data(date_dir_path) -> Dict[str, str]:
        
        print(f"date_dir_path list - {os.listdir(date_dir_path)}")
        logging.info(f"date_dir_path list - {os.listdir(date_dir_path)}")
        
        current_dir = os.path.dirname(os.path.realpath(__file__))

        processed_path_dir = os.path.join(os.path.split(current_dir)[0], 'processed')
        try:
            if not os.path.isdir(processed_path_dir):
                os.mkdir(processed_path_dir)
        except Exception as e:
            logging.error(f"ERROR: processed_path_dir - {traceback.format_exc()}")
            raise
        final_path_dir = os.path.join(os.path.split(current_dir)[0], 'final')
        try:
            if not os.path.isdir(final_path_dir):
                os.mkdir(final_path_dir)
        except Exception as e:
            logging.error(F"ERROR: final_path_dir - {traceback.format_exc()}")
            raise
        #
        try:
            logging.info(f"date_dir_path - {date_dir_path}")
            print(f"date_dir_path - {date_dir_path}")
            logging.info(f"processed_path_dir - {processed_path_dir}")
            print(f"processed_path_dir - {processed_path_dir}")
            logging.info(f"final_path_dir - {final_path_dir}")
            print(f"final_path_dir - {final_path_dir}")
            PrepData.start_prepData_json(path_raw=date_dir_path,
                                    path_processed=processed_path_dir,
                                    path_final=final_path_dir)
        except Exception as e:
            logging.error(F"ERROR: res - PrepData.start_prepData_json - {traceback.format_exc()}")
            raise 
        return {"processed_path_dir": processed_path_dir, "final_path_dir": final_path_dir}
    
    @task(task_id="train_and_valid_data")
    def train_and_vaild_data(data_dirs, **kwargs):

        DAGSHUB_USER = os.environ.get('DAGSHUB_USER')
        print(DAGSHUB_USER)
        logging.info(f"DAGSHUB_USER: {DAGSHUB_USER}")

        DAGSHUB_PASSWORD = os.environ.get('DAGSHUB_PASSWORD')
        print(DAGSHUB_PASSWORD)
        logging.info(f"DAGSHUB_PASSWORD: {DAGSHUB_PASSWORD}")

        DAGSHUB_TOKEN = os.environ.get('DAGSHUB_TOKEN')
        print(DAGSHUB_TOKEN)
        logging.info(f"DAGSHUB_TOKEN: {DAGSHUB_TOKEN}")

        processed_path_dir = str(data_dirs["processed_path_dir"])
        final_path_dir = str(data_dirs["final_path_dir"])

        path_Train_data = os.path.join(final_path_dir, "train_and_test", "train.csv")
        path_Test_data = os.path.join(final_path_dir, "train_and_test", "test.csv")
        path_Predict_data = os.path.join(final_path_dir, "static_valid", "Static_validation_Normal.csv")

        cur_date_time = time.time()
        loc_cur_date_time = time.localtime(cur_date_time)
        str_cur_date_time = time.strftime('%Y-%m-%d_%H-%M-%S', loc_cur_date_time)

        autoencoder = Autoencoder_Model()
        model = autoencoder.start_train_and_save_mlflow(path_Train_data=path_Train_data,
                                                path_Valid_Data=path_Test_data,
                                                path_Predict_Data= path_Predict_data,
                                                name_experiment=str_cur_date_time,
                                                dagshub_pass=DAGSHUB_PASSWORD,
                                                dagshub_token=DAGSHUB_TOKEN,
                                                dagshub_username=DAGSHUB_USER)

    
    jsons_dir_path = get_jsons_from_s3_to_local()

    preprocess_data_dirs = preprocess_data(jsons_dir_path)
    
    model = train_and_vaild_data(preprocess_data_dirs)

greet_dag = example_dag()