import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 

with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=DeprecationWarning)
    from datetime import datetime
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    import json
    import requests

dict_args = {
    "owner": "main",
    "start_date": datetime(2024, 1, 1, 00)
}


def get_data():
    # get data from api
    res = requests.get("https://randomuser.me/api/")
    
    # to json
    res = res.json()
    res = res["results"][0]
    
    return res


def format_user_data(res):
    user_data = {}

    location = res["location"]

    user_data["first_name"] = res["name"]["first"]
    user_data["last_name"] = res["name"]["last"]

    user_data["gender"] = res["gender"]
    user_data["address"] = \
        f"{str(location['street']['number'])} {location['street']['name']}, " \
        f"{location['city']} {location['state']} {location['country']}"
    user_data["postcode"] = location["postcode"]
    user_data["email"] = res["email"]
    user_data["username"] = res["login"]["username"]
    user_data["dob"] = res["dob"]["date"]
    user_data["registered_date"] = res["registered"]["date"]
    user_data["phone"] = res["phone"]
    user_data["picture"] = res["picture"]["medium"]

    return user_data

def stream_data():
    res = get_data()
    res = format_user_data(res)
    print(json.dumps(res, indent=3))

# test
stream_data()

# with DAG(
#     "dag_1",
#     default_args=dict_args,
#     schedule_interval="@daily",
#     catchup=False
# ) as dag:
#     task_streaming = PythonOperator(
#         task_id="stream_data_from_api",
#         python_calllable=data_stream
#     )    






