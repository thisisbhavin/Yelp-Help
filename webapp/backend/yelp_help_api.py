import pandas_gbq
from pathlib import Path
from google.oauth2 import service_account

from flask import Flask
from flask import jsonify
from flask import request
from flask_cors import CORS

app = Flask(__name__)
app.config.from_object('config')
CORS(app, supports_credentials=True)

credentials = service_account.Credentials.from_service_account_file(Path(__file__).with_name('yelp-help-demo-5bbf84fb876b.json'))
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = "yelp-help-demo"

table_name = "yelp-help-demo.bdayelp.menuv3"

@app.route('/health')
def health():
   return "OK"

@app.route("/getcities", methods=["GET"])
def get_cities():
    SQL = f"SELECT distinct city FROM `{table_name}`"
    df = pandas_gbq.read_gbq(SQL)
    cities = df.city.to_list()
    num_cities = len(cities)
    print(df.city.to_list())
    response = {
                    "status": "OK",
                    "cities": cities,
                    "num_cities": num_cities
                }

    response = jsonify(response)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response

@app.route("/getzipcodes", methods=["POST"])
def get_zipcodes():
    print(request.form)
    city = request.form['city']

    SQL = f"SELECT distinct zipcode FROM `{table_name}` where city='{city}' order by zipcode"
    df = pandas_gbq.read_gbq(SQL)

    zipcodes = df.zipcode.to_list()
    num_zipcodes = len(zipcodes)
    print(df.zipcode.to_list())

    response = {
                    "status": "OK",
                    "zipcodes": zipcodes,
                    "num_zipcodes": num_zipcodes
                }

    response = jsonify(response)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


@app.route("/getrestnames", methods=["POST"])
def get_rest_names():
    city = request.form['city']
    zipcode = request.form['zipcode']
    SQL = f"SELECT distinct business_id, name, zipcode, city, rating, num_reviews FROM `{table_name}` where zipcode={zipcode} and city='{city}'"
    df = pandas_gbq.read_gbq(SQL)
    df.loc[:, "rating"] = df.rating.round(2)

    rest_data = {}
    coldefs = []
    for col in df.columns:
        coldefs = coldefs + [{"title": col}]

    rest_data["coldefs"] = coldefs
    rest_data["data"] = df.to_numpy().tolist()
    print(rest_data)

    response = jsonify(rest_data)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


@app.route("/getmenu", methods=["POST"])
def get_menu():

    city = request.form['city']
    zipcode = request.form['zipcode']
    # rest_name = request.form['rest_name']
    id = request.form['business_id']

    SQL = f"SELECT menu, count FROM `{table_name}` where zipcode={zipcode} and city='{city}' and business_id='{id}'"

    df = pandas_gbq.read_gbq(SQL)
    print(df)

    menu_data = {}
    coldefs = [[{"title": "dummy"}]] # dummy because we are hiding 1st column in datatable so this acts as a dummy column
    for col in df.columns:
        coldefs = coldefs + [{"title": col}]

    menu_data["coldefs"] = coldefs

    _ = df.to_numpy()
    menu = eval(_[0][0].replace('"s', "'s").replace(" 's", ' "s').replace("['s", '["s'))
    counts = eval(_[0][1])

    menu_data["data"] = [[0,i, j] for i, j in zip(menu, counts)] # 0 because we are hiding 1st column in datatable so this acts as a dummy column
    

    response = jsonify(menu_data)
    response.headers.add('Access-Control-Allow-Origin', '*')

    return response


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=app.config['PORT_ID'])
