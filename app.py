from flask import Flask, jsonify, request
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import json
import os
from flask_marshmallow import Marshmallow


app = Flask(__name__)

ma = Marshmallow(app)

#cosmos db data
url = os.environ['COSMOS_URI']
key = os.environ['COSMOS_KEY']
database_name = 'testPowerPlants'
container_name = 'powerPlants'


# Initialize the Cosmos client
client = CosmosClient(url, credential=key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)


# Query data
query = "SELECT * from c"
plants = list(container.query_items(
    query=query,
    enable_cross_partition_query=True
))





@app.route("/")
def home():
    return "hello flask!"

@app.route('/powerplants', methods=['GET'])
def powerplants():
    result = plants_schema.dump(plants)
    return jsonify(result)



class PlantSchema(ma.Schema):
    class Meta:
        fields = ('name', 'coordinates', 'outputMWH', 'fuelTypes', 'Renewable')



plant_schema = PlantSchema()
plants_schema = PlantSchema(many=True)

if __name__ == '__main__':
    app.run()