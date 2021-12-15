from flask import Flask, jsonify, request, abort
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import json
import os
from flask_marshmallow import Marshmallow
import uuid


app = Flask(__name__)


# cosmos db data
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


@app.route("/")
def home():
    return "hello flask!"


@app.route('/powerplants', methods=['GET'])
def getPowerPlants():
    plants = list(container.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    result = plants_schema.dump(plants)
    return jsonify(result)


@app.route('/powerplants/<name>', methods=['GET'])
def getPowerPlantsByName(name):
    print(name)
    plants = list(container.query_items(
        query=f'SELECT * from c WHERE lower(c.name)=lower(\'{name}\')',
        enable_cross_partition_query=True
    ))

    if len(plants) == 0:
        abort(404)

    plant = plants[0]
    result = plant_schema.dump(plant)
    return jsonify(result)


@app.route('/addpowerplants', methods=['POST'])
def addNewPlant():
    if not request.json:
        abort(400)
    powerPlant = request.json
    powerPlantRecord = {**powerPlant, 'id': str(uuid.uuid4())}
    print(powerPlantRecord)
    matchingNames = list(container.query_items(
        query=f'SELECT * from c WHERE lower(c.name)=lower(\'{powerPlantRecord["name"]}\')',
        enable_cross_partition_query=True
    ))
    errors = []

    if len(matchingNames) > 0:
        errors.append('Duplicate names found')

    matchingIds = list(container.query_items(
        query=f'SELECT * from c WHERE lower(c.id)=lower(\'{powerPlantRecord["id"]}\')',
        enable_cross_partition_query=True
    ))

    if len(matchingIds) > 0:
        errors.append("Duplicate IDs found")

    if powerPlantRecord['outputMWH'] < 0:
        errors.append("outputMWH must be greater than 0")

    if powerPlantRecord['coordinates']['lat'] > 90 or powerPlantRecord['coordinates']['lat'] < -90 :
        errors.append("Latitude outside realistic range")
    
    if powerPlantRecord['coordinates']['lat'] > 90 or powerPlantRecord['coordinates']['lat'] < -90 :
        errors.append("Latitude outside realistic range")

    if len(errors) > 0:
        response = jsonify({'errors': errors})
        response.status_code = 400
        return response

    print(powerPlant)
    dbResult = container.upsert_item(powerPlantRecord)
    result = plant_schema.dump(dbResult)

    return result


# initialize serailizer
ma = Marshmallow(app)


class PlantSchema(ma.Schema):
    class Meta:
        fields = ('id', 'name', 'coordinates',
                  'outputMWH', 'fuelTypes', 'Renewable')


plant_schema = PlantSchema()
plants_schema = PlantSchema(many=True)


if __name__ == '__main__':
    app.run()
