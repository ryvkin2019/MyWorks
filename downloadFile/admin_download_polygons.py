import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text
import json

# Establish a connection to your PostgreSQL database

#engine = create_engine(f"mysql://admin:205Pishpeshonim@qa.cqjtnaxf7qxk.eu-west-1.rds.amazonaws.com/fieldin")
engine = create_engine(f"mysql://admin:205Pishpeshonim@production-repl1-dev.c59qewjoyitg.eu-west-1.rds.amazonaws.com/fieldin")

print('0')

def execute_query(group_id):
    # Create a cursor object to interact with the database
    with engine.connect() as connection:

    #    sql_query=f""" SELECT * FROM fieldin.`groups` where id = {group_id}"""

        sql_query = f"""
        CREATE TEMPORARY TABLE polygon_for_geojson
        AS
        SELECT JSON_OBJECT(
            'type', 'Feature',
            'properties', JSON_OBJECT(
                'crop', c.token,
                'polygon_name', p.name,
                'id', p.id,
                'row_span', pp2.value,
                'tree_span',pp3.value,
                'cultivar',cu.token,
                'year', pl.planted_on,
                'som', c2.som,
                'area', p.area
            ),
            'geometry', p.geometry
        ) AS feature
        FROM group_polygons gp
        INNER JOIN polygons p ON gp.polygon_id = p.id
        INNER JOIN polygon_properties pp ON p.id = pp.polygon_id AND pp.name = 'crop_id' AND pp.latest = 1
        INNER JOIN polygon_properties pp2 ON p.id = pp2.polygon_id AND pp2.name ='row_span' AND pp2.latest = 1
        INNER JOIN polygon_properties pp3 ON p.id = pp3.polygon_id AND pp3.name ='tree_span' AND pp3.latest = 1
        INNER JOIN plantings pl ON p.id = pl.block_id
        INNER JOIN cultivars cu ON cu.id = pl.cultivar_id
        INNER JOIN crops c ON c.id = pp.value
        INNER join companies c2 on c2.id=p.company_id 
        WHERE gp.group_id = {group_id}
          AND p.archived_at IS NULL
          AND p.deleted_at IS NULL
          AND p.`type` = 'plot'
         and pl.deleted_at is not NULL ;
        """

        connection.execute(text(sql_query), {"group_id": group_id})
        print('1')

        #pd.read_sql(text(sql_query), connection)
       # devices_table.to_dict(orient='records')

        sql_query3 = f"""SELECT JSON_OBJECT(
         'type', 'FeatureCollection',
          'features', JSON_ARRAYAGG(feature)
            ) AS geojson
            FROM polygon_for_geojson;"""

        devices_table = pd.read_sql(text(sql_query3), connection)
        records = devices_table.to_dict(orient='records')
        print(records)

        sql_query2 = f"""DROP TEMPORARY TABLE polygon_for_geojson;"""
        connection.execute(text(sql_query2), {"group_id": group_id})

        #convert to json ggg
        json_object=json.loads(records[0]['geojson'])
        print(json_object)

        som= {
        "imperial":{
            "area": 4.047
        }
        }

        for i in json_object["features"]:
            i["properties"]["area"]= int(i["properties"]["area"]) / som["imperial"]["area"]

        print("2")
        print(json_object)
        #print(type(records[0]['geojson']))

    # Open a file for writing
    with open('json_object.geojson', 'w') as f:
        # Convert Python object to JSON and write it to the file
        print(type(json_object))
        json.dump(json_object, f)
        print("4")


group_id = 1394

execute_query(group_id)


"""
import json

# Define your data
data = {
  "name": "John Doe",
  "email": "john@example.com",
  "age": 30
}

# Open a file for writing
with open('data.json', 'w') as f:
    # Convert Python object to JSON and write it to the file
    json.dump(data, f)
"""