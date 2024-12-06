import requests
from shapely.geometry import shape, Polygon
from shapely import wkt
import json
import datetime
import logging
import pandas as pd
from requests import adapters
from urllib3 import Retry
import csv


def get_datasette_http():
    """
    Function to return http session for querying Datasette 
    with retry strategy for larger queries
    """
    retry_strategy = Retry(total=3, status_forcelist=[400], backoff_factor=0)
    adapter = adapters.HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http

def get_datasette_query(
    db, sql, filter=None, url="https://datasette.planning.data.gov.uk"
):
    """
    Execute a query on Datasette and return results as a DataFrame
    """
    url = f"{url}/{db}.json"
    params = {"sql": sql, "_shape": "array", "_size": "max"}
    if filter:
        params.update(filter)
    try:
        http = get_datasette_http()
        resp = http.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.warning(e)
        return None

def convert_multipolygon_to_geojson(multipolygon_str):
    """Convert WKT MULTIPOLYGON to GeoJSON format"""
    try:
        #   WKT to Shapely geometry
        geometry = wkt.loads(multipolygon_str)
        
        # Convert to GeoJSON-compatible dictionary
        return {
            "type": "MultiPolygon", 
            "coordinates": [list(geometry.geoms)]
        }
    except Exception as e:
        print(f"Conversion error: {e}")
        return None

def get_types_and_check_overlap(heritage_categories):

    geojson_url = "https://services-eu1.arcgis.com/ZOdPfBS3aqqDYPUQ/arcgis/rest/services/Delisted/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"

    all_overlapping_entities = []
    
    try:
        geojson_response = requests.get(geojson_url)
        print(f"GeoJSON Response Status: {geojson_response.status_code}")

        if geojson_response.status_code == 200:
            geojson_data = geojson_response.json()
            
            heritage_data = {}
            for feature in geojson_data["features"]:
                heritage_description = feature["properties"].get("HERITAGECATEGORYDESCRIPTION")
                
                if heritage_description in heritage_categories:
                    if heritage_description not in heritage_data:
                        heritage_data[heritage_description] = []
                    
                    obj_geo = feature["geometry"]
                    article_version_name = feature["properties"].get("ARTICLEVERSIONNAME")
                    obj_id = feature["properties"].get("OBJECTID")

                    if obj_geo and article_version_name:
                        heritage_data[heritage_description].append({
                            'geometry': shape(obj_geo),
                            'article_version_name': article_version_name,
                            'object_id': obj_id
                        })

            # Check overlaps for each heritage category
            for category, datasette_database in heritage_categories.items():
                sql = """
                select
                  dataset,
                  end_date,
                  entity,
                  entry_date,
                  geojson,
                  geometry,
                  json,
                  name,
                  organisation_entity,
                  point,
                  prefix,
                  reference,
                  start_date,
                  typology
                from
                  entity
                order by
                  entity
                """
                
                datasette_data = get_datasette_query(datasette_database, sql)
                
                if datasette_data:
                    # Compare geometries for overlaps
                    for record in datasette_data:
                        geometry_json = record.get("geometry")
                        
                        if geometry_json:
                            try:
                                geojson_geometry = convert_multipolygon_to_geojson(geometry_json)
                                
                                if geojson_geometry:
                                    dataset_geometry = shape(geojson_geometry)
                                    
                                    # Check overlap with previously collected heritage data
                                    for heritage_category, heritage_items in heritage_data.items():
                                        for heritage_item in heritage_items:
                                            if dataset_geometry.intersects(heritage_item['geometry']):
                                                print(f"OVERLAP detected for {record.get('entity', 'Unknown Entity')} with {heritage_category}")
                                                print(f"Overlapping Heritage: {heritage_item['article_version_name']}")
                                                all_overlapping_entities.append({
                                                    'entity': record["entity"],
                                                    'entity_geo' : record["geometry"],
                                                    'dataset': datasette_database,
                                                    'overlapping_heritage_category': heritage_category,
                                                    'heritage_article_version_name': heritage_item['article_version_name'],
                                                    'heritage_object_id': heritage_item['object_id'],
                                                    'heritage_geo': heritage_item['geometry']
                                                })
                                                break
                            except Exception as geo_err:
                                print(f"Geometry Processing Error: {geo_err}")

    except Exception as e:
        print("An error occurred:", e)
        return None

    return all_overlapping_entities

def main():
    heritage_categories = {
        'Listing': 'listed-building',
        'Scheduling': 'scheduled-monument',
        'Park and Garden': 'park-and-garden',
        'Wreck': 'protected-wreck-site'
    }

    overlapping_entities = get_types_and_check_overlap(heritage_categories)

    if overlapping_entities:
        print("Overlapping Entities:", overlapping_entities)
    else:
        print("No overlaps detected.")



    csv_filename = 'heritage_overlaps.csv'

    headers = ['entity', 'entity_geo', 'dataset', 'overlapping_heritage_category', 'heritage_article_version_name', 'heritage_object_id', 'heritage_geo']

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.DictWriter(csvfile, fieldnames=headers)
        
        csv_writer.writeheader()
        
        csv_writer.writerows(overlapping_entities)

if __name__ == "__main__":
    main()