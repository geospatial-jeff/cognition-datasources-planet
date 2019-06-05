import json
import os

import datetime

from datasources.stac.query import STACQuery
from datasources.stac.item import STACItem
from datasources.sources.base import Datasource

from planet import api
from planet.api import filters

pl_api_key = os.environ['PL_API_KEY']

client = api.ClientV1()

geometry = {
        "type": "Polygon",
        "coordinates": [
          [
            [
              -118.27331542968749,
              34.01738017414994
            ],
            [
              -118.18542480468751,
              34.01738017414994
            ],
            [
              -118.18542480468751,
              34.08678665571845
            ],
            [
              -118.27331542968749,
              34.08678665571845
            ],
            [
              -118.27331542968749,
              34.01738017414994
            ]
          ]
        ]
      }

field_types = {
    'datetime': str,
    'eo:cloud_cover': float,
    'eo:gsd': float,
    'eo:azimuth': float,
    'eo:sun_elevation': float,
    'eo:off_nadir': float,
    'eo:instrument': str,
    'eo:epsg': int
}

field_mappings = {
    'eo:cloud_cover': 'cloud_cover',
    'eo:gsd': 'gsd',
    'eo:azimuth': 'satellite_azimuth',
    'eo:sun_azimuth': 'sun_azimuth',
    'eo:sun_elevation': 'sun_elevation',
    'eo:off_nadir': 'view_angle',
    'eo:instrument': 'satellite_id',
    'eo:epsg': 'epsg_code'
}

stac_mappings = {
    'cloud_cover': 'eo:cloud_cover',
    'gsd': 'eo:gsd',
    'satellite_azimuth': 'eo:azimuth',
    'sun_azimuth': 'eo:sun_azimuth',
    'sun_elevation': 'eo:sun_elevation',
    'view_angle': 'eo:off_nadir',
    'satellite_id': 'eo:instrument',
    'epsg_code': 'eo:epsg'
}

class PlanetData(Datasource):

    stac_compliant = False
    tags = ['Raster', 'EO']

    def __init__(self, manifest):
        super().__init__(manifest)

    def search(self, spatial, temporal=None, properties=None, limit=10, **kwargs):
        stac_query = STACQuery(spatial, temporal)

        # Start with spatial as its always required
        planet_query = filters.geom_filter(stac_query.spatial)

        if temporal:
            temporal_query = filters.and_filter(
                filters.date_range('acquired', gt=stac_query.temporal[0]),
                filters.date_range('acquired', lt=stac_query.temporal[1]),
            )
            planet_query = filters.and_filter(
                planet_query,
                temporal_query
            )

        if properties:
            property_queries = []
            for (field_name,v) in properties.items():
                if field_name == 'eo:instrument':
                    continue
                # Handle for searching on legacy extension
                if field_name.startswith('legacy:'):
                    field_name = field_name.repace('legacy:','')
                equality = list(v)[0]
                args = {equality: v[equality]}

                planet_field = field_mappings[field_name]
                field_type = field_types[field_name]

                if field_type == str:
                    property_queries.append(
                        filters.string_filter(planet_field, *args)
                    )
                elif field_type == float:
                    property_queries.append(
                        filters.range_filter(planet_field, **args)
                    )

            planet_query = filters.and_filter(
                planet_query,
                *property_queries
            )

        planet_request = api.filters.build_search_request(planet_query, kwargs['subdatasets'])
        self.manifest.searches.append([self, planet_request])

    def execute(self, api_request):
        response = client.quick_search(api_request)
        content = json.loads(response.get_raw())

        stac_items = []
        for feat in content['features']:
            properties = {}
            for prop in feat['properties']:
                if prop in list(stac_mappings):
                    properties.update({stac_mappings[prop]: feat['properties'][prop]})
                else:
                    properties.update({f'legacy:{prop}': feat['properties'][prop]})

            # Update date field
            properties['datetime'] = properties.pop('legacy:acquired')

            # Calculate bbox
            xvals = [x[0] for x in feat['geometry']['coordinates'][0]]
            yvals = [y[1] for y in feat['geometry']['coordinates'][0]]

            stac_item = {
                'id': feat['id'],
                'properties': properties,
                'collection': feat['properties']['item_type'],
                'geometry': feat['geometry'],
                'bbox': [min(xvals), min(yvals), max(xvals), max(yvals)],
                'assets': {
                    'thumbnail': {
                        'href': feat['_links']['thumbnail'] + f"?api_key={pl_api_key}"
                    },
                    'assets': {
                        'href': feat['_links']['assets']
                    }
                },
                'links': {}
            }
            stac_items.append(stac_item)

        return stac_items

