import os
from datetime import datetime
import json
import tempfile
import unittest
import operator

from shapely.geometry import Polygon
from stac_validator import stac_validator

from datasources import Manifest
from datasources.stac.query import STACQuery
from PlanetData import PlanetData


class PlanetDataTestCases(unittest.TestCase):

    def setUp(self):
        self.spatial_mode = 'geometry'
        self._setUp()
        self.manifest = Manifest()
        self.name = self.__class__.__name__.replace('TestCases', '')
        self.manifest.update({self.name: self.datasource(self.manifest)})
        self.spatial_geom = Polygon(self.spatial['coordinates'][0])

    def check_properties(self, asset, properties):
        for item in properties:
            equality = next(iter(properties[item]))
            comparison_operator = getattr(operator, equality)
            if not comparison_operator(asset[item], properties[item][equality]):
                return False
        return True

    def _setUp(self):
        self.datasource = PlanetData
        self.spatial = {
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
        self.temporal = ("2017-10-20", "2017-10-30")
        self.properties = {'eo:cloud_cover': {'lt': 30}}
        self.limit = 10

    def test_pattern(self):
        # Testing that datasource implements proper pattern
        for source in self.manifest.sources:
            self.assertTrue(hasattr(source, 'execute'))
            self.assertTrue(hasattr(source, 'search'))
            self.assertTrue(hasattr(source, 'tags'))
            self.assertTrue(hasattr(source, 'stac_compliant'))

    def test_spatial_search(self):
        self.manifest.flush()
        self.manifest[self.name].search(self.spatial, subdatasets=['SkySatScene'])
        response = self.manifest.execute()

        # Buffering the input geometry to account for small discrepencies in S2 (especially with large area searches)
        # This test passes if all returned geometries are within 3% of the average length of the polygon.
        buffered_geom = self.spatial_geom.buffer(0.03 * self.spatial_geom.length / 4)

        # Confirming that each output feature intersects input
        for feat in response[self.name]['features']:
            if self.spatial_mode == 'geometry':
                asset_geom = Polygon(feat['geometry']['coordinates'][0])
            elif self.spatial_mode == 'extent':
                asset_geom = Polygon([[feat['bbox'][0], feat['bbox'][3]],
                                      [feat['bbox'][2], feat['bbox'][3]],
                                      [feat['bbox'][2], feat['bbox'][1]],
                                      [feat['bbox'][0], feat['bbox'][1]],
                                      [feat['bbox'][0], feat['bbox'][3]]])

            self.assertTrue(asset_geom.intersects(buffered_geom))

    def test_temporal_search(self):
        self.manifest.flush()
        self.manifest[self.name].search(self.spatial, self.temporal, subdatasets=['SkySatScene'])

        response = self.manifest.execute()
        query = STACQuery(self.spatial, self.temporal)

        # Confirming that each output feature is within temporal window
        for feat in response[self.name]['features']:
            if len(feat['properties']['datetime']) == 10:
                year, month, day = feat['properties']['datetime'].split('-')
            else:
                year, month, day = feat['properties']['datetime'].split('T')[0].split('-')

            date_time = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")

            self.assertTrue(query.check_temporal(date_time))

    def test_properties_search(self):
        self.manifest.flush()
        self.manifest[self.name].search(self.spatial, properties=self.properties, subdatasets=['SkySatScene'])
        response = self.manifest.execute()
        print(json.dumps(response['PlanetData']['features'][0], indent=2))

        # Confirming that output features ars filtered properly
        for feat in response[self.name]['features']:
            self.assertTrue(self.check_properties(feat['properties'], self.properties))

    def test_subdataset_search(self):
        self.manifest.flush()
        self.manifest[self.name].search(self.spatial, properties=self.properties, subdatasets=['SkySatScene'])
        response = self.manifest.execute()
        for feat in response[self.name]['features']:
            self.assertEqual(feat['properties']['legacy:item_type'], 'SkySatScene')

        self.manifest.flush()
        self.manifest[self.name].search(self.spatial, properties=self.properties, subdatasets=['PSScene4Band'])
        response = self.manifest.execute()
        for feat in response[self.name]['features']:
            self.assertEqual(feat['properties']['legacy:item_type'], 'PSScene4Band')

    # def test_limit(self):
    #     # Confirming that the limit kwarg works
    #     self.manifest.flush()
    #     self.manifest[self.name].search(self.spatial, limit=self.limit, subdatasets=['SkySatScene'])
    #     response = self.manifest.execute()
    #     self.assertLessEqual(len(response[self.name]['features']), self.limit)
    #
    # def test_stac_compliant(self):
    #     self.manifest.flush()
    #     self.manifest[self.name].search(self.spatial, subdatasets=['SkySatScene'])
    #     response = self.manifest.execute()
    #
    #
    #     # Confirming that output features are STAC-compliant
    #     for feat in response[self.name]['features']:
    #         print(json.dumps(feat, indent=2))
    #
    #         fd, path = tempfile.mkstemp()
    #         try:
    #             with os.fdopen(fd, 'w') as tmp:
    #                 json.dump(feat, tmp)
    #
    #             stac = stac_validator.StacValidate(path)
    #             stac.run()
    #             try:
    #                 print(stac.message)
    #                 self.assertEqual(stac.status['items']['valid'], 1)
    #             except:
    #                 # TODO: figure out why this error happens
    #                 if 'Unresolvable JSON pointer' in stac.message[0]['error_message']:
    #                     pass
    #                 else:
    #                     raise
    #
    #         finally:
    #             os.remove(path)
