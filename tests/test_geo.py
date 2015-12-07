# coding: utf-8
# Copyright 2015 Jeethu Rao
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from twisted.internet import defer
from twisted.trial import unittest

from .mixins import REDIS_HOST, REDIS_PORT, RedisGeoCheckMixin

import txredisapi as redis


class TestGeo(unittest.TestCase, RedisGeoCheckMixin):
    _KEY = '_geo_test_key'

    @defer.inlineCallbacks
    def setUp(self):
        self.db = yield redis.Connection(REDIS_HOST, REDIS_PORT,
                                         reconnect=False)
        self.redis_geo_support = yield self.has_geo()
        yield self.db.delete(self._KEY)

    @defer.inlineCallbacks
    def tearDown(self):
        yield self.db.delete(self._KEY)
        yield self.db.disconnect()

    def _do_geoadd(self):
        return self.db.geoadd(self._KEY,
                              [(13.361389, 38.115556, "Palermo"),
                               (15.087269, 37.502669, "Catania")])

    @defer.inlineCallbacks
    def test_geoadd(self):
        self._skipCheck()
        r = yield self._do_geoadd()
        self.assertEqual(r, 2)
        r = yield self.db.zcard(self._KEY)
        self.assertEqual(r, 2)

    @defer.inlineCallbacks
    def test_geohash(self):
        self._skipCheck()
        yield self._do_geoadd()
        r = yield self.db.geohash(self._KEY, ('Palermo', 'Catania'))
        self.assertEqual(len(r), 2)
        self.assertIn("sqc8b49rny0", r)
        self.assertIn("sqdtr74hyu0", r)

    @defer.inlineCallbacks
    def test_geopos(self):
        self._skipCheck()
        yield self._do_geoadd()
        r = yield self.db.geopos(self._KEY,
                                 ('Palermo', 'Catania', 'NonExisting'))
        self.assertEqual(r,
                         [[13.361389338970184, 38.1155563954963],
                          [15.087267458438873, 37.50266842333162],
                          None])

    @defer.inlineCallbacks
    def test_geodist(self):
        self._skipCheck()
        yield self._do_geoadd()
        r = yield self.db.geodist(self._KEY, ('Palermo', 'Catania'))
        self.assertAlmostEqual(r, 166274.15156960033)
        r = yield self.db.geodist(self._KEY, ('Palermo', 'Catania'),
                                  unit='km')
        self.assertAlmostEqual(r, 166.27415156960032)
        r = yield self.db.geodist(self._KEY, ('Palermo', 'Catania'), 'mi')
        self.assertAlmostEqual(r, 103.31822459492733)
        r = yield self.db.geodist(self._KEY, ('Foo', 'Bar'))
        self.assertIs(r, None)

    @defer.inlineCallbacks
    def test_georadius(self):
        self._skipCheck()
        yield self._do_geoadd()
        r = yield self.db.georadius(self._KEY, 15, 37, 200, 'km', withdist=True)
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0][0], 'Palermo')
        self.assertAlmostEqual(r[0][1], 190.4424)
        self.assertEqual(r[1][0], 'Catania')
        self.assertAlmostEqual(r[1][1], 56.4413)
        r = yield self.db.georadius(self._KEY, 15, 37, 200, 'km',
                                    withcoord=True)
        self.assertEqual(r,
                         [[u'Palermo',
                           [13.361389338970184, 38.1155563954963]],
                          [u'Catania',
                           [15.087267458438873, 37.50266842333162]]])
        r = yield self.db.georadius(self._KEY, 15, 37, 200, 'km',
                                    withdist=True, withcoord=True)
        self.assertEqual(r,
                         [[u'Palermo', 190.4424,
                           [13.361389338970184, 38.1155563954963]],
                          [u'Catania', 56.4413,
                           [15.087267458438873, 37.50266842333162]]])

    @defer.inlineCallbacks
    def test_georadiusbymember(self):
        self._skipCheck()
        r = yield self.db.geoadd(self._KEY,
                                 [(13.583333, 37.316667, "Agrigento")])
        self.assertEqual(r, 1)
        r = yield self.db.geoadd(self._KEY, [(13.361389, 38.115556, "Palermo"),
                                             (15.087269, 37.502669, "Catania")])
        self.assertEqual(r, 2)
        r = yield self.db.georadiusbymember(self._KEY, "Agrigento", 100, "km")
        self.assertEqual(r, ["Agrigento", "Palermo"])
