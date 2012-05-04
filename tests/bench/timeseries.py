__benchmark__ = True
from datetime import datetime, date

from stdnet import odm, test
from stdnet.utils import convert_bytes

from examples.data import hash_data

BENCHMARK_TEMPLATE = '{0[test]}\nRepeated {0[number]} times.\
 Average {0[mean]} secs, Stdev {0[std]}, Bytes sent {0[bytes]},\
 Bytes recv {0[resp_bytes]}.\n----------------------------------------------\
-----------------------------------------------------'


class TestCase(test.TestCase):
    @classmethod
    def setUpClass(cls):
        size = cls.worker.cfg.size
        cls.data = hash_data(size = size, fieldtype = 'date')
        
    def setUp(self):
        self.backend.load_scripts()
        

######### Create TEST CASES

class CreateTest(TestCase):
    '''Create the timeseries'''
    benchmark_template = BENCHMARK_TEMPLATE
    
    def startUp(self):
        session = self.session()
        # start the transaction
        self.transaction = session.begin()
        self.ts = ts = session.add(odm.TS())
        self.ts.update(self.data.items())
        self.assertTrue(self.ts.cache.toadd)
        
    def testCommit(self):
        self.ts.session.commit()
        
    def getInfo(self, delta, dt, info):
        cmd = self.transaction.commands
        raw = cmd['raw_command']
        resp_raw = cmd['request'].raw_response
        if 'bytes' not in info:
            info['bytes'] = 0
            info['resp_bytes'] = 0
        info['bytes'] += len(raw)
        info['resp_bytes'] += len(resp_raw)
    
    def getSummary(self, number, total_time, total_time2, info):
        info['bytes'] = convert_bytes(float(info['bytes'])/number)
        info['resp_bytes'] = convert_bytes(float(info['resp_bytes'])/number)
        return info