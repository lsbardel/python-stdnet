import datetime
from stdnet import test
from stdnet.utils import populate, zip, range

from examples.models import SportAtDate

NUM_DATES = 200

dates = populate('date',NUM_DATES,
                 start=datetime.date(2005,6,1),
                 end=datetime.date(2010,6,6))
sports = ['football','rugby','swimming','running','cycling']
groups = populate('choice',NUM_DATES,choice_from=sports)


class TestOrdering(test.TestCase):
    
    def setUp(self):
        self.orm.register(SportAtDate)
    
    def fill(self):
        with SportAtDate.transaction() as t:
            for n,d in zip(groups,dates):
                SportAtDate(name = n, dt = d).save(t)
                
    def testMeta(self):
        self.assertEqual(SportAtDate._meta.order_by.name,'dt')
        
    def testAdd(self):
        self.fill()
        self.assertEqual(SportAtDate.objects.all().count(),NUM_DATES)