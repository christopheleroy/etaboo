
import etaboo
import unittest

class TestParsing(unittest.TestCase):

    def tableMaker(self):

        zoo = {}

        zoo["bear"]= [
            "firstName,lastName,dob,tel",
            "Pablo,Escobar,1949-12-01,none",
            "Maria,Henao,1961-06-30,+44-131-8981",
            "Sebastian,Marroquin,1977-06-30,+56-131-5461",
            "Helmer,Herrera,1951-08-24,none"
        ]
        zoo["bear"] = map(lambda x: x.split(","), zoo["bear"])

        return zoo

    def test_parsing_straightforward(self):
        pp = etaboo.TableParser({"header":{"columns":["firstName","lastName","dob"]}})

        zoo = self.tableMaker()

        pp0 = pp.parse(zoo['bear'])
        pp1 = pp.parse(zoo['bear'], True)

        self.assertEquals(len(pp0), len(zoo['bear'])-1)
        self.assertEquals(len(pp0), len(pp1))
        self.assertTrue( pp0[0].has_key('lastName') )
        self.assertEquals( pp0[0]['lastName'], zoo['bear'][1][1])
        self.assertEquals( pp0[0]['dob'], pp1[0][2])

    def test_parsing_name_remap_order_change(self):
        # pp0 isn't respecting the straight order of columns
        pp0 = etaboo.TableParser({"header":{"columns":["lastName","firstName","dob"]}})
        # pp1 is specifying column name changes, and a partial match for header name
        pp1 = etaboo.TableParser({"header":{
            "columns": [
                {"name":"fn", "title":"first"},
                {"name":"ln", "title":"last"},
                "dob"
            ]
        }});

        zoo = self.tableMaker()

        tp0 = pp0.parse(zoo['bear'])
        tp1 = pp1.parse(zoo['bear'])

        self.assertEquals(len(tp0), len(zoo['bear'])-1)
        self.assertEquals(len(tp0), len(tp1))

        self.assertTrue(tp0[0].has_key('firstName'))
        self.assertTrue(tp1[0].has_key('fn'))
        self.assertEquals(tp0[0]['firstName'], tp1[0]['fn'])

        self.assertEquals( tp1[0]['ln'], zoo['bear'][1][1])
        self.assertEquals( tp1[0]['ln'], tp0[0]['lastName'])

        tp2 = pp0.parse(zoo['bear'], asArray=True)
        self.assertEquals(len(tp2), len(tp1))
        self.assertEquals( tp2[0][1], zoo['bear'][1][0])
        self.assertEquals( tp2[1][1], zoo['bear'][2][0])
        self.assertEquals( tp2[2][1], zoo['bear'][3][0])
        for i in range(len(tp2)):
            self.assertEquals(tp2[i][0], tp1[i]['ln'])
            self.assertEquals(tp2[i][1], tp1[i]['fn'])
            self.assertEquals(tp2[i][2], tp1[i]['dob'])


    def test_parsing_star(self):
        pp0 = etaboo.TableParser({"header":"*"})
        pp1 = etaboo.TableParser({"header":{
            "columns": [
                {"name":"fn", "title":"first"},
                {"name":"ln", "title":"last"},
                "dob"
            ]
        }});

        zoo = self.tableMaker()
        # import pdb; pdb.set_trace()
        tp0 = pp0.parse(zoo['bear'])
        tp1 = pp1.parse(zoo['bear'])

        self.assertEquals(len(tp0), len(tp1))
        for i in range(len(tp0)):
            for cc in [ ('fn', 'firstName'), ('ln', 'lastName'), ('dob', 'dob')]:
                self.assertEquals(tp0[i][cc[1]], tp1[i][cc[0]])

        maria0 = filter(lambda x: x['dob'] == '1961-06-30', tp0)
        maria1 = filter(lambda x: x['dob'] == '1961-06-30', tp1)
        self.assertEquals(len(maria0)*len(maria1),1)
        self.assertEquals(maria0[0]['firstName'], 'Maria')
        self.assertEquals(maria1[0]['fn'], 'Maria')
