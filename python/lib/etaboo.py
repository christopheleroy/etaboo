import re, sets

class HeaderFinder(object):
    """The HeaderFinder holds the specifications for how to identify a header in tabular data.
       When a header if found, a ColumnMapper is returned"""

    def __init__(self,specs):
        if(specs is None or specs == "*"):
            specs = {"rowMax":20, "columns":"*"}

        self.searchRowMax = (specs['rowMax'] if(specs.has_key('rowMax'))else 20)
        self.searchRowMin = (specs['skipRows'] if(specs.has_key('skipRows')) else 0)
        self.columnSpecs = specs['columns']
        mode = specs['mode'] if(specs.has_key('mode'))else "i"

        if(self.columnSpecs == "*"):
            self.useFirstRow = True
            self.compiledSpecs = None
        else:
            self.useFirstRow = False
            self.compiledSpecs = []
            for c in self.columnSpecs:
                try:
                    cmode = mode if (type(c) is not dict) else (c['mode'] if(c.has_key('mode')) else (c['m'] if(c.has_key('m'))else mode))
                    self.compiledSpecs.append( ColumnFinder(c, len(self.compiledSpecs), cmode) )
                except:
                    print "Column Spec cannot be parsed: "
                    print c
                    raise



    def findHeader(self, rowIndex, row):


        if(rowIndex> self.searchRowMax): return None
        if(rowIndex< self.searchRowMin): return None

        if(self.useFirstRow and self.compiledSpecs is None):
            colSpecs = []
            avoidDups = set()
            for i in range(len(row)):
                if(len(row[i])>0):
                    if(row[i] not in avoidDups):
                        colSpecs.append( ColumnFinder(row[i], len(colSpecs), "=") )
                        avoidDups.add(row[i])

            if(len(colSpecs)>0):
                self.compiledSpecs = colSpecs
            else:
                return None

        if(self.compiledSpecs is None):
            return None


        cols = []
        for c in self.compiledSpecs:
            hits = filter(lambda i: c.isMatch(row[i]), range(len(row)))
            ok = (len(hits)==1) or (len(hits)==0 and c.isOptional())
            cols.append( (ok, hits, c) )
        not_ok = filter(lambda t3: not t3[0], cols)

        if(len(not_ok)==0):
            return ColumnMapper(cols)
        mults = filter(lambda t3: len(t3[1])>1, cols)
        if(len(mults)>0):
            print "Row {0} (index {1}) has multiple hits...".format(rowIndex+1, rowIndex)
            for m in mults:
                print m[2].name + " ==> "
                print map(lambda i: "{0}: {1}".format(i, row[i]), m[1])

        return None










class ColumnFinder(object):


    def __init__(self, specs, position, mode="ic"):
        name,title = None,None
        if(type(specs) is str):
            name,title = specs,specs
        elif(specs.has_key('name')):
            name = specs['name']
            if(specs.has_key('title')):
                title = specs['title']
            else:
                title = name

        self.columnName = name
        self.columnTitle = title
        self.beExact = ("=" in mode)
        self.beCaseSensitive = not ("i" in mode)
        self.checkContains = "c" in mode
        self.optional = "o" in mode

        if(not "x" in mode):
            title = title.replace("*", ".*")

        if(self.beCaseSensitive):
            self.pattern = re.compile(title)
        else:
            self.pattern = re.compile(title, re.IGNORECASE)

        if(type(specs) is dict and specs.has_key('pos')):
            position = specs['pos']
        if(type(position) is not int):
            raise ValueError("position must be an int")
        self.position = position


    def isMatch(self, strung):
        if(self.beExact):
            return (strung == self.columnTitle)
        elif(self.checkContains):
            return self.pattern.search(strung) is not None
        else:
            return self.pattern.match(strung) is not None

    def isOptional(self):
        return self.optional




class ColumnMapper(object):
    """A ColumnMapper is created by a HeaderFinder and is helpful in parsing the data rows into a well defined object"""



    def __init__(self, headerFindings):
        assignments = map(lambda f:  (f[2].position, f[2].columnName, f[1][0]), headerFindings)
        self.assignments = sorted( assignments, key=lambda t3: t3[0])
        # if some column position are unexpected, this is a bit off - we'll use less than optimal assignments methods
        off = (filter( lambda i: self.assignments[i][0] != i, range(len(self.assignments))))
        self.off = (len(off)>0)


    def mapArray(self, row):
        if(self.off):
            raise Error("column mapper is off and mapArray isn't supported yet in that case ...")
        return map( (lambda ass: "" if(len(row)<=ass[2]) else row[ass[2]]), self.assignments)

    def mapObject(self,row, obj=None):
        m = {} if(obj is None) else obj.copy()
        for asi in self.assignments:
            m[asi[1]] = "" if(len(row)<=asi[2])else row[asi[2]]
        return m





class TableParser(object):

    def __init__(self, specs):
        self.headSpecs = specs['header']
        self.headerFinder = HeaderFinder(specs['header'])


    def findMapper(self, table):
        searchMax = len(table) if self.headerFinder.searchRowMax > len(table) else self.headerFinder.searchRowMax

        mapper = None
        start_data = 0
        for i in range(searchMax):
            mapper = self.headerFinder.findHeader(i, table[i])
            start_data = i+1
            if(mapper):
                break

        return (mapper, start_data)

    def parse(self, table, asArray = False):

        mapper, start_data = self.findMapper(table)
        if(mapper is not None):
            lama = lambda i: mapper.mapObject(table[i])
            if(asArray):
                lama = lambda i: mapper.mapArray(table[i])
            outs = map(lama, range(start_data, len(table)))
            return outs

        return None


    def reset(self):
        self.headerFinder = HeaderFinder(self.headSpecs)



class DbAdapter(object):
    def __init__(self, conn, dbtype):
        self.dbtype=dbtype
        self.conn = conn

    @staticmethod
    def fromConnectionJson(connJson):
        if(connJson['type'] == 'pg'):
            import psycopg2
            conn = psycopg2.connect(**connJson)
            return DbAdapter(conn, 'std')
        if(connJson['type'] == 'sl'):
            import sqlite3
            conn = sqlite3.connect(connJson['file'])
            return DbAdapter(conn, 'std')
        return None


    def commit(self):
        self.conn.commit()


    def select(self, statement):
        if(self.dbtype=='std'):
            return self.select_std(statement)
        else:
            return self.select_ms(statement)

    def insert(self, statement, tuples):
        if(self.dbtype=='std'):
            return self.insert_std(statement, tuples)
        else:
            return self.insert_ms(statement, tuples)

    def update(self, statement, tuples):
        if(self.dbtype=='std'):
            return self.update_std(statement, tuples)
        else:
            return self.update_ms(statement, tuples)

    def _fetchall_(self, cursive, do_close=True):
        outs =[]
        for r in cursive.fetchall():
            outs.append(r)
        if(do_close): cursive.close()
        return outs

    def select_std(self, statement):
        cursive = self.conn.cursor()
        cursive.execute(statement)
        return self._fetchall_(cursive, True)

    def select_ms(self, statement):
        cursive = self.conn.cursor()
        cursive.execute(statement)
        return self._fetchall_(cursive, False)

    def update_std(self, statement, tuples):
        cursive = self.conn.cursor()
        cursive.executemany(statement, tuples)
        cursive.close()

    def update_ms(self, statement, tuples):
        self.update_std(satement, tuples)

    def insert_std(self, statement, tuples):
        self.update_std(statement,tuples)

    def insert_ms(self, statement,tuples):
        self.update_ms(statement, tuples)

class DbUpdater(object):

    def __init__(self, specs):
        self.sqlSelect = specs['select']
        self.sqlSelectColumns = specs['select-columns']
        self.key = specs['key']
        self.sqlInsert = specs['insert']
        self.sqlInsertColumns = specs['insert-columns']
        self.sqlUpdate = specs['update']
        self.sqlUpdateColumns = specs['update-columns']
        self.singleKey = (type(self.key) is str)
        self.silence = False

        badKeys = []
        cols = self.sqlSelectColumns
        keys = list(self.key) if(not type(self.key) is str) else [ self.key ]
        badKeys = filter(lambda k: not k in cols, keys)
        if(len(badKeys)>0):
            raise ValueError("the defined key is not entirely contained in the select-columns: " + str(badKeys))

        badCols = filter(lambda k: not k in cols, self.sqlUpdateColumns)
        if(len(badCols)>0):
            raise ValueError("the update-columns are not all found in select-columns: " + str(badCols))



        keyIndices = map(lambda k: cols.index(k), list(self.key))
        self.keyIndices = keyIndices
        self.updateIndices = map(lambda k: cols.index(k), list(self.sqlUpdateColumns))


    def mkKey(self, obj):
        if(type(obj) is list or type(obj) is tuple):
            ln = len(obj)
            return tuple( map(lambda i: obj[i] if(ln>i)else "", self.keyIndices) )

        if(self.singleKey):
            return obj[ self.key ]
        else:
            return tuple( map(lambda k: obj[k] if(obj.has_key(k)) else "", self.key) )



    def synchronize(self, dbadaptor, dataset):
        inx = {}

        for r in dbadaptor.select(self.sqlSelect):
            k = self.mkKey(r)
            inx[k] = r

        # import pdb; pdb.set_trace()
        newData = filter(lambda obj: not inx.has_key( self.mkKey(obj) ), dataset)
        currData = filter(lambda obj: inx.has_key(self.mkKey(obj)), dataset)

        insertTuples = []
        updateTuples = []
        if(self.sqlInsert is not None):
            cols = self.sqlInsertColumns
            for d in newData:
                t = tuple( map(lambda k: d[k] if(d.has_key(k)) else "", cols) )
                insertTuples.append(t)

        if(self.sqlUpdate is not None):
            cols = self.sqlUpdateColumns

            for d in currData:
                dbd = inx[ self.mkKey(d) ]
                dbtuple = tuple( map(lambda i: dbd[i], self.updateIndices))
                dctuple = tuple( map(lambda k: d[k], cols))
                if(not dbtuple == dctuple):
                        updateTuples.append(dctuple)

        if(len(updateTuples)>0):
            if(not self.silence): print "Update {0} tuples for '{1}'".format(len(updateTuples), self.sqlUpdate)
            dbadaptor.update(self.sqlUpdate, updateTuples)

        if(len(insertTuples)>0):
            if(not self.silence): print "Insert {0} tuples for '{1}'".format(len(insertTuples), self.sqlInsert)
            dbadaptor.insert(self.sqlInsert, insertTuples)

        if(len(updateTuples)+len(insertTuples)>0):
            dbadaptor.commit()
        else:
            if(not self.silence): print "No records found to insert or update ..."
