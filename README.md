Etaboo is (going to be) a suite of simple ETL tools in a few languages.

This shall serve as a little bit of training for beginners.


See the file ``python/etaboo-example-config.json`` where the configuration list streams (actually one).

You can load the stream with a command such as:
```
python etbSync.py etaboo-example-config.json --stream load-contacts --files ../db/load01.csv
```

The stream's configurations makes it explicit how to :
- parse the files and find columns so as to load the data in memory
- select the database data, and find the "key" for this data
- update the database data that has changed
- insert the database data that is missing from the database but found in the file.


In this specific example configuration:
- the ``parser`` section explains how to find the Header in your file and how to name internally the columns. For example, the column with a title header 'Identifier' will be internally represented as a property of property name 'id'.
- the ``select`` and ``select-columns`` explains how to find the data we're trying to maintain, with ``select`` being the SQL statement and ``select-columns`` contains the list of column names as in the list of property names of the ``parser`` section. The order of columns is to be respected.
- the ``key``  feature is what is crucial to help identify which records are already in the database (needing a change or none) and which are not
- the ``update`` and ``update-columns`` explain how to find the data that needs to be changed, and how to change it. Only the columns mentioned in the update are going to be checked. Here, you can see that we do not allow to change the contact's names, but we can change the phone number or email address.
- the ``insert`` and ``insert-columns`` explain how to insert new data
- the ``db`` property is explaining how to connect to the database. Use type 'sl' for sqllite, 'pg' for Postgres (untested), 'ms' for Microsoft SQL server (untested)  etc.


