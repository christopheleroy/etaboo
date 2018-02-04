
import etaboo
import argparse, json, csv

parser = argparse.ArgumentParser()
parser.add_argument("config", nargs=1, help="configuration JSON file")
parser.add_argument("--stream", nargs="?", help='the stream name for the files to be loaded', default="missing-please-supply")
parser.add_argument("--files", nargs="*", help='the list of files to be loaded, in the order to be loaded')
parser.add_argument("--password", nargs="?", help='the database password to use (required if not in the configuration file)')
parser.add_argument("--passvar", nargs="?", help="the environment variable containing the db password if you don't want to pass it on the command line or have it in the configuration file...")

args = parser.parse_args()
# THIS SCRIPT IS COMMITTED in GitHub WITHOUT ANY TESTING YET !

# import pdb; pdb.set_trace()
with open(args.config[0], "rb") as configF:
    config = json.load(configF)
    my_stream = filter(lambda x: x.has_key("stream-name") and x["stream-name"] == args.stream, config)
    if(len(my_stream)==1):
        dbdetails = my_stream[0]['db'].copy()
        if(args.passvar is not None):
            dbdetails['password'] = process.env[ args.passvar ]
        elif(args.password is not None):
            dbdetails['password'] = args.password

        dbadaptor = etaboo.DbAdapter.fromConnectionJson(dbdetails)
        master = etaboo.DbUpdater(my_stream[0].copy())

        tbParser = etaboo.TableParser(my_stream[0]["parser"].copy())

        for f in args.files:
            with open(f, 'rb') as ff:
                tble = []
                for row in  csv.reader(ff):
                    tble.append(row)
                tblData = tbParser.parse(tble)
                if(tblData is None):
                    print "file could not be parsed: {0}".format(f)
                    continue
                master.synchronize(dbadaptor, tblData)
    else:
        print "Cannot find stream {0}".format(args.stream)
