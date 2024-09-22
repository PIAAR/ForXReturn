import pymongo
from urllib.parse import quote_plus

username = quote_plus('rjmoore')
password = quote_plus('Anubis2030')
cluster = 'dashxbot'
authSource = '<authSource>'
authMechanism = '<authMechanism>'

uri = 'mongodb+srv://' + username + ':' + password + '@' + cluster + '/?authSource=' + authSource + '&authMechanism=' + authMechanism

client = pymongo.MongoClient(uri)

result = client["<dbName"]["<collName>"].find()

# print results
for i in result:
    print(i)
