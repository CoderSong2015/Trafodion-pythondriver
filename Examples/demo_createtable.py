from pdbc.trafodion import connector


#eg 1
config = {
        'host': '192.168.0.128',
        'port': 23400,
        'schema': 'seabase',
        'user': 'db__root',
        'password': 'traf123',
        'charset': 'utf-8',
        'use_unicode': True,
        'get_warnings': True
    }

cnx = connector.connect(**config)
cursor = cnx.cursor()
cursor.execute('create table zhanghan(id int)')
cnx.close()

