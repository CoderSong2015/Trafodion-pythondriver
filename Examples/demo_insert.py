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
cursor.execute('insert into zhanghan values(17)')
cnx.close()

cnx = connector.connect(**config)
cursor = cnx.cursor()
cursor.execute('insert into zhanghan values(?)', [12,])
cnx.close()


cnx = connector.connect(**config)
cursor = cnx.cursor()
batch_data = (
    (22,),
    (25,),
    (27,),
)
cursor.executemany('insert into zhanghan values(?)', batch_data)
cnx.close()
