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
cursor.execute('select * from zhanghan')
ans = cursor.fetchall()
print(ans)
cnx.close()

cnx = connector.connect(**config)
cursor = cnx.cursor()
cursor.execute('select * from zhanghan')
for ans in cursor: #(print row one by one)
    print(ans)
cnx.close()

