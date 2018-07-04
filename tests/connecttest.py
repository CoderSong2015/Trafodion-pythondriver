import sys
sys.path.append("..")

import pdbc.trafodion.connector


if __name__ == '__main__':

    config = {
        'host': '192.168.0.59',
        'port': 23400,
        'database': 'seabase',
        'user': 'db__root',
        'password': '123456',
        'charset': 'utf-8',
        'use_unicode': True,
        'get_warnings': True,
    }

    out = pdbc.trafodion.connector.connect(**config)

    cur = out.cursor()
    cur.execute("select * from songsong")
    rs = cur.fetchone()
    while rs:
        print(rs)
        rs = cur.fetchone()

    print(rs)
