import sys

from pdbc.trafodion.connector import Connect


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

    out = Connect(**config)

    cur = out.cursor()
    #cur.execute("create table test_type(id int,id2 SMALLINT , id3 largeint, de decimal(10,5),varchar1 varchar(100))")
    #cur.execute("insert into songsong values('testint', 'testint' , ?)", [223])
    cur.execute("select * from songsong ")
    rs = cur.fetchone()
    while rs:
        print(rs)
        rs = cur.fetchone()
#
    print(rs)