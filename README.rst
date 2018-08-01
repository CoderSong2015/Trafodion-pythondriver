.. image:: https://img.shields.io/badge/license-Apache2.0-blue.svg
    :target: https://github.com/CoderSong2015/Trafodion-pythondriver/blob/master/LICENSE

.. image:: https://badge.fury.io/py/PyTrafodion.svg
    :target: https://badge.fury.io/py/PyTrafodion


Python Driver of Apache Trafodion
=================================
About Apache Trafodion
----------------------
https://trafodion.apache.org/

::

Apache Trafodion is a webscale SQL-on-Hadoop solution enabling transactional or operational workloads on Apache Hadoop.

The name "Trafodion" (the Welsh word for transactions, pronounced "Tra-vod-eee-on") was chosen specifically to emphasize the differentiation that Trafodion provides in closing a critical gap in the Hadoop ecosystem.

Trafodion builds on the scalability, elasticity, and flexibility of Hadoop. Trafodion extends Hadoop to provide guaranteed transactional integrity, enabling new kinds of big data applications to run on Hadoop.



About driver
------------
    Implements the Python Database API Specification v2.0 (PEP-249)
    This is a Pure-Python Driver of Apache Trafodion

Requirement
-----------

* Python -- one of the following:

  - CPython_ : >= 3.4
  - PyPy_ : Not support(havn't try)

* Trafodion Server -- one of the following:

  - Trafodion_ >= 2.3

* Python Module:
    cryptography >=2.3 (You can still use 2.2.2 although it has a known security vulnerability. Python-driver doesn't use related functions)
.. _CPython: https://www.python.org/
.. _PyPy: https://pypy.org/
.. _Trafodion: https://trafodion.apache.org/

Installation
------------


You can install it with pip::

    $ python3 -m pip install PyTrafodion


Example
-------
    * TODO

Resources
---------

* DB-API 2.0: http://www.python.org/dev/peps/pep-0249

* Apache Trafodion： https://trafodion.apache.org/

License
-------
PyTrafodion is released under the Apache License. See LICENSE for more information.

Contract me
-----------
   email: haolin.song@esgyn.cn