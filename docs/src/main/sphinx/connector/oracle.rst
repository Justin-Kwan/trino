================
Oracle connector
================

The Oracle connector allows querying and creating tables in an external Oracle
database. Connectors let Trino join data provided by different databases,
like Oracle and Hive, or different Oracle database instances.

Configuration
-------------

To configure the Oracle connector as the ``oracle`` catalog, create a file named
``oracle.properties`` in ``etc/catalog``. Include the following connection
properties in the file:

.. code-block:: text

    connector.name=oracle
    connection-url=jdbc:oracle:thin:@example.net:1521/ORCLCDB
    connection-user=root
    connection-password=secret

.. note::
    Oracle does not expose metadata comment via ``REMARKS`` column by default
    in JDBC driver. You can enable it using ``oracle.remarks-reporting.enabled`` config option. See `Additional Oracle Performance Extensions
    <https://docs.oracle.com/en/database/oracle/oracle-database/19/jjdbc/performance-extensions.html#GUID-96A38C6D-A288-4E0B-9F03-E711C146632B>`_
    for more details.

By default, the Oracle connector uses connection pooling for performance
improvement. The below configuration shows the typical default values. To update
them, change the properties in the catalog configuration file:

.. code-block:: properties

    oracle.connection-pool.max-size=30
    oracle.connection-pool.min-size=1
    oracle.connection-pool.inactive-timeout=20m

To disable connection pooling, update properties to include the following:

.. code-block:: text

    oracle.connection-pool.enabled=false

Multiple Oracle servers
^^^^^^^^^^^^^^^^^^^^^^^

If you want to connect to multiple Oracle servers, configure another instance of
the Oracle connector as a separate catalog.

To add another Oracle catalog, create a new properties file. For example, if
you name the property file ``sales.properties``, Trino creates a catalog named
sales.

Querying Oracle
---------------

The Oracle connector provides a schema for every Oracle database.

Run ``SHOW SCHEMAS`` to see the available Oracle databases::

    SHOW SCHEMAS FROM oracle;

If you used a different name for your catalog properties file, use that catalog
name instead of ``oracle``.

.. note::
    The Oracle user must have access to the table in order to access it from Trino.
    The user configuration, in the connection properties file, determines your
    privileges in these schemas.

Examples
^^^^^^^^

If you have an Oracle database named ``web``, run ``SHOW TABLES`` to see the
tables it contains::

    SHOW TABLES FROM oracle.web;

To see a list of the columns in the ``clicks`` table in the ``web``
database, run either of the following::

    DESCRIBE oracle.web.clicks;
    SHOW COLUMNS FROM oracle.web.clicks;

To access the clicks table in the web database, run the following::

    SELECT * FROM oracle.web.clicks;

.. _oracle-type-mapping:

Type mapping
------------

Both Oracle and Trino have types that are not supported by the Oracle
connector. The following sections explain their type mapping.

Oracle to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trino supports selecting Oracle database types. This table shows the Oracle to
Trino data type mapping:


.. list-table:: Oracle to Trino type mapping
  :widths: 20, 20, 60
  :header-rows: 1

  * - Oracle database type
    - Trino type
    - Notes
  * - ``NUMBER(p, s)``
    - ``DECIMAL(p, s)``
    -  See :ref:`number mapping`
  * - ``NUMBER(p)``
    - ``DECIMAL(p, 0)``
    - See :ref:`number mapping`
  * - ``FLOAT[(p)]``
    - ``DOUBLE``
    -
  * - ``BINARY_FLOAT``
    - ``REAL``
    -
  * - ``BINARY_DOUBLE``
    - ``DOUBLE``
    -
  * - ``VARCHAR2(n CHAR)``
    - ``VARCHAR(n)``
    -
  * - ``VARCHAR2(n BYTE)``
    - ``VARCHAR(n)``
    -
  * - ``NVARCHAR2(n)``
    - ``VARCHAR(n)``
    -
  * - ``CHAR(n)``
    - ``CHAR(n)``
    -
  * - ``NCHAR(n)``
    - ``CHAR(n)``
    -
  * - ``CLOB``
    - ``VARCHAR``
    -
  * - ``NCLOB``
    - ``VARCHAR``
    -
  * - ``RAW(n)``
    - ``VARBINARY``
    -
  * - ``BLOB``
    - ``VARBINARY``
    -
  * - ``DATE``
    - ``TIMESTAMP``
    - See :ref:`datetime mapping`
  * - ``TIMESTAMP(p)``
    - ``TIMESTAMP``
    - See :ref:`datetime mapping`
  * - ``TIMESTAMP(p) WITH TIME ZONE``
    - ``TIMESTAMP WITH TIME ZONE``
    - See :ref:`datetime mapping`

Trino to Oracle type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Trino supports creating tables with the following types in an Oracle database.
The table shows the mappings from Trino to Oracle data types:

.. note::
   For types not listed in the table below, Trino can't perform the ``CREATE
   TABLE <table> AS SELECT`` operations. When data is inserted into existing
   tables ``Oracle to Trino`` type mapping is used.

.. list-table:: Trino to Oracle Type Mapping
  :widths: 20, 20, 60
  :header-rows: 1

  * - Trino type
    - Oracle database type
    - Notes
  * - ``TINYINT``
    - ``NUMBER(3)``
    -
  * - ``SMALLINT``
    - ``NUMBER(5)``
    -
  * - ``INTEGER``
    - ``NUMBER(10)``
    -
  * - ``BIGINT``
    - ``NUMBER(19)``
    -
  * - ``DECIMAL(p, s)``
    - ``NUMBER(p, s)``
    -
  * - ``REAL``
    - ``BINARY_FLOAT``
    -
  * - ``DOUBLE``
    - ``BINARY_DOUBLE``
    -
  * - ``VARCHAR``
    - ``NCLOB``
    -
  * - ``VARCHAR(n)``
    - ``VARCHAR2(n CHAR)`` or ``NCLOB``
    - See :ref:`character mapping`
  * - ``CHAR(n)``
    - ``CHAR(n CHAR)`` or ``NCLOB``
    - See :ref:`character mapping`
  * - ``VARBINARY``
    - ``BLOB``
    -
  * - ``DATE``
    - ``DATE``
    - See :ref:`datetime mapping`
  * - ``TIMESTAMP``
    - ``TIMESTAMP(3)``
    - See :ref:`datetime mapping`
  * - ``TIMESTAMP WITH TIME ZONE``
    - ``TIMESTAMP(3) WITH TIME ZONE``
    - See :ref:`datetime mapping`

.. _number mapping:

Mapping numeric types
^^^^^^^^^^^^^^^^^^^^^

An Oracle ``NUMBER(p, s)`` maps to Trino's ``DECIMAL(p, s)`` except in these
conditions:

- No precision is specified for the column (example: ``NUMBER`` or
  ``NUMBER(*)``), unless ``oracle.number.default-scale`` is set.
- Scale (``s`` ) is greater than precision.
- Precision (``p`` ) is greater than 38.
- Scale is negative and the difference between ``p`` and ``s`` is greater than
  38, unless ``oracle.number.rounding-mode`` is set to a different value than
  ``UNNECESSARY``.

If ``s`` is negative, ``NUMBER(p, s)`` maps to ``DECIMAL(p + s, 0)``.

For Oracle ``NUMBER`` (without precision and scale), you can change
``oracle.number.default-scale=s`` and map the column to ``DECIMAL(38, s)``.

.. _datetime mapping:

Mapping datetime types
^^^^^^^^^^^^^^^^^^^^^^

Selecting a timestamp with fractional second precision (``p``) greater than 3
truncates the fractional seconds to three digits instead of rounding it.

Oracle ``DATE`` type may store hours, minutes, and seconds, so it is mapped
to Trino ``TIMESTAMP``.

.. warning::

  Due to date and time differences in the libraries used by Trino and the
  Oracle JDBC driver, attempting to insert or select a datetime value earlier
  than ``1582-10-15`` results in an incorrect date inserted.

.. _character mapping:

Mapping character types
^^^^^^^^^^^^^^^^^^^^^^^

Trino's ``VARCHAR(n)`` maps to ``VARCHAR2(n CHAR)`` if ``n`` is no greater than
4000. A larger or unbounded ``VARCHAR`` maps to ``NCLOB``.

Trino's ``CHAR(n)`` maps to ``CHAR(n CHAR)`` if ``n`` is no greater than 2000.
A larger ``CHAR`` maps to ``NCLOB``.

Using ``CREATE TABLE AS`` to create an ``NCLOB`` column from a ``CHAR`` value
removes the trailing spaces from the initial values for the column. Inserting
``CHAR`` values into existing ``NCLOB`` columns keeps the trailing spaces. For
example::

    CREATE TABLE vals AS SELECT CAST('A' as CHAR(2001)) col;
    INSERT INTO vals (col) VALUES (CAST('BB' as CHAR(2001)));
    SELECT LENGTH(col) FROM vals;

.. code-block:: text

     _col0
    -------
      2001
         1
    (2 rows)

Attempting to write a ``CHAR`` that doesn't fit in the column's actual size
fails. This is also true for the equivalent ``VARCHAR`` types.

.. include:: jdbc-type-mapping.fragment

Number to decimal configuration properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. list-table::
  :widths: 20, 20, 50, 10
  :header-rows: 1

  * - Configuration property name
    - Session property name
    - Description
    - Default
  * - ``oracle.number.default-scale``
    - ``number_default_scale``
    - Default Trino ``DECIMAL`` scale for Oracle ``NUMBER`` (without precision
      and scale) date type. When not set then such column is treated as not
      supported.
    - not set
  * - ``oracle.number.rounding-mode``
    - ``number_rounding_mode``
    - Rounding mode for the Oracle ``NUMBER`` data type. This is useful when
      Oracle ``NUMBER`` data type specifies higher scale than is supported in
      Trino. Possible values are:

      - ``UNNECESSARY`` - Rounding mode to assert that the
        requested operation has an exact result,
        hence no rounding is necessary.
      - ``CEILING`` - Rounding mode to round towards
        positive infinity.
      - ``FLOOR`` - Rounding mode to round towards negative
        infinity.
      - ``HALF_DOWN`` - Rounding mode to round towards
        ``nearest neighbor`` unless both neighbors are
        equidistant, in which case rounding down is used.
      - ``HALF_EVEN`` - Rounding mode to round towards the
        ``nearest neighbor`` unless both neighbors are equidistant,
        in which case rounding towards the even neighbor is
        performed.
      - ``HALF_UP`` - Rounding mode to round towards
        ``nearest neighbor`` unless both neighbors are
        equidistant, in which case rounding up is used
      - ``UP`` - Rounding mode to round towards zero.
      - ``DOWN`` - Rounding mode to round towards zero.

    - ``UNNECESSARY``

Synonyms
--------

Based on performance reasons, Trino disables support for Oracle ``SYNONYM``. To
include ``SYNONYM``, add the following configuration property:

.. code-block:: text

    oracle.synonyms.enabled=true

.. _oracle-pushdown:

Pushdown
--------

The connector supports pushdown for a number of operations:

* :ref:`join-pushdown`
* :ref:`limit-pushdown`
* :ref:`topn-pushdown`

Limitations
-----------

The following SQL statements are not supported:

* :doc:`/sql/delete`
* :doc:`/sql/alter-table`
* :doc:`/sql/grant`
* :doc:`/sql/revoke`
* :doc:`/sql/show-grants`
* :doc:`/sql/show-roles`
* :doc:`/sql/show-role-grants`
