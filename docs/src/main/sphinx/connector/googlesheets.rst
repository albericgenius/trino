=======================
Google Sheets connector
=======================

.. raw:: html

  <img src="../_static/img/google-sheets.png" class="connector-logo">

The Google Sheets connector allows reading `Google Sheets <https://www.google.com/sheets/about/>`_ spreadsheets as tables in Trino.

Configuration
-------------

Create ``etc/catalog/sheets.properties``
to mount the Google Sheets connector as the ``sheets`` catalog,
replacing the properties as appropriate:

.. code-block:: text

    connector.name=gsheets
    gsheets.credentials-path=/path/to/google-sheets-credentials.json
    gsheets.metadata-sheet-id=exampleId

Configuration properties
------------------------

The following configuration properties are available:

=================================== =====================================================================
Property name                       Description
=================================== =====================================================================
``gsheets.credentials-path``        Path to the Google API JSON key file
``gsheets.metadata-sheet-id``       Sheet ID of the spreadsheet, that contains the table mapping
``gsheets.max-data-cache-size``     Maximum number of spreadsheets to cache, defaults to ``1000``
``gsheets.data-cache-ttl``          How long to cache spreadsheet data or metadata, defaults to ``5m``
``gsheets.read-timeout``            Timeout to read data from spreadsheet, defaults to ``20s``
=================================== =====================================================================

Credentials
-----------

The connector requires credentials in order to access the Google Sheets API.

1. Open the `Google Sheets API <https://console.developers.google.com/apis/library/sheets.googleapis.com>`_
   page and click the *Enable* button. This takes you to the API manager page.

2. Select a project using the drop down menu at the top of the page.
   Create a new project, if you do not already have one.

3. Choose *Credentials* in the left panel.

4. Click *Manage service accounts*, then create a service account for the connector.
   On the *Create key* step, create and download a key in JSON format.

The key file needs to be available on the Trino coordinator and workers.
Set the ``gsheets.credentials-path`` configuration property to point to this file.
The exact name of the file does not matter -- it can be named anything.

Metadata sheet
--------------

The metadata sheet is used to map table names to sheet IDs.
Create a new metadata sheet. The first row must be a header row
containing the following columns in this order:

* Table Name
* Sheet ID
* Owner
* Notes

See this `example sheet <https://docs.google.com/spreadsheets/d/1Es4HhWALUQjoa-bQh4a8B5HROz7dpGMfq_HbfoaW5LM>`_
as a reference.

The metadata sheet must be shared with the service account user,
the one for which the key credentials file was created. Click the *Share*
button to share the sheet with the email address of the service account.

Set the ``gsheets.metadata-sheet-id`` configuration property to the ID of this sheet.

Querying sheets
---------------

The service account user must have access to the sheet in order for Trino
to query it. Click the *Share* button to share the sheet with the email
address of the service account.

The sheet needs to be mapped to a Trino table name. Specify a table name
(column A) and the sheet ID (column B) in the metadata sheet. To refer
to a specific tab in the sheet, add the tab name after the sheet ID, separated
with ``#``. If tab name is not provided, connector loads only 10,000 rows by default from
the first tab in the sheet.

API usage limits
----------------

The Google Sheets API has `usage limits <https://developers.google.com/sheets/api/limits>`_,
that may impact the usage of this connector. Increasing the cache duration and/or size
may prevent the limit from being reached. Running queries on the ``information_schema.columns``
table without a schema and table name filter may lead to hitting the limit, as this requires
fetching the sheet data for every table, unless it is already cached.

Type mapping
------------

Because Trino and Google Sheets each support types that the other does not, this
connector :ref:`modifies some types <type-mapping-overview>` when reading data.

Google Sheets type to Trino type mapping
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The connector maps Google Sheets types to the corresponding Trino types
following this table:

.. list-table:: Google Sheets type to Trino type mapping
  :widths: 30, 20
  :header-rows: 1

  * - Google Sheets type
    - Trino type
  * - ``TEXT``
    - ``VARCHAR``

No other types are supported.

.. _google-sheets-sql-support:

SQL support
-----------

The connector provides :ref:`globally available <sql-globally-available>` and
:ref:`read operation <sql-read-operations>` statements to access data and
metadata in Google Sheets.
