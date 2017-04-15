# H2 Data Logger
Datalogger component for OpenMUC that uses an [H2 database](http://www.h2database.com/html/main.html) to store logged values. Currently, the following features are implemented:

* Storage of all kind of OpenMUC values in a local H2 file-based database
* Channel information including a timestamp for last initialization will be stored in database
* Periodical cleanup of old values in the database (ring buffer)

H2 is dual licensed and available under the MPL 2.0 (Mozilla Public License Version 2.0) or under the EPL 1.0 (Eclipse Public License). For more information see [the H2 website](http://www.h2database.com/html/license.html). 

## Datatypes
The following table shows the OpenMUC data types and the related H2 database data types that are used within the datalogger component:

| OpenMUC type | [H2 database type](http://www.h2database.com/html/datatypes.html) |
|:--------------|:-----------|
| LONG | BIGINT |
| INTEGER | INT |
| SHORT | INT |
| BYTE | INT |
| BOOLEAN | BOOLEAN |
| BYTE_ARRAY | VARCHAR(1024) |
| STRING | VARCHAR(1024) |
| DOUBLE | DOUBLE |

For each H2 database data type, the component uses a separate database table to store the values. The channel information table stores the used data type for each channel.

## Database internals
Each record consists of the following information: channel id, timestamp and an OpenMUC flag. The channel information table stores the following details of each channel that should be logged:

| Property | Description |
|:--------------|:-----------|
| ID | The OpenMUC channel id (must be unique) |
| DESCRIPTION | Description of the channel |
| UNIT | The unit associated with the channel |
| LAST_INIT | Timestamp of the last initialization in OpenMUC |
| VALUE_TYPE | The OpenMUC value type |

On startup or every change of the channel configuration in OpenMUC, logging will be initiated. The OpenMUC framework tells the logger component(s), which channels should be logged. Every time this happens, the `LAST_INIT` field in the channel information table will be set to the current system time. This enables to identify channels in the database that doesn't exist or at least will not be logged any longer.

The default database name is `./h2logger/database`. If the database file doesn't exist, the directory and a database file will be created during component initialization. The default database user is `sa`, no password will be set. The default database name can be changed by setting the `org.openmucextensions.datalogger.h2.database` system property.

## Periodical cleanup
Because available disk space is limited on many systems, the component supports a periodical cleanup of old values in the database. This means, that values that are older than a certain threshold compared to the local system time will be deleted. The threshold (interval) can be set in milliseconds by using the system property `org.openmucextensions.datalogger.h2.storageInterval`. The default value is 14 days. If the property is set to zero, periodical cleanup will be disabled.
