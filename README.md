# H2 Data Logger
Datalogger component for OpenMUC that uses an [H2 database](http://www.h2database.com/html/main.html) to store logged values. Currently, the following features are implemented:

* Storage of `double` values in a local H2 file-based database
* Channel information including a timestamp for last initialization will be stored in database
* Periodical cleanup of old values in the database (ring buffer)
