#!/bin/sh
#
# Run the Edit Database program for the DAP/D1 servlet. Option -h for help.

java -Xms256m -Xmx1024m -jar target/DatasetsDatabase-1.0-SNAPSHOT.jar $*
