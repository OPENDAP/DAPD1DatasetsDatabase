#!/bin/sh
#
# Run the Edit Database program for the DAP/D1 servlet. Option -h for help.

#profiler="-javaagent:/Users/jimg/src/jip-src-1.2/profile/profile.jar \
#-Dprofile.properties=/Users/jimg/src/olfs/resources/profile.properties"

java $profiler -Xms256m -Xmx1024m -jar target/DatasetsDataBase-1.0-SNAPSHOT.jar $*
