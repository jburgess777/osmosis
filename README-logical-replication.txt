This code implements a new task called: --replicate-apidb-lsn

This task operates similarly to the previous --replicate-apidb but
instead of using transaction XIDs to track progress it relies on
the docoding the logical replication feed from the osm-logical
plugin instead.

The reason for this change is described here:
https://github.com/openstreetmap/operations/issues/154

Basic setup steps:

* osm-logical plugin:
-- git clone https://github.com/zerebubuth/osm-logical.git
-- cd osm-logical
-- make
-- sudo make install

* Install the openstreetmap-website
-- https://github.com/openstreetmap/openstreetmap-website/blob/master/INSTALL.md

* Adjust posgressql.conf to allow logical replication:

wal_level=logical
max_wal_senders = 4
wal_keep_segments = 4
max_replication_slots=4

* Adjust postgres pg_hba.conf to allow replication.

* Build osmosis code
-- http://wiki.openstreetmap.org/wiki/Osmosis/Installation

Running
-------

$ mkdir output
$ package/bin/osmosis --replicate-apidb-lsn database="openstreetmap" validateSchemaVersion="no" maxInterval=1000 --write-replication workingDirectory="output"

On the first run it will attempt to create the replication slot in the database

The output should look somethnig like the following:

$ package/bin/osmosis --replicate-apidb-lsn database="openstreetmap" validateSchemaVersion="no" maxInterval=1000 --write-replication workingDirectory="output"
Dec 20, 2017 11:39:27 PM org.openstreetmap.osmosis.core.Osmosis run
INFO: Osmosis Version -SNAPSHOT
Dec 20, 2017 11:39:27 PM org.openstreetmap.osmosis.core.Osmosis run
INFO: Preparing pipeline.
Dec 20, 2017 11:39:27 PM org.openstreetmap.osmosis.core.Osmosis run
INFO: Launching pipeline execution.
Dec 20, 2017 11:39:27 PM org.openstreetmap.osmosis.core.Osmosis run
INFO: Pipeline executing, waiting for completion.
Dec 20, 2017 11:39:27 PM org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicatorLsn replicateImpl
INFO: Entering loop with maxInterval = 1000
Dec 20, 2017 11:39:28 PM org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicatorLsn replicateImpl
INFO: Replication sequence complete. LSN LSN{0/0}...LSN{0/0}
Dec 20, 2017 11:39:28 PM org.openstreetmap.osmosis.core.Osmosis run
INFO: Pipeline complete.
Dec 20, 2017 11:39:28 PM org.openstreetmap.osmosis.core.Osmosis run
INFO: Total execution time: 1625 milliseconds.

In the database you can see the new replication slot:

openstreetmap=# select * from pg_replication_slots ;
 slot_name |   plugin    | slot_type | datoid |   database    | active | active_pid | xmin | catalog_xmin | restart_lsn | confirmed_flush_lsn 
-----------+-------------+-----------+--------+---------------+--------+------------+------+--------------+-------------+---------------------
 osmosis   | osm-logical | logical   |  24599 | openstreetmap | f      |          ¤ |    ¤ |        23551 | 0/34C1950   | 0/34C1988
(1 row)


The first run also produces the first state file:

[jburgess@localhost osmosis]$ find output -type f
output/000/000/000.state.txt
output/state.txt
output/replicate.lock


If the same command is run for a second time a new state file and the first change file should be produced. It is likely these will be empy.

$ package/bin/osmosis --replicate-apidb-lsn database="openstreetmap" validateSchemaVersion="no" maxInterval=1000 --write-replication workingDirectory="output"

$ find output -type f
output/000/000/001.osc.gz
output/000/000/000.state.txt
output/000/000/001.state.txt
output/state.txt
output/replicate.lock

$ cat output/state.txt
#Wed Dec 20 23:42:14 GMT 2017
sequenceNumber=1
slotName=osmosis
lsnMaxQueried=0/0
timestamp=2017-12-20T23\:42\:14Z
lsnMax=0/0
[jburgess@localhost osmosis]$ zcat output/000/000/001.osc.gz
<?xml version='1.0' encoding='UTF-8'?>
<osmChange version="0.6" generator="Osmosis -SNAPSHOT">
</osmChange>


Upload a change to your local OSM API, then run osmosis again.
This time it should report that it found changes and the OSC show the change:

Dec 20, 2017 11:44:49 PM org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicatorLsn replicateImpl
INFO: NEW nodes 23 1
Dec 20, 2017 11:44:49 PM org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicatorLsn replicateImpl
INFO: Replication sequence complete. LSN LSN{0/0}...LSN{0/34D0490}


$ zcat output/000/000/002.osc.gz
<?xml version='1.0' encoding='UTF-8'?>
<osmChange version="0.6" generator="Osmosis -SNAPSHOT">
  <create>
    <node id="23" version="1" timestamp="2017-12-20T23:44:40Z" uid="1" user="Localhost Test" changeset="4" lat="50.7535397" lon="-1.5278456">
      <tag k="shop" v="bicycle"/>
    </node>
  </create>
</osmChange>


If maxInterval is set to say 30000ms (30 seconds) then osmosis will accumulate changes for this period.
Upload changes while it is running and they should appear:

$ package/bin/osmosis --replicate-apidb-lsn database="openstreetmap" validateSchemaVersion="no" maxInterval=30000 --write-replication workingDirectory="output"

INFO: Entering loop with maxInterval = 30000
Dec 20, 2017 11:48:11 PM org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicatorLsn replicateImpl
INFO: NEW nodes 23 2
Dec 20, 2017 11:48:17 PM org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicatorLsn replicateImpl
INFO: NEW nodes 8 3
Dec 20, 2017 11:48:17 PM org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicatorLsn replicateImpl
INFO: NEW nodes 9 3
Dec 20, 2017 11:48:17 PM org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicatorLsn replicateImpl
INFO: NEW nodes 10 3
Dec 20, 2017 11:48:26 PM org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicatorLsn replicateImpl
INFO: Replication sequence complete. LSN LSN{0/34D0490}...LSN{0/35106B8}

