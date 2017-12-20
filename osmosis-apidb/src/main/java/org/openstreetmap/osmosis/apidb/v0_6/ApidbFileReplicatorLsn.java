// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.apidb.v0_6;

import org.openstreetmap.osmosis.apidb.common.DatabaseContext2;
import org.openstreetmap.osmosis.apidb.common.DatabaseReplicationContext;
import org.openstreetmap.osmosis.apidb.v0_6.impl.AllEntityDao;
import org.openstreetmap.osmosis.apidb.v0_6.impl.LsnDao;
import org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicationSource;
import org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicatorLsn;
import org.openstreetmap.osmosis.apidb.v0_6.impl.SchemaVersionValidator;
import org.openstreetmap.osmosis.apidb.v0_6.impl.SystemTimeLoader;
import org.openstreetmap.osmosis.apidb.v0_6.impl.TimeDao;
import org.openstreetmap.osmosis.core.database.DatabaseLoginCredentials;
import org.openstreetmap.osmosis.core.database.DatabasePreferences;
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink;
import org.openstreetmap.osmosis.core.task.v0_6.RunnableChangeSource;


/**
 * Performs replication from an API database into change files using logical sequence numbers.
 */
public class ApidbFileReplicatorLsn implements RunnableChangeSource {

    private DatabaseLoginCredentials loginCredentials;
    private DatabasePreferences preferences;
    private int iterations;
    private int minInterval;
    private int maxInterval;
    private ChangeSink changeSink;
    private String slotName;


    /**
     * Creates a new instance.
     *
     * @param loginCredentials
     *            Contains all information required to connect to the database.
     * @param preferences
     *            Contains preferences configuring database behaviour.
     * @param iterations
     *            The number of replication intervals to execute. 0 means
     *            infinite.
     * @param minInterval
     *            The minimum number of milliseconds between intervals.
     * @param maxInterval
     *            The maximum number of milliseconds between intervals if no new
     *            data is available. This isn't a hard limit because proces
     * @param slotName
     */
    public ApidbFileReplicatorLsn(DatabaseLoginCredentials loginCredentials, DatabasePreferences preferences,
            int iterations, int minInterval, int maxInterval, String slotName) {
        this.loginCredentials = loginCredentials;
        this.preferences = preferences;
        this.iterations = iterations;
        this.minInterval = minInterval;
        this.maxInterval = maxInterval;
        this.slotName = slotName;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setChangeSink(ChangeSink changeSink) {
        this.changeSink = changeSink;
    }


    /**
     * Runs the task implementation. This is called by the run method within a transaction.
     *
     * @param dbCtx
     *            Used to access the database for general SQL
     * @param repCtx
     *            Database replication stream access
     */
    protected void runImpl(DatabaseContext2 dbCtx, DatabaseReplicationContext repCtx) {
        ReplicatorLsn replicator;
        ReplicationSource source;
        LsnDao lsnLoader;
        SystemTimeLoader systemTimeLoader;

        new SchemaVersionValidator(loginCredentials, preferences)
        .validateVersion(ApidbVersionConstants.SCHEMA_MIGRATIONS);

        source = new AllEntityDao(dbCtx.getJdbcTemplate());
        lsnLoader = new LsnDao(dbCtx.getJdbcTemplate(), repCtx, slotName);
        systemTimeLoader = new TimeDao(dbCtx.getJdbcTemplate());

        replicator = new ReplicatorLsn(source, changeSink, lsnLoader,
                systemTimeLoader, iterations, minInterval, maxInterval);

        replicator.replicate();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        try (
                DatabaseContext2 dbCtx = new DatabaseContext2(loginCredentials);
                DatabaseReplicationContext repCtx = new DatabaseReplicationContext(loginCredentials)) {
            runImpl(dbCtx, repCtx);
        }
    }
}
