// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.apidb.v0_6.impl;

import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.container.v0_6.ChangeContainer;
import org.openstreetmap.osmosis.core.lifecycle.ReleasableIterator;
import org.openstreetmap.osmosis.core.task.v0_6.ChangeSink;
import org.postgresql.replication.PGReplicationStream;


/**
 * Replicates changes from the database utilising logical sequence numbers.
 */
public class ReplicatorLsn {

    private static final Logger LOG = Logger.getLogger(ReplicatorLsn.class.getName());

    /**
     * This is the maximum number of transaction ids sent in a single query. If
     * larger than 2 power 16 it fails due to a 16 bit number failing, but still
     * fails below that with a stack limit being exceeded. The current value is
     * near to the maximum value known to work, it will work slightly higher but
     * this is a round number. It is dependent on the max_stack_depth parameter
     * defined in postgresql.conf.
     */
    private static final int TRANSACTION_QUERY_SIZE_MAX = 25000;

    private ChangeSink changeSink;
    private ReplicationSource source;
    private LsnDao lsnLoader;
    private SystemTimeLoader systemTimeLoader;
    private int iterations;
    private int minInterval;
    private int maxInterval;


    /**
     * Creates a new instance.
     *
     * @param source
     *            The source for all replication changes.
     * @param changeSink
     *            The destination for all replicated changes.
     * @param lsnLoader
     *            Loads transaction snapshots from the database.
     * @param systemTimeLoader
     *            Loads the current system time from the database.
     * @param iterations
     *            The number of replication intervals to execute. 0 means
     *            infinite.
     * @param minInterval
     *            The minimum number of milliseconds between intervals.
     * @param maxInterval
     *            The maximum number of milliseconds between intervals if no new
     *            data is available. This isn't a hard limit because processing
     *            latency may increase the duration.
     */
    public ReplicatorLsn(ReplicationSource source, ChangeSink changeSink,
            LsnDao lsnLoader, SystemTimeLoader systemTimeLoader, int iterations,
            int minInterval, int maxInterval) {
        this.source = source;
        this.changeSink = changeSink;
        this.lsnLoader = lsnLoader;
        this.systemTimeLoader = systemTimeLoader;
        this.iterations = iterations;
        this.minInterval = minInterval;
        this.maxInterval = maxInterval;
    }



    private void copyChanges(ReleasableIterator<ChangeContainer> sourceIterator, ReplicationStateLsn state) {
        try (ReleasableIterator<ChangeContainer> i = sourceIterator) {
            Date currentTimestamp;

            // As we process, we must update the timestamp to match the latest
            // record we have received.
            currentTimestamp = state.getTimestamp();

            while (i.hasNext()) {
                ChangeContainer change;
                Date nextTimestamp;

                change = i.next();
                nextTimestamp = change.getEntityContainer().getEntity().getTimestamp();

                if (currentTimestamp.compareTo(nextTimestamp) < 0) {
                    currentTimestamp = nextTimestamp;
                }

                changeSink.process(change);
            }

            state.setTimestamp(currentTimestamp);

        }
    }


    /**
     * Replicates the next set of changes from the database.
     */
    public void replicate() {
        try {
            replicateLoop();

        } finally {
            changeSink.close();
        }
    }


    /**
     * The main replication loop. This continues until the maximum number of
     * replication intervals has been reached.
     */
    private void replicateLoop() {
        // Perform replication up to the number of iterations, or infinitely if
        // set to 0.
        for (int iterationCount = 1; true; iterationCount++) {

            // Perform the replication interval.
            replicateImpl();

            // Stop if we've reached the target number of iterations.
            if (iterations > 0 && iterationCount >= iterations) {
                LOG.fine("Exiting replication loop.");
                break;
            }
        }
    }


    /**
     * Replicates the next set of changes from the database.
     */
    private void replicateImpl() {
        ReplicationStateLsn state;
        Date systemTimestamp;
        Map<String, Object> metaData;
        boolean inCommit = false;

        // Create an initial replication state.
        state = new ReplicationStateLsn();

        // Initialise the sink and provide it with a reference to the state
        // object. A single state instance is shared by both ends of the
        // pipeline.
        metaData = new HashMap<String, Object>(1);
        metaData.put(ReplicationStateLsn.META_DATA_KEY, state);
        // Initialising the sync has the side effect of reading the state previous file
        changeSink.initialize(metaData);

        // Start logical replication stream
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("Begin streaming logical changes from the database.");
        }
        PGReplicationStream stream = lsnLoader.getReplicationStream(state);

        systemTimestamp = systemTimeLoader.getSystemTime();
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("Loaded system time " + systemTimestamp + " from the database.");
        }
        LOG.info("Entering loop with maxInterval = " + maxInterval);

        // Read and process logical stream
        while (true) {
            //non blocking receive message
            ByteBuffer msg;
            try {
                msg = stream.readPending();
            } catch (SQLException e) {
                throw new OsmosisRuntimeException("Unable to receive new logical replication data.", e);
            }

            if (msg == null) {
                if (!inCommit) {
                    // Finish this state file if we've reached the maximum interval or
                    // if our remaining interval exceeds the maximum (clock skew).
                    Date currentTimestamp = systemTimeLoader.getSystemTime();
                    long remainingInterval = maxInterval + systemTimestamp.getTime() - currentTimestamp.getTime();
                    if (remainingInterval <= 0 || remainingInterval > maxInterval) {
                        if (remainingInterval <= 0)
                            LOG.finer("Ending because remainingInterval <= 0: " + remainingInterval);
                        if (remainingInterval > maxInterval)
                            LOG.finer("Ending because remainingInterval > maxInterval: " + remainingInterval);
                        break;
                    } else
                        LOG.finer("Waiting for more data, time left: " + remainingInterval + "ms");
                }
                try {
                    TimeUnit.SECONDS.sleep(1L);
                } catch (InterruptedException e) {
                    //throw new OsmosisRuntimeException("Unable to sleep until data becomes available.", e);
                    break;
                }
                continue;
            }

            int offset = msg.arrayOffset();
            byte[] log_source = msg.array();
            int length = log_source.length - offset;
            String line = new String(log_source, offset, length);

            //System.out.println(line);

            String ops[] = line.split(" ");
            if (ops.length > 0) {
                String op = ops[0];
                if ("COMMIT".equals(op)) {
                    LOG.fine("Received a COMMIT");
                    inCommit = false;
                    // FIXME: Prefer to end state file at the end of a transaction
                } else if ("BEGIN".equals(op)) {
                    LOG.fine("Received a BEGIN");
                    inCommit = true;
                } else if ("NEW".equals(op)) {
                    if (ops.length == 4) {
                        String element = ops[1];
                        Long id = Long.parseLong((ops[2]));
                        int version = Integer.parseInt(ops[3]);
                        LOG.info("NEW " + element + " " + id + " " + version);
                        // Write the change to the destination.
                        copyChanges(source.getHistory(element, id, version), state);
                    }
                } else if ("UPDATE".equals(op)) {
                    // FIXME: When will these ever be seen and what should be put into the change file?
                } else {
                    LOG.info("Received unknown stream message: " + line);
                }
            } else {
                LOG.info("Received bad stream message: \"" + line + "\"");
            }
        }

        /*
         * We must get the latest timestamp before proceeding. Using an earlier
         * timestamp runs the risk of marking a replication sequence with a
         * timestamp that is too early which may lead to replication clients
         * starting with a later sequence than they should.
         */
        systemTimestamp = systemTimeLoader.getSystemTime();
        if (LOG.isLoggable(Level.FINER)) {
            LOG.finer("Loaded system time " + systemTimestamp + " from the database.");
        }

        // Update state file with the metadata about what we processed
        state.setLsnMax(state.getLsnMaxQueried());
        state.setLsnMaxQueried(stream.getLastReceiveLSN());
        state.setTimestamp(systemTimestamp);

        // Commit changes.
        changeSink.complete();

        // FIXME: Consider giving feedback at start of next run instead
        stream.setAppliedLSN(state.getLsnMaxQueried());
        stream.setFlushedLSN(state.getLsnMaxQueried());

        // The status messages are only sent periodically,
        // force an update before we finish
        try {
            stream.forceUpdateStatus();
        } catch (SQLException e) {
            throw new OsmosisRuntimeException("Failed to update logical replication status.", e);
        }

        LOG.info("Replication sequence complete: " + state.getLsnMax() + "..." + state.getLsnMaxQueried());
    }

}
