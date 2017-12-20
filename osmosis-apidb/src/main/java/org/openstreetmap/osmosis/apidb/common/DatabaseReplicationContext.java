// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.apidb.common;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.apidb.v0_6.impl.ReplicationStateLsn;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.database.DatabaseLoginCredentials;
import org.openstreetmap.osmosis.core.database.DatabaseType;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;


/**
 * This class manages the lifecycle of JDBC objects to minimise the risk of connection leaks and to
 * support a consistent approach to database access.
 *
 * @author Brett Henderson
 */
public class DatabaseReplicationContext implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(DatabaseReplicationContext.class.getName());

    private final DatabaseLoginCredentials loginCredentials;

    private Connection connection;
    private PGConnection pgConnection;


    /**
     * Creates a new instance.
     *
     * @param loginCredentials Contains all information required to connect to the database.
     */
    public DatabaseReplicationContext(DatabaseLoginCredentials loginCredentials) {
        this.loginCredentials = loginCredentials;
        try {
            switch (loginCredentials.getDbType()) {
            case POSTGRESQL:
                Class.forName("org.postgresql.Driver");
                break;
            case MYSQL:
                throw createUnsupportedDbTypeException();
            default:
                throw createUnknownDbTypeException();
            }

        } catch (ClassNotFoundException e) {
            throw new OsmosisRuntimeException("Unable to find database driver.", e);
        }
    }

    private OsmosisRuntimeException createUnsupportedDbTypeException() {
        return new OsmosisRuntimeException("Replication not supported for database type " + loginCredentials.getDbType() + ".");
    }

    private OsmosisRuntimeException createUnknownDbTypeException() {
        return new OsmosisRuntimeException("Unknown database type " + loginCredentials.getDbType() + ".");
    }

    /**
     * If no database connection is open, a new connection is opened. The database connection is
     * then returned.
     *
     * @return The database connection.
     */
    private PGConnection getConnection() {
        if (pgConnection == null) {
            switch (loginCredentials.getDbType()) {
            case POSTGRESQL:
                pgConnection = getPostgresReplicationConnection();
                break;
            case MYSQL:
                throw createUnsupportedDbTypeException();
            default:
                throw createUnknownDbTypeException();
            }
        }

        return pgConnection;
    }

    /**
     * Returns the database type currently in use. This should only be used when it is not possible
     * to write database agnostic statements.
     *
     * @return The database type.
     */
    public DatabaseType getDatabaseType() {
        return loginCredentials.getDbType();
    }


    /**
     * Releases all database resources. This method is guaranteed not to throw transactions and
     * should always be called in a finally or try-with-resources block whenever this class is used.
     */
    public void close() {

        if (connection != null) {
            try {
                connection.close();

            } catch (SQLException e) {
                // We cannot throw an exception within a release statement.
                LOG.log(Level.WARNING, "Unable to close database connection.", e);
            }

            connection = null;
            pgConnection = null;
        }
    }


    /**
     * Enforces cleanup of any remaining resources during garbage collection. This is a safeguard
     * and should not be required if release is called appropriately.
     *
     * @throws Throwable If a problem occurs during finalization.
     */
    @Override
    protected void finalize() throws Throwable {
        close();

        super.finalize();
    }

    /**
     * @return postgres replication connection
     */
    private PGConnection getPostgresReplicationConnection() {
        PGConnection replConnection = null;
        try {
            LOG.finer("Creating a new database replication connection.");

            String url = "jdbc:postgresql://" + loginCredentials.getHost() + "/"
                    + loginCredentials.getDatabase();

            Properties props = new Properties();
            PGProperty.USER.set(props, loginCredentials.getUser());
            PGProperty.PASSWORD.set(props, loginCredentials.getPassword());
            PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
            PGProperty.REPLICATION.set(props, "database");
            PGProperty.PREFER_QUERY_MODE.set(props, "simple");
            //PGProperty.LOGGER_LEVEL.set(props, "DEBUG");

            connection = DriverManager.getConnection(url, props);
            replConnection = connection.unwrap(PGConnection.class);
        } catch (SQLException e) {
            throw new OsmosisRuntimeException("Unable to establish a database replication connection.", e);
        }
        return replConnection;
    }

    /**
     * Creates a new replication slot
     *
     * @param slotName
     *            The name of the new replication slot.
     * @param pluginName
     *            The name of the logical decoding plugin to use.
     */
    public void createReplicationSlot(String slotName, String pluginName) {
        PGConnection replConnection = getConnection();

        try {
            replConnection.getReplicationAPI()
            .createReplicationSlot()
            .logical()
            .withSlotName(slotName)
            .withOutputPlugin(pluginName)
            .make();
        } catch (SQLException e) {
            throw new OsmosisRuntimeException("Unable to create new replication slot " + slotName + " with plugin " + pluginName + ".", e);
        }
    }

    /**
     * Creates a logical replication stream
     *
     * @param slotName
     *            The name of the new replication slot.
     * @param startLsn
     *            The logical sequence number to start from.
     */
    public PGReplicationStream createReplicationStream(String slotName, LogSequenceNumber startLsn) {
        PGConnection replConnection = getConnection();
        PGReplicationStream stream = null;

        try {
            stream = replConnection.getReplicationAPI()
                    .replicationStream()
                    .logical()
                    .withSlotName(slotName)
                    //.withSlotOption("include-xids", false)
                    //.withSlotOption("skip-empty-xacts", true)
                    .withStartPosition(startLsn)
                    .withStatusInterval(20, TimeUnit.SECONDS)
                    .start();
        } catch (SQLException e) {
            throw new OsmosisRuntimeException("Unable to create new replication stream for " + slotName
                    + " from LSN " + startLsn.asString() + ".", e);
        }
        return stream;
    }

    /**
     * Creates a logical replication stream
     *
     * @param state
     *            The current replication state
     */
    public PGReplicationStream createReplicationStream(ReplicationStateLsn state, String slotName) {

        // If this is the first replication, assume we need to create the stream
        if (state.getLsnMax() == LogSequenceNumber.INVALID_LSN) {
            createReplicationSlot(slotName, "\"osm-logical\"");
            state.setSlotName(slotName);
        }

        return createReplicationStream(slotName, state.getLsnMaxQueried());
    }

}
