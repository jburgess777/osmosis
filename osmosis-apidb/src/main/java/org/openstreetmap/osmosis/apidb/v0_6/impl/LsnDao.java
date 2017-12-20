// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.apidb.v0_6.impl;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.openstreetmap.osmosis.apidb.common.DatabaseContext2;
import org.openstreetmap.osmosis.apidb.common.DatabaseReplicationContext;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;


/**
 * Reads logical changes from the database logical replication stream
 * to allow up-to-current queries to be performed
 * when extracting changesets from the history tables.
 */
public class LsnDao {
    private static final Logger LOG = Logger.getLogger(LsnDao.class.getName());

    private DatabaseReplicationContext repCtx;

    private JdbcTemplate jdbcTemplate;

    private String slotName;


    /**
     * Creates a new instance.
     * @param jdbcTemplate
     *
     * @param repCtx
     *            Used to access the database.
     * @param slotName
     *            The logical replication slot name.
     */
    public LsnDao(JdbcTemplate jdbcTemplate, DatabaseReplicationContext repCtx, String slotName) {
        this.jdbcTemplate = jdbcTemplate;
        this.repCtx = repCtx;
        this.slotName = slotName;
    }

    public PGReplicationStream getReplicationStream(ReplicationStateLsn state) {
        return repCtx.createReplicationStream(state, slotName);
    }
}
