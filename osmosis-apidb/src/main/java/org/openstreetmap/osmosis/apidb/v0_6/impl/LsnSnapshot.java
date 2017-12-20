// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.apidb.v0_6.impl;

import org.postgresql.replication.LogSequenceNumber;

/**
 * Represents the data associated with a database transaction snapshot providing information about
 * currently in-flight transactions.
 */
public class LsnSnapshot {
    private LogSequenceNumber lsn;


    /**
     * Creates a new instance.
     *
     * @param strValue
     *           logical sequence number string
     */
    public LsnSnapshot(String strValue) {
        this.lsn = LogSequenceNumber.valueOf(strValue);
    }


    /**
     * Gets the next logical sequence number to be created.
     *
     * @return The logical sequence number.
     */
    public LogSequenceNumber getLSN() {
        return lsn;
    }

}