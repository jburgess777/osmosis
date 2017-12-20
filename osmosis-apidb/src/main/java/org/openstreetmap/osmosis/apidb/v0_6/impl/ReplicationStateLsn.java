// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.apidb.v0_6.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.openstreetmap.osmosis.apidb.common.DatabaseContext;

/**
 * Contains the state to be remembered between replication invocations. This state ensures that no
 * data is missed during replication, and none is repeated.
 */
public class ReplicationStateLsn extends org.openstreetmap.osmosis.replication.common.ReplicationState {
    private LogSequenceNumber lsnMax;
    private LogSequenceNumber lsnMaxQueried;
    private String slotName;


    /**
     * Creates a new instance with all values set to defaults.
     */
    public ReplicationStateLsn() {
        super();
        this.lsnMax = LogSequenceNumber.INVALID_LSN;
        this.lsnMaxQueried = LogSequenceNumber.INVALID_LSN;
        this.slotName = null;
    }


    /**
     * Creates a new instance.
     *
     * @param slotName
     *            The replication slot name.
     * @param lsnMax
     *            The maximum log sequence number in the database.
     * @param lsnMaxQueried
     *            The maximum log sequence number currently replicated from the database.
     * @param timestamp
     *            The maximum timestamp of data currently read from the database.
     * @param sequenceNumber
     *            The replication sequence number.
     */
    public ReplicationStateLsn(String slotName, LogSequenceNumber lsnMax,
            LogSequenceNumber lsnMaxQueried, Date timestamp, long sequenceNumber) {
        super(timestamp, sequenceNumber);
        this.lsnMax = lsnMax;
        this.lsnMaxQueried = lsnMaxQueried;
        this.slotName = slotName;
    }


    /**
     * Creates a new instance.
     *
     * @param properties
     *            The properties to load state from.
     */
    public ReplicationStateLsn(Map<String, String> properties) {
        load(properties);
    }


    /**
     * Loads all state from the provided properties object.
     *
     * @param properties
     *            The properties to be read.
     */
    public void load(Map<String, String> properties) {
        super.load(properties);
        lsnMax = LogSequenceNumber.valueOf(properties.get("lsnMax"));
        lsnMaxQueried = LogSequenceNumber.valueOf(properties.get("lsnMaxQueried"));
        slotName = properties.get("slotName");
    }


    @Override
    public void store(Map<String, String> properties) {
        super.store(properties);
        properties.put("lsnMax", lsnMax.asString());
        properties.put("lsnMaxQueried", lsnMaxQueried.asString());
        properties.put("slotName", slotName);
    }


    @Override
    public Map<String, String> store() {
        Map<String, String> properties = new HashMap<String, String>();
        store(properties);
        return properties;
    }


    /**
     * Gets the maximum log sequence number in the database.
     *
     * @return The log sequence number.
     */
    public LogSequenceNumber getLsnMax() {
        return lsnMax;
    }


    /**
     * Sets the maximum log sequence number in the database.
     *
     * @param lsnMax
     *            The log sequence number.
     */
    public void setLsnMax(LogSequenceNumber lsnMax) {
        this.lsnMax = lsnMax;
    }


    public String getSlotName() {
        return slotName;
    }

    public void setSlotName(String s) {
        slotName = s;
    }


    /**
     * Gets the maximum log sequence number currently replicated from the database.
     *
     * @return The log sequence number.
     */
    public LogSequenceNumber getLsnMaxQueried() {
        return lsnMaxQueried;
    }


    /**
     * Sets the maximum log sequence number currently replicated from the database.
     *
     * @param lsnMaxQueried
     *            The log sequence number.
     */
    public void setLsnMaxQueried(LogSequenceNumber lsnMaxQueried) {
        this.lsnMaxQueried = lsnMaxQueried;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        boolean result;

        if (obj instanceof ReplicationStateLsn) {
            ReplicationStateLsn compareState = (ReplicationStateLsn) obj;

            if (super.equals(obj)
                    && slotName.equals(compareState.slotName)
                    && lsnMax == compareState.lsnMax
                    && lsnMaxQueried == compareState.lsnMaxQueried) {
                result = true;
            } else {
                result = false;
            }
        } else {
            result = false;
        }

        return result;
    }


    @Override
    public int hashCode() {
        return super.hashCode() + lsnMax.hashCode() + lsnMaxQueried.hashCode() + slotName.hashCode();
    }


    @Override
    public String toString() {
        return "ReplicationStateLsn(slotName=" + slotName
                + ", lsnMax=" + lsnMax.asString()
                + ", lsnMaxQueried=" + lsnMaxQueried.asString()
                + ", timestamp=" + getTimestamp()
                + ", sequenceNumber=" + getSequenceNumber()
                + ")";
    }


}
