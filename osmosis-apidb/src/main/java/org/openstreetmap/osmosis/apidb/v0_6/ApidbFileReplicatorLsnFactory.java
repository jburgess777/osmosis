// This software is released into the Public Domain.  See copying.txt for details.
package org.openstreetmap.osmosis.apidb.v0_6;

import org.openstreetmap.osmosis.core.database.DatabaseLoginCredentials;
import org.openstreetmap.osmosis.core.database.DatabasePreferences;
import org.openstreetmap.osmosis.core.database.DatabaseTaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.v0_6.RunnableChangeSourceManager;


/**
 * The task factory for a file-based database replicator.
 */
public class ApidbFileReplicatorLsnFactory extends DatabaseTaskManagerFactory {
    private static final String ARG_ITERATIONS = "iterations";
    private static final String ARG_MIN_INTERVAL = "minInterval";
    private static final String ARG_MAX_INTERVAL = "maxInterval";
    private static final String ARG_SLOT_NAME = "slotName";
    private static final int DEFAULT_ITERATIONS = 1;
    private static final int DEFAULT_MIN_INTERVAL = 0;
    private static final int DEFAULT_MAX_INTERVAL = 0;
    private static final String DEFAULT_SOT_NAME = "osmosis";


    /**
     * {@inheritDoc}
     */
    @Override
    protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
        DatabaseLoginCredentials loginCredentials;
        DatabasePreferences preferences;
        String slotName;
        int iterations;
        int minInterval;
        int maxInterval;

        // Get the task arguments.
        loginCredentials = getDatabaseLoginCredentials(taskConfig);
        preferences = getDatabasePreferences(taskConfig);
        iterations = getIntegerArgument(taskConfig, ARG_ITERATIONS, DEFAULT_ITERATIONS);
        minInterval = getIntegerArgument(taskConfig, ARG_MIN_INTERVAL, DEFAULT_MIN_INTERVAL);
        maxInterval = getIntegerArgument(taskConfig, ARG_MAX_INTERVAL, DEFAULT_MAX_INTERVAL);
        slotName = getStringArgument(taskConfig, ARG_SLOT_NAME, DEFAULT_SOT_NAME);

        return new RunnableChangeSourceManager(
                taskConfig.getId(),
                new ApidbFileReplicatorLsn(loginCredentials, preferences, iterations, minInterval, maxInterval, slotName),
                taskConfig.getPipeArgs()
                );
    }
}
