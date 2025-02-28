/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.flink;

public class FlinkConstants {

    private FlinkConstants() {
        throw new IllegalStateException("Utility class");
    }

    /**
     * flink command
     * usage: flink run [OPTIONS] <jar-file> <arguments>
     */
    public static final String FLINK_COMMAND = "flink";
    public static final String FLINK_RUN = "run";

    /**
     * flink sql command
     * usage: sql-client.sh -i <initialization file>, -f <script file>
     */
    public static final String FLINK_SQL_COMMAND = "sql-client.sh";

    /**
     * flink run options
     */
    public static final String FLINK_RUN_APPLICATION = "run-application";
    public static final String FLINK_YARN_CLUSTER = "yarn-cluster";
    public static final String FLINK_YARN_APPLICATION = "yarn-application";
    public static final String FLINK_YARN_PER_JOB = "yarn-per-job";
    public static final String FLINK_LOCAL = "local";
    public static final String FLINK_RUN_MODE = "-m";
    public static final String FLINK_EXECUTION_TARGET = "-t";
    public static final String FLINK_YARN_SLOT = "-ys";
    public static final String FLINK_APP_NAME = "-ynm";
    public static final String FLINK_YARN_QUEUE_FOR_MODE = "-yqu";
    public static final String FLINK_YARN_QUEUE_FOR_TARGETS = "-Dyarn.application.queue";
    public static final String FLINK_TASK_MANAGE = "-yn";
    public static final String FLINK_JOB_MANAGE_MEM = "-yjm";
    public static final String FLINK_TASK_MANAGE_MEM = "-ytm";
    public static final String FLINK_MAIN_CLASS = "-c";
    public static final String FLINK_PARALLELISM = "-p";
    public static final String FLINK_SHUTDOWN_ON_ATTACHED_EXIT = "-sae";
    public static final String FLINK_PYTHON = "-py";
    public static final String FLINK_SAVEPOINT = "savepoint";
    public static final String FLINK_METRICS = "metrics";
    public static final String FLINK_OVERVIEW = "overview";
    public static final String FLINK_JOBS = "jobs";
    public static final String FLINK_CANCEL = "cancel";
    // For Flink SQL
    public static final String FLINK_FORMAT_EXECUTION_TARGET = "set execution.target=%s";
    public static final String FLINK_FORMAT_YARN_APPLICATION_NAME = "set yarn.application.name=%s";
    public static final String FLINK_FORMAT_YARN_APPLICATION_QUEUE = "set yarn.application.queue=%s";
    public static final String FLINK_FORMAT_JOBMANAGER_MEMORY_PROCESS_SIZE = "set jobmanager.memory.process.size=%s";
    public static final String FLINK_FORMAT_TASKMANAGER_MEMORY_PROCESS_SIZE = "set taskmanager.memory.process.size=%s";
    public static final String FLINK_FORMAT_TASKMANAGER_NUMBEROFTASKSLOTS = "set taskmanager.numberOfTaskSlots=%d";
    public static final String FLINK_FORMAT_PARALLELISM_DEFAULT = "set parallelism.default=%d";
    public static final String FLINK_SQL_SCRIPT_FILE = "-f";
    public static final String FLINK_SQL_INIT_FILE = "-i";
    public static final String FLINK_SQL_NEWLINE = ";\n";
}
