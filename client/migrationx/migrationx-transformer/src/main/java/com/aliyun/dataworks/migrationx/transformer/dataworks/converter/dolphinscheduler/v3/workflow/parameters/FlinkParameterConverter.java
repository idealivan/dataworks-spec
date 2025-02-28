/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.utils.ArgsUtils;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ProgramType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.flink.FlinkConstants;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.flink.FlinkDeployMode;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.flink.FlinkParameters;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

public class FlinkParameterConverter extends AbstractParameterConverter<FlinkParameters> {
    private static final String FLINK_VERSION_BEFORE_1_10 = "<1.10";
    private static final String FLINK_VERSION_AFTER_OR_EQUALS_1_12 = ">=1.12";
    private static final String FLINK_VERSION_AFTER_OR_EQUALS_1_13 = ">=1.13";
    public static final FlinkDeployMode DEFAULT_DEPLOY_MODE = FlinkDeployMode.CLUSTER;

    public FlinkParameterConverter(Properties properties, SpecWorkflow specWorkflow, DagData processMeta, TaskDefinition taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);

        convertFileResourceList(specNode);

        SpecScript script = new SpecScript();
        ProgramType programType = this.parameter.getProgramType();
        String type;
        if (ProgramType.SQL.equals(programType)) {
            type = getSqlConverterType();
        } else {
            throw new RuntimeException("not support flink type: " + programType);
        }

        CodeProgramType codeProgramType = CodeProgramType.getNodeTypeByName(type);
        String language = codeToLanguageIdentifier(codeProgramType);
        script.setLanguage(language);
        //runtime
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setEngine(codeProgramType.getCalcEngineType().getLabel());
        runtime.setCommand(codeProgramType.getName());
        script.setRuntime(runtime);

        script.setPath(getScriptPath(specNode));
        String code = buildRunCommandLine(this.parameter);
        String resourceReference = buildFileResourceReference(specNode, RESOURCE_REFERENCE_PREFIX);
        code = replaceCodeWithParams(code, specVariableList);
        script.setContent(resourceReference + code);
        script.setContent(code);
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
        runtime.setFlinkConf(buildConf(this.parameter));
        postHandle("FLINK", script);
    }

    public String buildRunCommandLine(FlinkParameters param) {
        switch (param.getProgramType()) {
            case SQL:
                List<String> sqls = buildRunCommandLineForSql(param);
                return String.join("\n", sqls);
            default:
                List<String> args = buildRunCommandLineForOthers(param);
                return String.join(" ", args);
        }
    }

    public Map<String, Object> buildConf(FlinkParameters param) {
        Map<String, Object> conf = new HashMap<>();
        //conf.put("executionMode", "SQL");
        Map<String, Object> streamingResourceSetting = new HashMap<>();
        streamingResourceSetting.put("resourceSettingMode", "BASIC");
        Map<String, Object> basicResourceSetting = new HashMap<>();
        basicResourceSetting.put("parallelism", param.getParallelism());
        basicResourceSetting.put("taskmanagerResourceSettingSpec",
                ImmutableMap.<String, Object>of("memory", param.getTaskManagerMemory(), "cpu", 1.0));
        basicResourceSetting.put("jobmanagerResourceSettingSpec",
                ImmutableMap.<String, Object>of("memory", param.getJobManagerMemory(), "cpu", 1.0));

        conf.put("streamingResourceSetting", streamingResourceSetting);
        Map<String, Object> flinkConf = new HashMap<>();
        //flinkConf.put("execution.checkpointing.interval", "1second");
        flinkConf.put("taskmanager.numberOfTaskSlots", param.getSlot());
        conf.put("flinkConf", flinkConf);
        return conf;
    }

    /**
     * build flink run command line for SQL
     *
     * @return argument list
     */
    private List<String> buildRunCommandLineForSql(FlinkParameters flinkParameters) {
        List<String> args = new ArrayList<>();

        //args.add(FlinkConstants.FLINK_SQL_COMMAND);
        // -i
        //args.add(FlinkConstants.FLINK_SQL_INIT_FILE);
        args.add(flinkParameters.getInitScript());

        // -f
        //args.add(FlinkConstants.FLINK_SQL_SCRIPT_FILE);
        args.add(flinkParameters.getRawScript());

        //String others = flinkParameters.getOthers();
        //
        //if (StringUtils.isNotEmpty(others)) {
        //    args.add(others);
        //}
        return args;
    }

    public List<String> buildInitOptionsForSql(FlinkParameters flinkParameters) {
        List<String> initOptions = new ArrayList<>();

        FlinkDeployMode deployMode =
                Optional.ofNullable(flinkParameters.getDeployMode()).orElse(FlinkDeployMode.CLUSTER);

        /**
         * Currently flink sql on yarn only supports yarn-per-job mode
         */
        if (FlinkDeployMode.LOCAL == deployMode) {
            // execution.target
            initOptions.add(String.format(FlinkConstants.FLINK_FORMAT_EXECUTION_TARGET, FlinkConstants.FLINK_LOCAL));
        } else if (FlinkDeployMode.STANDALONE == deployMode) {
            // standalone exec
        } else {
            // execution.target
            initOptions.add(
                    String.format(FlinkConstants.FLINK_FORMAT_EXECUTION_TARGET, FlinkConstants.FLINK_YARN_PER_JOB));

            // taskmanager.numberOfTaskSlots
            int slot = flinkParameters.getSlot();
            if (slot > 0) {
                initOptions.add(String.format(FlinkConstants.FLINK_FORMAT_TASKMANAGER_NUMBEROFTASKSLOTS, slot));
            }

            // yarn.application.name
            String appName = flinkParameters.getAppName();
            if (StringUtils.isNotEmpty(appName)) {
                initOptions.add(
                        String.format(FlinkConstants.FLINK_FORMAT_YARN_APPLICATION_NAME, ArgsUtils.escape(appName)));
            }

            // jobmanager.memory.process.size
            String jobManagerMemory = flinkParameters.getJobManagerMemory();
            if (StringUtils.isNotEmpty(jobManagerMemory)) {
                initOptions.add(
                        String.format(FlinkConstants.FLINK_FORMAT_JOBMANAGER_MEMORY_PROCESS_SIZE, jobManagerMemory));
            }

            // taskmanager.memory.process.size
            String taskManagerMemory = flinkParameters.getTaskManagerMemory();
            if (StringUtils.isNotEmpty(taskManagerMemory)) {
                initOptions.add(
                        String.format(FlinkConstants.FLINK_FORMAT_TASKMANAGER_MEMORY_PROCESS_SIZE, taskManagerMemory));
            }

            // yarn.application.queue
            String yarnQueue = flinkParameters.getYarnQueue();
            if (StringUtils.isNotEmpty(yarnQueue)) {
                initOptions.add(String.format(FlinkConstants.FLINK_FORMAT_YARN_APPLICATION_QUEUE, yarnQueue));
            }
        }

        // parallelism.default
        int parallelism = flinkParameters.getParallelism();
        if (parallelism > 0) {
            initOptions.add(String.format(FlinkConstants.FLINK_FORMAT_PARALLELISM_DEFAULT, parallelism));
        }

        return initOptions;
    }

    private List<String> buildRunCommandLineForOthers(FlinkParameters flinkParameters) {
        List<String> args = new ArrayList<>();

        args.add(FlinkConstants.FLINK_COMMAND);
        FlinkDeployMode deployMode = Optional.ofNullable(flinkParameters.getDeployMode()).orElse(DEFAULT_DEPLOY_MODE);
        String flinkVersion = flinkParameters.getFlinkVersion();
        // build run command
        switch (deployMode) {
            case CLUSTER:
                if (FLINK_VERSION_AFTER_OR_EQUALS_1_12.equals(flinkVersion)
                        || FLINK_VERSION_AFTER_OR_EQUALS_1_13.equals(flinkVersion)) {
                    args.add(FlinkConstants.FLINK_RUN); // run
                    args.add(FlinkConstants.FLINK_EXECUTION_TARGET); // -t
                    args.add(FlinkConstants.FLINK_YARN_PER_JOB); // yarn-per-job
                } else {
                    args.add(FlinkConstants.FLINK_RUN); // run
                    args.add(FlinkConstants.FLINK_RUN_MODE); // -m
                    args.add(FlinkConstants.FLINK_YARN_CLUSTER); // yarn-cluster
                }
                break;
            case APPLICATION:
                args.add(FlinkConstants.FLINK_RUN_APPLICATION); // run-application
                args.add(FlinkConstants.FLINK_EXECUTION_TARGET); // -t
                args.add(FlinkConstants.FLINK_YARN_APPLICATION); // yarn-application
                break;
            case LOCAL:
                args.add(FlinkConstants.FLINK_RUN); // run
                break;
            case STANDALONE:
                args.add(FlinkConstants.FLINK_RUN); // run
                break;
        }

        String others = flinkParameters.getOthers();

        // build args
        switch (deployMode) {
            case CLUSTER:
            case APPLICATION:
                int slot = flinkParameters.getSlot();
                if (slot > 0) {
                    args.add(FlinkConstants.FLINK_YARN_SLOT);
                    args.add(String.format("%d", slot)); // -ys
                }

                String appName = flinkParameters.getAppName();
                if (StringUtils.isNotEmpty(appName)) { // -ynm
                    args.add(FlinkConstants.FLINK_APP_NAME);
                    args.add(ArgsUtils.escape(appName));
                }

                // judge flink version, the parameter -yn has removed from flink 1.10
                if (flinkVersion == null || FLINK_VERSION_BEFORE_1_10.equals(flinkVersion)) {
                    int taskManager = flinkParameters.getTaskManager();
                    if (taskManager > 0) { // -yn
                        args.add(FlinkConstants.FLINK_TASK_MANAGE);
                        args.add(String.format("%d", taskManager));
                    }
                }
                String jobManagerMemory = flinkParameters.getJobManagerMemory();
                if (StringUtils.isNotEmpty(jobManagerMemory)) {
                    args.add(FlinkConstants.FLINK_JOB_MANAGE_MEM);
                    args.add(jobManagerMemory); // -yjm
                }

                String taskManagerMemory = flinkParameters.getTaskManagerMemory();
                if (StringUtils.isNotEmpty(taskManagerMemory)) { // -ytm
                    args.add(FlinkConstants.FLINK_TASK_MANAGE_MEM);
                    args.add(taskManagerMemory);
                }

                break;
            case LOCAL:
                break;
            case STANDALONE:
                break;
        }

        int parallelism = flinkParameters.getParallelism();
        if (parallelism > 0) {
            args.add(FlinkConstants.FLINK_PARALLELISM);
            args.add(String.format("%d", parallelism)); // -p
        }

        // If the job is submitted in attached mode, perform a best-effort cluster shutdown when the CLI is terminated
        // abruptly
        // The task status will be synchronized with the cluster job status
        args.add(FlinkConstants.FLINK_SHUTDOWN_ON_ATTACHED_EXIT); // -sae

        // -s -yqu -yat -yD -D
        if (StringUtils.isNotEmpty(others)) {
            args.add(others);
        }

        // determine yarn queue
        determinedYarnQueue(args, flinkParameters, deployMode, flinkVersion);
        ProgramType programType = flinkParameters.getProgramType();
        String mainClass = flinkParameters.getMainClass();
        if (programType != null && programType != ProgramType.PYTHON && StringUtils.isNotEmpty(mainClass)) {
            args.add(FlinkConstants.FLINK_MAIN_CLASS); // -c
            args.add(flinkParameters.getMainClass()); // main class
        }

        ResourceInfo mainJar = flinkParameters.getMainJar();
        if (mainJar != null) {
            // -py
            if (ProgramType.PYTHON == programType) {
                args.add(FlinkConstants.FLINK_PYTHON);
            }
            String name = mainJar.getResourceName();
            if (name == null && mainJar.getId() != null) {
                name = getResourceNameById(mainJar.getId());
            }
            args.add(name);
        }

        String mainArgs = flinkParameters.getMainArgs();
        if (StringUtils.isNotEmpty(mainArgs)) {
            args.add(mainArgs);
        }

        return args;
    }

    private void determinedYarnQueue(List<String> args, FlinkParameters flinkParameters,
            FlinkDeployMode deployMode, String flinkVersion) {
        switch (deployMode) {
            case CLUSTER:
                if (FLINK_VERSION_AFTER_OR_EQUALS_1_12.equals(flinkVersion)
                        || FLINK_VERSION_AFTER_OR_EQUALS_1_13.equals(flinkVersion)) {
                    doAddQueue(args, flinkParameters, FlinkConstants.FLINK_YARN_QUEUE_FOR_TARGETS);
                } else {
                    doAddQueue(args, flinkParameters, FlinkConstants.FLINK_YARN_QUEUE_FOR_MODE);
                }
                break;
            case APPLICATION:
                doAddQueue(args, flinkParameters, FlinkConstants.FLINK_YARN_QUEUE_FOR_TARGETS);
                break;
        }
    }

    private void doAddQueue(List<String> args, FlinkParameters flinkParameters, String option) {
        String others = flinkParameters.getOthers();
        if (StringUtils.isEmpty(others) || !others.contains(option)) {
            String yarnQueue = flinkParameters.getYarnQueue();
            if (StringUtils.isNotEmpty(yarnQueue)) {
                switch (option) {
                    case FlinkConstants.FLINK_YARN_QUEUE_FOR_TARGETS:
                        args.add(String.format(FlinkConstants.FLINK_YARN_QUEUE_FOR_TARGETS + "=%s", yarnQueue));
                        break;
                    case FlinkConstants.FLINK_YARN_QUEUE_FOR_MODE:
                        args.add(FlinkConstants.FLINK_YARN_QUEUE_FOR_MODE);
                        args.add(yarnQueue);
                        break;
                }
            }
        }
    }

    private String getConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_FLINK_NODE_TYPE_AS);
        String defaultConvertType = CodeProgramType.DIDE_SHELL.name();
        return getConverterType(convertType, defaultConvertType);
    }

    private String getSqlConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_FLINK_SQL_NODE_TYPE_AS);
        if (convertType == null) {
            convertType = properties.getProperty(Constants.CONVERTER_TARGET_FLINK_NODE_TYPE_AS);
        }
        String defaultConvertType = CodeProgramType.FLINK_SQL_BATCH.name();
        return getConverterType(convertType, defaultConvertType);
    }

    private String getPythonConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_FLINK_PYTHON_NODE_TYPE_AS);
        String defaultConvertType = CodeProgramType.PYTHON.name();
        return getConverterType(convertType, defaultConvertType);
    }
}
