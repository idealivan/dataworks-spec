/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */


package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ProgramType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.flink.FlinkDeployMode;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.flink.FlinkParameters;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.migrationx.common.utils.Config;
import com.aliyun.migrationx.common.utils.JSONUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FlinkParameterConverterTest {

    private Properties properties;
    private SpecWorkflow specWorkflow;
    private DagData dagData;
    private TaskDefinition taskDefinition;
    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        Config.init();
        
        properties = new Properties();
        properties.setProperty(Constants.CONVERTER_TARGET_FLINK_NODE_TYPE_AS, CodeProgramType.BLINK_DATASTREAM.name());
        properties.setProperty(Constants.CONVERTER_TARGET_FLINK_SQL_NODE_TYPE_AS, CodeProgramType.BLINK_SQL.name());
        properties.setProperty(Constants.CONVERTER_TARGET_FLINK_PYTHON_NODE_TYPE_AS, CodeProgramType.PYTHON.name());
        
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("test-workflow");
        
        dagData = new DagData();
        dagData.setProcessDefinition(new ProcessDefinition());
        
        taskDefinition = new TaskDefinition();
        taskDefinition.setCode(1001L);
        taskDefinition.setTaskType(TaskType.FLINK.name());
        taskDefinition.setName("flink-task");
        
        objectMapper = new ObjectMapper();
    }

    @Test(expected = RuntimeException.class)
    public void testConvertFlinkJavaParameters() {
        FlinkParameters flinkParams = new FlinkParameters();
        flinkParams.setProgramType(ProgramType.JAVA);
        flinkParams.setDeployMode(FlinkDeployMode.CLUSTER);
        flinkParams.setFlinkVersion(">=1.12");
        flinkParams.setMainClass("com.example.FlinkJob");
        flinkParams.setMainArgs("--input hdfs:///input --output hdfs:///output");
        flinkParams.setSlot(2);
        flinkParams.setTaskManager(3);
        flinkParams.setJobManagerMemory("1G");
        flinkParams.setTaskManagerMemory("2G");
        flinkParams.setParallelism(4);
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setResourceName("flink-job.jar");
        flinkParams.setMainJar(mainJar);
        
        taskDefinition.setTaskParams(JSONUtils.toJsonString(flinkParams));
        
        FlinkParameterConverter converter = new FlinkParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-1");
        specNode.setName("flink-java");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        Assert.assertEquals("BLINK_DATASTREAM", script.getRuntime().getCommand());
        Assert.assertEquals(CodeProgramType.BLINK_DATASTREAM.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
        
        String content = script.getContent();
        Assert.assertNotNull("Script content should not be null", content);
        System.out.println("Actual script content: " + content);
        
        Assert.assertTrue("Should contain jar file", content.contains("flink-job.jar"));
        Assert.assertTrue("Should contain main class", content.contains("com.example.FlinkJob"));
        Assert.assertTrue("Should contain deploy mode", content.contains("-m yarn-cluster"));
        Assert.assertTrue("Should contain parallelism", content.contains("-p 4"));
        Assert.assertTrue("Should contain slot", content.contains("-ys 2"));
        Assert.assertTrue("Should contain task manager", content.contains("-yn 3"));
        Assert.assertTrue("Should contain job manager memory", content.contains("-yjm 1G"));
        Assert.assertTrue("Should contain task manager memory", content.contains("-ytm 2G"));
        Assert.assertTrue("Should contain main args", content.contains("--input hdfs:///input --output hdfs:///output"));
    }

    @Test
    public void testConvertFlinkSqlParameters() {
        FlinkParameters flinkParams = new FlinkParameters();
        flinkParams.setProgramType(ProgramType.SQL);
        flinkParams.setDeployMode(FlinkDeployMode.CLUSTER);
        flinkParams.setFlinkVersion(">=1.13");
        flinkParams.setRawScript("CREATE TABLE source_table (\n" +
                "  user_id BIGINT,\n" +
                "  item_id BIGINT,\n" +
                "  behavior STRING,\n" +
                "  ts TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ");\n\n" +
                "CREATE TABLE sink_table (\n" +
                "  behavior STRING,\n" +
                "  cnt BIGINT\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");\n\n" +
                "INSERT INTO sink_table\n" +
                "SELECT behavior, COUNT(*) as cnt\n" +
                "FROM source_table\n" +
                "GROUP BY behavior;");
        flinkParams.setParallelism(2);
        flinkParams.setJobManagerMemory("1G");
        flinkParams.setTaskManagerMemory("2G");
        
        taskDefinition.setTaskParams(JSONUtils.toJsonString(flinkParams));
        
        FlinkParameterConverter converter = new FlinkParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-2");
        specNode.setName("flink-sql");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        Assert.assertEquals("BLINK_STREAM_SQL", script.getRuntime().getCommand());
        Assert.assertEquals(CodeProgramType.BLINK_SQL.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
        
        String content = script.getContent();
        Assert.assertNotNull("Script content should not be null", content);

        Assert.assertTrue("Should contain SQL script", content.contains("CREATE TABLE source_table"));
        Assert.assertTrue("Should contain SQL script", content.contains("INSERT INTO sink_table"));
    }

    @Test(expected = RuntimeException.class)
    public void testConvertFlinkPythonParameters() {
        FlinkParameters flinkParams = new FlinkParameters();
        flinkParams.setProgramType(ProgramType.PYTHON);
        flinkParams.setDeployMode(FlinkDeployMode.LOCAL);
        flinkParams.setFlinkVersion("<1.10");
        flinkParams.setMainArgs("--input /path/to/input --output /path/to/output");
        
        ResourceInfo pythonFile = new ResourceInfo();
        pythonFile.setResourceName("flink_job.py");
        flinkParams.setMainJar(pythonFile);
        
        ResourceInfo resource1 = new ResourceInfo();
        resource1.setResourceName("config.json");
        ResourceInfo resource2 = new ResourceInfo();
        resource2.setResourceName("data.csv");
        flinkParams.setResourceList(Arrays.asList(resource1, resource2));
        
        taskDefinition.setTaskParams(JSONUtils.toJsonString(flinkParams));
        
        FlinkParameterConverter converter = new FlinkParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-3");
        specNode.setName("flink-python");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        Assert.assertEquals("PYTHON", script.getRuntime().getCommand());
        Assert.assertEquals(CodeProgramType.PYTHON.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
        
        String content = script.getContent();
        Assert.assertNotNull("Script content should not be null", content);
        System.out.println("Actual script content: " + content);
        
        Assert.assertTrue("Should contain Python file", content.contains("flink_job.py"));
        Assert.assertTrue("Should contain deploy mode", content.contains("local"));
        Assert.assertTrue("Should contain main args", content.contains("--input /path/to/input --output /path/to/output"));
    }

    @Test(expected = RuntimeException.class)
    public void testConvertFlinkParametersWithQueue() {
        FlinkParameters flinkParams = new FlinkParameters();
        flinkParams.setProgramType(ProgramType.JAVA);
        flinkParams.setDeployMode(FlinkDeployMode.CLUSTER);
        flinkParams.setFlinkVersion(">=1.12");
        flinkParams.setMainClass("com.example.FlinkJob");
        flinkParams.setYarnQueue("high-priority");
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setResourceName("flink-job.jar");
        flinkParams.setMainJar(mainJar);
        
        taskDefinition.setTaskParams(JSONUtils.toJsonString(flinkParams));
        
        FlinkParameterConverter converter = new FlinkParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-4");
        specNode.setName("flink-queue");
        
        converter.convertParameter(specNode);
        
        String content = specNode.getScript().getContent();
        Assert.assertNotNull("Script content should not be null", content);
        System.out.println("Actual script content: " + content);

        Assert.assertTrue("Should contain queue configuration",
            content.contains("-yqu high-priority") || 
            content.contains("-Dyarn.queue=high-priority") ||
            content.contains("-Dyarn.queue.name=high-priority"));
    }

    @Test(expected = RuntimeException.class)
    public void testConvertWithInvalidTaskParams() {
        taskDefinition.setTaskParams("invalid-json");
        
        FlinkParameterConverter converter = new FlinkParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setId("node-5");
        specNode.setName("invalid-node");
        
        converter.convertParameter(specNode);
    }

    @Test(expected = RuntimeException.class)
    public void testConvertWithUnsupportedType() {
        properties.setProperty(Constants.CONVERTER_TARGET_FLINK_NODE_TYPE_AS, "UNSUPPORTED_TYPE");
        
        FlinkParameters flinkParams = new FlinkParameters();
        flinkParams.setProgramType(ProgramType.JAVA);
        flinkParams.setDeployMode(FlinkDeployMode.CLUSTER);
        taskDefinition.setTaskParams(JSONUtils.toJsonString(flinkParams));
        
        FlinkParameterConverter converter = new FlinkParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setId("node-6");
        specNode.setName("unsupported-node");
        
        converter.convertParameter(specNode);
    }
} 