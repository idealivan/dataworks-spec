/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */


package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters;

import java.util.ArrayList;
import java.util.Properties;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ProgramType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.spark.SparkConstants;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.spark.SparkParameters;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.migrationx.common.utils.Config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SparkParameterConverterTest {

    @Mock
    private DolphinSchedulerV3Context mockContext;

    private Properties properties;
    private SpecWorkflow specWorkflow;
    private DagData dagData;
    private TaskDefinition taskDefinition;
    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        Config.init();
        
        properties = new Properties();
        properties.setProperty(Constants.CONVERTER_TARGET_SPARK_SUBMIT_TYPE_AS, CodeProgramType.EMR_SPARK_SHELL.name());
        properties.setProperty(Constants.CONVERTER_TARGET_SPARK_SQL_TYPE_AS, CodeProgramType.EMR_SPARK_SQL.name());
        
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("test-workflow");
        
        dagData = new DagData();
        dagData.setProcessDefinition(new ProcessDefinition());

            
        taskDefinition = new TaskDefinition();
        taskDefinition.setCode(1001L);
        taskDefinition.setTaskType(TaskType.SPARK.name());
        taskDefinition.setName("spark-task");
        
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testConvertSparkJavaParameter() {
        SparkParameters sparkParams = new SparkParameters();
        sparkParams.setProgramType(ProgramType.JAVA);
        sparkParams.setMainClass("com.example.SparkApp");
        sparkParams.setDeployMode("cluster");
        sparkParams.setDriverCores(2);
        sparkParams.setDriverMemory("4g");
        sparkParams.setNumExecutors(3);
        sparkParams.setExecutorCores(2);
        sparkParams.setExecutorMemory("8g");
        sparkParams.setAppName("test-spark-app");
        sparkParams.setYarnQueue("default");
        sparkParams.setMainArgs("arg1 arg2");
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setId(1);
        mainJar.setResourceName("spark-app.jar");
        sparkParams.setMainJar(mainJar);
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(sparkParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            SparkParameterConverter converter = new SparkParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setFileResources(new ArrayList<>());
            specNode.setId("node-1");
            specNode.setName("spark-java-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.EMR_SPARK_SHELL.getName(), script.getRuntime().getCommand());
            Assert.assertEquals(CodeProgramType.EMR_SPARK_SHELL.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("spark-submit"));
            Assert.assertTrue(content.contains("--master yarn"));
            Assert.assertTrue(content.contains("--deploy-mode cluster"));
            Assert.assertTrue(content.contains("spark-app.jar"));
            Assert.assertTrue(content.contains("arg1 arg2"));
        }
    }

    @Test
    public void testConvertSparkSqlParameter() {
        SparkParameters sparkParams = new SparkParameters();
        sparkParams.setProgramType(ProgramType.SQL);
        sparkParams.setSqlExecutionType(SparkConstants.TYPE_SCRIPT);
        sparkParams.setRawScript("SELECT * FROM test_table WHERE dt = '${bizdate}'");
        sparkParams.setDeployMode("client");
        sparkParams.setDriverMemory("2g");
        sparkParams.setExecutorMemory("4g");
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(sparkParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            SparkParameterConverter converter = new SparkParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("spark-sql-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.EMR_SPARK_SQL.getName(), script.getRuntime().getCommand());
            Assert.assertEquals(CodeProgramType.EMR_SPARK_SQL.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("SELECT * FROM test_table WHERE dt = '${bizdate}'"));
        }
    }

    @Test
    public void testConvertSparkPythonParameter() {
        SparkParameters sparkParams = new SparkParameters();
        sparkParams.setProgramType(ProgramType.PYTHON);
        sparkParams.setDeployMode("cluster");
        sparkParams.setDriverMemory("2g");
        sparkParams.setExecutorMemory("4g");
        
        ResourceInfo mainScript = new ResourceInfo();
        mainScript.setId(1);
        mainScript.setResourceName("spark_script.py");
        sparkParams.setMainJar(mainScript);
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(sparkParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            SparkParameterConverter converter = new SparkParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setFileResources(new ArrayList<>());
            specNode.setId("node-1");
            specNode.setName("spark-python-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.EMR_SPARK_SHELL.getName(), script.getRuntime().getCommand());
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("spark-submit"));
            Assert.assertTrue(content.contains("--master yarn"));
            Assert.assertTrue(content.contains("--deploy-mode cluster"));
            Assert.assertTrue(content.contains("spark_script.py"));
        }
    }

    @Test
    public void testConvertOdpsSparkParameter() {
        SparkParameters sparkParams = new SparkParameters();
        sparkParams.setProgramType(ProgramType.JAVA);
        sparkParams.setMainClass("com.example.OdpsSparkApp");
        sparkParams.setDriverCores(2);
        sparkParams.setDriverMemory("4g");
        sparkParams.setNumExecutors(3);
        sparkParams.setExecutorCores(2);
        sparkParams.setExecutorMemory("8g");
        sparkParams.setMainArgs("arg1 arg2");
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setId(1);
        mainJar.setResourceName("odps-spark-app.jar");
        sparkParams.setMainJar(mainJar);
        
        properties.setProperty(Constants.CONVERTER_TARGET_SPARK_SUBMIT_TYPE_AS, CodeProgramType.ODPS_SPARK.name());
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(sparkParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            SparkParameterConverter converter = new SparkParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setFileResources(new ArrayList<>());
            specNode.setId("node-1");
            specNode.setName("odps-spark-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.ODPS_SPARK.getName(), script.getRuntime().getCommand());
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("\"mainJar\":\"odps-spark-app.jar\""));
            Assert.assertTrue(content.contains("\"mainClass\":\"com.example.OdpsSparkApp\""));
            Assert.assertTrue(content.contains("\"version\":\"2.x\""));
            Assert.assertTrue(content.contains("\"language\":\"java\""));
            Assert.assertTrue(content.contains("\"args\":\"arg1 arg2\""));
            Assert.assertTrue(content.contains("spark.driver.cores=2"));
            Assert.assertTrue(content.contains("spark.driver.memory=4g"));
            Assert.assertTrue(content.contains("spark.executor.instances=3"));
            Assert.assertTrue(content.contains("spark.executor.cores=2"));
            Assert.assertTrue(content.contains("spark.executor.memory=8g"));
        }
    }


} 