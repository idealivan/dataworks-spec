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
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.mr.MapReduceParameters;
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
public class MrParameterConverterTest {

    private Properties properties;
    private SpecWorkflow specWorkflow;
    private DagData dagData;
    private TaskDefinition taskDefinition;
    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        Config.init();
        
        properties = new Properties();
        properties.setProperty(Constants.CONVERTER_TARGET_MR_NODE_TYPE_AS, CodeProgramType.EMR_MR.name());
        
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("test-workflow");
        
        dagData = new DagData();
        dagData.setProcessDefinition(new ProcessDefinition());
        
        taskDefinition = new TaskDefinition();
        taskDefinition.setCode(1001L);
        taskDefinition.setTaskType(TaskType.MR.name());
        taskDefinition.setName("mr-task");
        
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testConvertEmrMrParametersWithBasicConfig() {
        MapReduceParameters mrParams = new MapReduceParameters();
        mrParams.setProgramType(ProgramType.JAVA);
        mrParams.setMainClass("com.example.MainClass");
        mrParams.setMainArgs("input_path output_path");
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setResourceName("mapreduce-job.jar");
        mrParams.setMainJar(mainJar);
        
        taskDefinition.setTaskParams(JSONUtils.toJsonString(mrParams));
        
        MrParameterConverter converter = new MrParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-1");
        specNode.setName("emr-mr-basic");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        Assert.assertEquals("EMR_MR", script.getRuntime().getCommand());
        Assert.assertEquals(CodeProgramType.EMR_MR.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
        
        String content = script.getContent();
        Assert.assertNotNull("Script content should not be null", content);
        System.out.println("Actual script content: " + content);
        Assert.assertTrue("Should contain jar file", content.contains("mapreduce-job.jar"));
        Assert.assertTrue("Should contain main class", content.contains("com.example.MainClass"));
        Assert.assertTrue("Should contain main args", content.contains("input_path output_path"));
    }

    @Test
    public void testConvertEmrMrParametersWithFullConfig() {
        MapReduceParameters mrParams = new MapReduceParameters();
        mrParams.setProgramType(ProgramType.JAVA);
        mrParams.setMainClass("com.example.FullConfigJob");
        mrParams.setAppName("full-config-job");
        mrParams.setYarnQueue("high-priority");
        mrParams.setMainArgs("arg1 arg2 arg3");
        mrParams.setOthers("-D mapred.job.priority=HIGH -D mapred.job.name=CustomName");
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setResourceName("full-job.jar");
        mrParams.setMainJar(mainJar);
        
        taskDefinition.setTaskParams(JSONUtils.toJsonString(mrParams));
        
        MrParameterConverter converter = new MrParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-2");
        specNode.setName("emr-mr-full");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        
        String content = script.getContent();
        Assert.assertNotNull("Script content should not be null", content);
        System.out.println("Actual script content: " + content);
        Assert.assertTrue("Should contain jar file", content.contains("full-job.jar"));
        Assert.assertTrue("Should contain main class", content.contains("com.example.FullConfigJob"));
        
        // Check for app name with more flexible approach
        boolean containsAppName = content.contains("-D mapreduce.job.name=full-config-job") || 
                                 content.contains("-Dmapreduce.job.name=full-config-job") ||
                                 content.contains("-D\"mapreduce.job.name=full-config-job\"") ||
                                 content.contains("-D mapreduce.job.name=\"full-config-job\"");
        Assert.assertTrue("Should contain app name", containsAppName);
        
        Assert.assertTrue("Should contain queue name", content.contains("-D mapreduce.job.queuename=high-priority") || 
                                                     content.contains("-Dmapreduce.job.queuename=high-priority"));
        Assert.assertTrue("Should contain custom properties", content.contains("-D mapred.job.priority=HIGH"));
        Assert.assertTrue("Should contain main args", content.contains("arg1 arg2 arg3"));
    }

    @Test
    public void testConvertOdpsMrParameters() {
        properties.setProperty(Constants.CONVERTER_TARGET_MR_NODE_TYPE_AS, CodeProgramType.ODPS_MR.name());
        
        MapReduceParameters mrParams = new MapReduceParameters();
        mrParams.setProgramType(ProgramType.JAVA);
        mrParams.setMainClass("com.example.OdpsJob");
        mrParams.setMainArgs("odps_input odps_output");
        
        ResourceInfo mainJar = new ResourceInfo();
        mainJar.setResourceName("odps-job.jar");
        mrParams.setMainJar(mainJar);
        
        ResourceInfo resource1 = new ResourceInfo();
        resource1.setResourceName("data.txt");
        ResourceInfo resource2 = new ResourceInfo();
        resource2.setResourceName("config.json");
        mrParams.setResourceList(Arrays.asList(resource1, resource2));
        
        taskDefinition.setTaskParams(JSONUtils.toJsonString(mrParams));
        
        MrParameterConverter converter = new MrParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-3");
        specNode.setName("odps-mr");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        Assert.assertEquals("ODPS_MR", script.getRuntime().getCommand());
        Assert.assertEquals(CodeProgramType.ODPS_MR.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
        
        String content = script.getContent();
        Assert.assertNotNull("Script content should not be null", content);
        System.out.println("Actual script content: " + content);
        
        Assert.assertTrue("Should contain resource references", content.contains("data.txt"));
        Assert.assertTrue("Should contain resource references", content.contains("config.json"));
        Assert.assertTrue("Should contain main jar", content.contains("odps-job.jar"));
        Assert.assertTrue("Should contain main class", content.contains("com.example.OdpsJob"));
        Assert.assertTrue("Should contain main args", content.contains("odps_input odps_output"));
        Assert.assertTrue("Should contain version info", content.contains("\"version\":\"2.x\""));
        Assert.assertTrue("Should contain language info", content.contains("\"language\":\"java\""));
    }

    @Test(expected = RuntimeException.class)
    public void testConvertWithInvalidTaskParams() {
        taskDefinition.setTaskParams("invalid-json");
        
        MrParameterConverter converter = new MrParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setId("node-4");
        specNode.setName("invalid-node");
        
        converter.convertParameter(specNode);
    }

    @Test(expected = RuntimeException.class)
    public void testConvertWithUnsupportedType() {
        properties.setProperty(Constants.CONVERTER_TARGET_MR_NODE_TYPE_AS, "UNSUPPORTED_TYPE");
        
        MapReduceParameters mrParams = new MapReduceParameters();
        mrParams.setProgramType(ProgramType.JAVA);
        taskDefinition.setTaskParams(JSONUtils.toJsonString(mrParams));
        
        MrParameterConverter converter = new MrParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setId("node-5");
        specNode.setName("unsupported-node");
        
        converter.convertParameter(specNode);
    }
} 