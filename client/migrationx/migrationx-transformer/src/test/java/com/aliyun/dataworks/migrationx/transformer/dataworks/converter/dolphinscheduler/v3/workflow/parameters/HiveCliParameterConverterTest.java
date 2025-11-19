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
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.hivecli.HiveCliParameters;
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
public class HiveCliParameterConverterTest {

    private Properties properties;
    private SpecWorkflow specWorkflow;
    private DagData dagData;
    private TaskDefinition taskDefinition;
    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        Config.init();
        
        properties = new Properties();
        properties.setProperty(Constants.CONVERTER_TARGET_HIVE_CLI_NODE_TYPE_AS, CodeProgramType.EMR_HIVE.name());
        
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("test-workflow");
        
        dagData = new DagData();
        dagData.setProcessDefinition(new ProcessDefinition());
        
        taskDefinition = new TaskDefinition();
        taskDefinition.setCode(1001L);
        taskDefinition.setTaskType(TaskType.HIVECLI.name());
        taskDefinition.setName("hive-task");
        
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testConvertEmrHiveCliParametersWithBasicConfig() {
        HiveCliParameters hiveParams = new HiveCliParameters();
        hiveParams.setHiveCliTaskExecutionType("SCRIPT");
        hiveParams.setHiveCliOptions("-v");
        hiveParams.setHiveSqlScript("SELECT * FROM test_table LIMIT 10;");
        
        taskDefinition.setTaskParams(JSONUtils.toJsonString(hiveParams));
        
        HiveCliParameterConverter converter = new HiveCliParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-1");
        specNode.setName("emr-hive-basic");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        Assert.assertEquals("EMR_HIVE", script.getRuntime().getCommand());
        Assert.assertEquals(CodeProgramType.EMR_HIVE.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
        
        String content = script.getContent();
        Assert.assertNotNull("Script content should not be null", content);
        Assert.assertTrue("Should contain HiveCli script", content.contains("SELECT * FROM test_table LIMIT 10;"));
    }

    @Test
    public void testConvertEmrHiveCliParametersWithFileScript() {
        HiveCliParameters hiveParams = new HiveCliParameters();
        hiveParams.setHiveCliTaskExecutionType("FILE");
        hiveParams.setHiveCliOptions("-v --hiveconf mapred.job.queue.name=default");
        
        ResourceInfo scriptFile = new ResourceInfo();
        scriptFile.setResourceName("query.hql");
        hiveParams.setResourceList(java.util.Collections.singletonList(scriptFile));
        
        taskDefinition.setTaskParams(JSONUtils.toJsonString(hiveParams));
        
        HiveCliParameterConverter converter = new HiveCliParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-2");
        specNode.setName("emr-hive-file");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        
        String content = script.getContent();
        Assert.assertNotNull("Script content should not be null", content);
        Assert.assertTrue("Should contain script file reference", content.contains("query.hql"));
        Assert.assertTrue("Should contain HiveCli options", content.contains("-v"));
        Assert.assertTrue("Should contain queue configuration", 
            content.contains("--hiveconf mapred.job.queue.name=default"));
    }

    @Test
    public void testConvertOdpsHiveCliParameters() {
        properties.setProperty(Constants.CONVERTER_TARGET_HIVE_CLI_NODE_TYPE_AS, CodeProgramType.ODPS_SQL.name());
        
        HiveCliParameters hiveParams = new HiveCliParameters();
        hiveParams.setHiveCliTaskExecutionType("SCRIPT");
        hiveParams.setHiveSqlScript("SELECT * FROM odps_table WHERE dt='${bizdate}';");
        
        taskDefinition.setTaskParams(JSONUtils.toJsonString(hiveParams));
        
        HiveCliParameterConverter converter = new HiveCliParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-3");
        specNode.setName("odps-hive");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        Assert.assertEquals("ODPS_SQL", script.getRuntime().getCommand());
        Assert.assertEquals(CodeProgramType.ODPS_SQL.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
        
        String content = script.getContent();
        Assert.assertNotNull("Script content should not be null", content);
        Assert.assertTrue("Should contain SQL script", content.contains("SELECT * FROM odps_table WHERE dt='${bizdate}';"));
    }

    @Test
    public void testConvertHiveCliParametersWithResources() {
        HiveCliParameters hiveParams = new HiveCliParameters();
        hiveParams.setHiveCliTaskExecutionType("SCRIPT");
        hiveParams.setHiveSqlScript("SELECT * FROM test_table WHERE id IN (SELECT id FROM ${UDF_TABLE});");
        hiveParams.setHiveCliOptions("--hiveconf hive.execution.engine=mr");
        
        ResourceInfo resource1 = new ResourceInfo();
        resource1.setResourceName("udf.jar");
        ResourceInfo resource2 = new ResourceInfo();
        resource2.setResourceName("data.txt");
        hiveParams.setResourceList(Arrays.asList(resource1, resource2));
        
        taskDefinition.setTaskParams(JSONUtils.toJsonString(hiveParams));
        
        HiveCliParameterConverter converter = new HiveCliParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-4");
        specNode.setName("hive-resources");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        
        String content = script.getContent();
        Assert.assertNotNull("Script content should not be null", content);
    }

    @Test(expected = RuntimeException.class)
    public void testConvertWithInvalidTaskParams() {
        taskDefinition.setTaskParams("invalid-json");
        
        HiveCliParameterConverter converter = new HiveCliParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setId("node-5");
        specNode.setName("invalid-node");
        
        converter.convertParameter(specNode);
    }

    @Test(expected = RuntimeException.class)
    public void testConvertWithUnsupportedType() {
        properties.setProperty(Constants.CONVERTER_TARGET_HIVE_CLI_NODE_TYPE_AS, "UNSUPPORTED_TYPE");
        
        HiveCliParameters hiveParams = new HiveCliParameters();
        hiveParams.setHiveCliTaskExecutionType("SCRIPT");
        hiveParams.setHiveSqlScript("SELECT 1;");
        taskDefinition.setTaskParams(JSONUtils.toJsonString(hiveParams));
        
        HiveCliParameterConverter converter = new HiveCliParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setId("node-6");
        specNode.setName("unsupported-node");
        
        converter.convertParameter(specNode);
    }
} 