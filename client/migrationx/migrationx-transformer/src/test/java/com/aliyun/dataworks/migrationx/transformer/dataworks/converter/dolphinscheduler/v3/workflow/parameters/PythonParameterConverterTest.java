/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */


package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableScopeType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.python.PythonParameters;
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
public class PythonParameterConverterTest {

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
        properties.setProperty(Constants.CONVERTER_TARGET_PYTHON_NODE_TYPE_AS, CodeProgramType.PYTHON.name());
        
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("test-workflow");
        
        dagData = new DagData();
        dagData.setProcessDefinition(new ProcessDefinition());
        
        taskDefinition = new TaskDefinition();
        taskDefinition.setCode(1001L);
        taskDefinition.setTaskType(TaskType.PYTHON.name());
        taskDefinition.setName("python-task");
        
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testConvertBasicPythonParameter() {
        PythonParameters pythonParams = new PythonParameters();
        pythonParams.setRawScript("print('Hello World')");
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(pythonParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            PythonParameterConverter converter = new PythonParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setFileResources(new ArrayList<>());
            specNode.setId("node-1");
            specNode.setName("basic-python-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.PYTHON.getName(), script.getRuntime().getCommand());
            Assert.assertEquals(CodeProgramType.PYTHON.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("print('Hello World')"));
        }
    }

    @Test
    public void testConvertPythonParameterWithVariables() {
        PythonParameters pythonParams = new PythonParameters();
        pythonParams.setRawScript("date = '${bizdate}'\nprint(f'Processing data for {date}')");
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(pythonParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            PythonParameterConverter converter = new PythonParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setFileResources(new ArrayList<>());
            specNode.setId("node-1");
            specNode.setName("python-with-vars-node");
            
            SpecVariable var = new SpecVariable();
            var.setId("bizdate-var");
            var.setName("bizdate");
            var.setValue("20240101");
            var.setType(VariableType.CONSTANT);
            var.setScope(VariableScopeType.NODE_PARAMETER);
            List<SpecVariable> variables = new ArrayList<>();
            variables.add(var);
            //specNode.setVariables(variables);
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("${bizdate}"));
        }
    }

    @Test
    public void testConvertPythonParameterWithResources() {
        PythonParameters pythonParams = new PythonParameters();
        pythonParams.setRawScript("from utils import process_data\nprocess_data()");
        
        ResourceInfo resource = new ResourceInfo();
        resource.setId(1);
        resource.setResourceName("utils.py");
        List<ResourceInfo> resources = new ArrayList<>();
        resources.add(resource);
        pythonParams.setResourceList(resources);
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(pythonParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            PythonParameterConverter converter = new PythonParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setFileResources(new ArrayList<>());
            specNode.setId("node-1");
            specNode.setName("python-with-resources-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("from utils import process_data"));
            Assert.assertTrue(content.contains("process_data()"));
        }
    }

    @Test
    public void testConvertPythonParameterWithCustomNodeType() {
        PythonParameters pythonParams = new PythonParameters();
        pythonParams.setRawScript("import pandas as pd\ndf = pd.read_csv('data.csv')");
        
        properties.setProperty(Constants.CONVERTER_TARGET_PYTHON_NODE_TYPE_AS, CodeProgramType.EMR_SHELL.name());
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(pythonParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            PythonParameterConverter converter = new PythonParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setFileResources(new ArrayList<>());
            specNode.setId("node-1");
            specNode.setName("custom-python-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.EMR_SHELL.getName(), script.getRuntime().getCommand());
            Assert.assertEquals(CodeProgramType.EMR_SHELL.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("import pandas as pd"));
            Assert.assertTrue(content.contains("df = pd.read_csv('data.csv')"));
        }
    }

    @Test(expected = RuntimeException.class)
    public void testConvertWithInvalidTaskParams() {
        taskDefinition.setTaskParams("invalid-json");
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            PythonParameterConverter converter = new PythonParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("invalid-node");
            
            converter.convertParameter(specNode);
        }
    }
} 