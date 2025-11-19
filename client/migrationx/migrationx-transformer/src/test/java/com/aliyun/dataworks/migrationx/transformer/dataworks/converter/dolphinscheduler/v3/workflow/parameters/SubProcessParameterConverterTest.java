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
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.subprocess.SubProcessParameters;
import com.aliyun.migrationx.common.utils.Config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SubProcessParameterConverterTest {

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
        
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("test-workflow");
        
        dagData = new DagData();
        
        taskDefinition = new TaskDefinition();
        taskDefinition.setCode(1001L);
        taskDefinition.setTaskType(TaskType.SUB_PROCESS.name());
        taskDefinition.setName("subprocess-task");
        
        objectMapper = new ObjectMapper();
    }


    @Test
    public void testConvertParameterWithValidSubProcessParameters() {
        SubProcessParameters subProcessParams = new SubProcessParameters();
        subProcessParams.setProcessDefinitionCode(2001L);
        
        ObjectNode taskParams = objectMapper.createObjectNode();
        taskParams.set("processDefinitionCode", objectMapper.valueToTree(subProcessParams.getProcessDefinitionCode()));
        taskDefinition.setTaskParams(taskParams.toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            Map<Long, Object> subProcessCodeNodeMap = new HashMap<>();
            when(mockContext.getSubProcessCodeNodeMap()).thenReturn(subProcessCodeNodeMap);
            
            SubProcessParameterConverter converter = new SubProcessParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("subprocess-node");
            
            SpecVariable var1 = new SpecVariable();
            var1.setId("var1-id");
            var1.setName("var1");
            var1.setValue("${var1}");
            var1.setType(VariableType.CONSTANT);
            var1.setScope(VariableScopeType.NODE_PARAMETER);
            List<SpecVariable> variables = new ArrayList<>();
            variables.add(var1);
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.SUB_PROCESS.getName(), script.getRuntime().getCommand());
            Assert.assertEquals(CodeProgramType.SUB_PROCESS.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
            
            Assert.assertNotNull(script.getParameters());
            
            verify(mockContext).getSubProcessCodeNodeMap();
            Assert.assertEquals(specNode, subProcessCodeNodeMap.get(2001L));
        }
    }
    
    @Test
    public void testConvertParameterWithNoParameters() {
        SubProcessParameters subProcessParams = new SubProcessParameters();
        subProcessParams.setProcessDefinitionCode(2001L);
        
        ObjectNode taskParams = objectMapper.createObjectNode();
        taskParams.set("processDefinitionCode", objectMapper.valueToTree(subProcessParams.getProcessDefinitionCode()));
        taskDefinition.setTaskParams(taskParams.toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            Map<Long, Object> subProcessCodeNodeMap = new HashMap<>();
            when(mockContext.getSubProcessCodeNodeMap()).thenReturn(subProcessCodeNodeMap);
            
            SubProcessParameterConverter converter = new SubProcessParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("subprocess-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.SUB_PROCESS.getName(), script.getRuntime().getCommand());
            
            Assert.assertNotNull(script.getParameters());
            Assert.assertTrue(script.getParameters().isEmpty());
            
            verify(mockContext).getSubProcessCodeNodeMap();
            Assert.assertEquals(specNode, subProcessCodeNodeMap.get(2001L));
        }
    }
    
    @Test
    public void testConvertParameterWithOutputVariables() {
        SubProcessParameters subProcessParams = new SubProcessParameters();
        subProcessParams.setProcessDefinitionCode(2001L);
        
        ObjectNode taskParams = objectMapper.createObjectNode();
        taskParams.set("processDefinitionCode", objectMapper.valueToTree(subProcessParams.getProcessDefinitionCode()));
        taskDefinition.setTaskParams(taskParams.toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            Map<Long, Object> subProcessCodeNodeMap = new HashMap<>();
            when(mockContext.getSubProcessCodeNodeMap()).thenReturn(subProcessCodeNodeMap);
            
            SubProcessParameterConverter converter = new SubProcessParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("subprocess-node");
            
            List<SpecVariable> variables = new ArrayList<>();
            
            SpecVariable inputVar = new SpecVariable();
            inputVar.setId("input-id");
            inputVar.setName("input");
            inputVar.setValue("input-value");
            inputVar.setType(VariableType.CONSTANT);
            inputVar.setScope(VariableScopeType.NODE_PARAMETER);
            variables.add(inputVar);
            
            SpecVariable outputVar = new SpecVariable();
            outputVar.setId("output-id");
            outputVar.setName("output");
            outputVar.setValue("output-value");
            outputVar.setType(VariableType.NODE_OUTPUT);
            outputVar.setScope(VariableScopeType.NODE_CONTEXT);
            variables.add(outputVar);
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            
            Assert.assertNotNull(script.getParameters());

            verify(mockContext).getSubProcessCodeNodeMap();
            Assert.assertEquals(specNode, subProcessCodeNodeMap.get(2001L));
        }
    }
    
    @Test(expected = RuntimeException.class)
    public void testConvertParameterWithInvalidTaskParams() {
        taskDefinition.setTaskParams("invalid-json");
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);

            SubProcessParameterConverter converter = new SubProcessParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("subprocess-node");
            
            converter.convertParameter(specNode);
        }
    }
} 