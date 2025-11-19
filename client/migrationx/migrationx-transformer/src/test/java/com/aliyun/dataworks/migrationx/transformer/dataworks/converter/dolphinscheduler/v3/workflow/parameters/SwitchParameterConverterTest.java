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
import com.aliyun.dataworks.common.spec.domain.noref.SpecBranch;
import com.aliyun.dataworks.common.spec.domain.noref.SpecBranches;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.SwitchResultVo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.switchs.SwitchParameters;
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

import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class SwitchParameterConverterTest {

    @Mock
    private DolphinSchedulerV3Context mockContext;
    
    private Properties properties;
    private SpecWorkflow specWorkflow;
    private DagData dagData;
    private TaskDefinition taskDefinition;
    private ObjectMapper objectMapper;
    
    @Before
    public void setUp() {
        properties = new Properties();
        
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("test-workflow");
        
        dagData = new DagData();
        
        taskDefinition = new TaskDefinition();
        taskDefinition.setCode(1001L);
        taskDefinition.setTaskType(TaskType.SWITCH.name());
        taskDefinition.setName("switch-task");
        taskDefinition.setTaskParams("{\n"
                + "          \"localParams\": [],\n"
                + "          \"rawScript\": \"\",\n"
                + "          \"resourceList\": [],\n"
                + "          \"switchResult\": {\n"
                + "            \"dependTaskList\": [\n"
                + "              {\n"
                + "                \"condition\": \"if $test==1\",\n"
                + "                \"nextNode\": 16032048684672\n"
                + "              },\n"
                + "              {\n"
                + "                \"condition\": \"if $test==2\",\n"
                + "                \"nextNode\": 16032052121728\n"
                + "              }\n"
                + "            ],\n"
                + "            \"nextNode\": 16032048684672\n"
                + "          }\n"
                + "        }");
        
        objectMapper = new ObjectMapper();
        Config.init();
    }

    @Test
    public void testConvertParameterWithValidSwitchParameters() {
        SwitchParameters switchParams = new SwitchParameters();
        switchParams.setNextNode(1004L); // default next node
        
        List<SwitchResultVo> dependTaskList = new ArrayList<>();
        
        SwitchResultVo branch1 = new SwitchResultVo();
        branch1.setCondition("${var1} > 10");
        branch1.setNextNode(1002L);
        dependTaskList.add(branch1);
        
        SwitchResultVo branch2 = new SwitchResultVo();
        branch2.setCondition("${var1} <= 10");
        branch2.setNextNode(1003L);
        dependTaskList.add(branch2);
        
        switchParams.setDependTaskList(dependTaskList);
        
        ObjectNode taskParams = objectMapper.createObjectNode();
        taskParams.set("switchResult", objectMapper.valueToTree(switchParams));
        taskDefinition.setTaskParams(taskParams.toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            Map<Long, TaskDefinition> taskCodeMap = new HashMap<>();
            
            TaskDefinition task1 = new TaskDefinition();
            task1.setCode(1002L);
            task1.setTaskType(TaskType.SWITCH.name());
            task1.setName("branch1-task");
            taskCodeMap.put(1002L, task1);
            
            TaskDefinition task2 = new TaskDefinition();
            task2.setCode(1003L);
            task2.setTaskType(TaskType.SWITCH.name());
            task2.setName("branch2-task");
            taskCodeMap.put(1003L, task2);
            
            when(mockContext.getTaskCodeMap()).thenReturn(taskCodeMap);
            
            SwitchParameterConverter converter = new SwitchParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("switch-node");
            
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
            Assert.assertEquals(CodeProgramType.CONTROLLER_BRANCH.getName(), script.getRuntime().getCommand());

            SpecBranch branch = specNode.getBranch();
            Assert.assertNotNull(branch);
            List<SpecBranches> branches = branch.getBranches();
            Assert.assertNotNull(branches);
            Assert.assertEquals(2, branches.size());
            
            SpecBranches firstBranch = branches.get(0);
            Assert.assertEquals("${var1} > 10", firstBranch.getWhen());
            Assert.assertEquals("1002", firstBranch.getOutput().getData());
            
            SpecBranches secondBranch = branches.get(1);
            Assert.assertEquals("${var1} <= 10", secondBranch.getWhen());
            Assert.assertEquals("1003", secondBranch.getOutput().getData());
        }
    }
    
    @Test
    public void testConvertParameterWithEmptyDependTaskList() {
        SwitchParameters switchParams = new SwitchParameters();
        switchParams.setNextNode(1004L);
        switchParams.setDependTaskList(new ArrayList<>());
        
        ObjectNode taskParams = objectMapper.createObjectNode();
        taskParams.set("switchResult", objectMapper.valueToTree(switchParams));
        taskDefinition.setTaskParams(taskParams.toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);

            SwitchParameterConverter converter = new SwitchParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("switch-node");
            
            converter.convertParameter(specNode);
            
            Assert.assertNotNull(specNode.getScript());
            
            SpecBranch branch = specNode.getBranch();
            Assert.assertNotNull(branch);
            Assert.assertNull(branch.getBranches());
        }
    }
    
    @Test(expected = RuntimeException.class)
    public void testConvertParameterWithInvalidTaskParams() {
        taskDefinition.setTaskParams("invalid-json");
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            SwitchParameterConverter converter = new SwitchParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("switch-node");

            converter.convertParameter(specNode);
            
            Assert.assertNotNull(specNode.getScript());
            
            SpecBranch branch = specNode.getBranch();
            Assert.assertNotNull(branch);
            Assert.assertNull(branch.getBranches());
        }
    }
    
    @Test
    public void testConvertParameterWithNullNextNode() {
        SwitchParameters switchParams = new SwitchParameters();
        List<SwitchResultVo> dependTaskList = new ArrayList<>();
        
        SwitchResultVo branch = new SwitchResultVo();
        branch.setCondition("${var1} > 0");
        branch.setNextNode(null);
        dependTaskList.add(branch);
        
        switchParams.setDependTaskList(dependTaskList);
        
        ObjectNode taskParams = objectMapper.createObjectNode();
        taskParams.set("switchResult", objectMapper.valueToTree(switchParams));
        taskDefinition.setTaskParams(taskParams.toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getTaskCodeMap()).thenReturn(new HashMap<>());
            
            SwitchParameterConverter converter = new SwitchParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("switch-node");
            converter.convertParameter(specNode);
            Assert.assertNotNull(specNode.getScript());
            
            SpecBranch branch1 = specNode.getBranch();
            Assert.assertNotNull(branch1);
            Assert.assertNotNull(branch1.getBranches());
            Assert.assertTrue(branch1.getBranches().isEmpty());
        }
    }
} 