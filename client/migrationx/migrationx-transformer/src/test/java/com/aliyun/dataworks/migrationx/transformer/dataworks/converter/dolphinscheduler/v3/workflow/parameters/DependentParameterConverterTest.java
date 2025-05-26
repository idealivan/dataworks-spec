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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessTaskRelation;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DependentRelation;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.DependentItem;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.DependentTaskModel;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.dependent.DependentParameters;
import com.aliyun.migrationx.common.utils.Config;
import com.aliyun.migrationx.common.utils.GsonUtils;

import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DependentParameterConverterTest {

    private Properties properties;
    private SpecWorkflow specWorkflow;
    private DagData dagData;
    private TaskDefinition taskDefinition;
    private DolphinSchedulerV3Context context;

    @Before
    public void setUp() {
        Config.init();
        
        properties = new Properties();
        
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("test-workflow");
        
        ProcessDefinition processDefinition = new ProcessDefinition();
        processDefinition.setCode(1000L);
        processDefinition.setName("test-process");
        
        dagData = new DagData();
        dagData.setProcessDefinition(processDefinition);
        
        taskDefinition = new TaskDefinition();
        taskDefinition.setCode(2000L);
        taskDefinition.setTaskType(TaskType.DEPENDENT.name());
        taskDefinition.setName("dependent-task");
        
        List<Project> projects = new ArrayList<>();
        List<DagData> dagDatas = new ArrayList<>();
        dagDatas.add(dagData);
        DolphinSchedulerV3Context.initContext(projects, dagDatas, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
        context = DolphinSchedulerV3Context.getContext();
        
        setupTaskRelations();
    }
    
    private void setupTaskRelations() {
        TaskDefinition task1 = new TaskDefinition();
        task1.setCode(3000L);
        task1.setTaskType(TaskType.SHELL.name());
        task1.setName("shell-task-1");
        
        TaskDefinition task2 = new TaskDefinition();
        task2.setCode(4000L);
        task2.setTaskType(TaskType.SHELL.name());
        task2.setName("shell-task-2");
        
        List<TaskDefinition> taskDefinitions = new ArrayList<>();
        taskDefinitions.add(task1);
        taskDefinitions.add(task2);
        context.getProcessCodeTaskRelationMap().put(1000L, taskDefinitions);
        
        ProcessTaskRelation relation1 = new ProcessTaskRelation();
        relation1.setPreTaskCode(3000L);
        relation1.setPostTaskCode(4000L);
        
        ProcessTaskRelation relation2 = new ProcessTaskRelation();
        relation2.setPreTaskCode(4000L);
        relation2.setPostTaskCode(2000L);
        
        dagData.setProcessTaskRelationList(Arrays.asList(relation1, relation2));
    }

    @Test
    public void testConvertParameterWithSpecificDependency() {
        DependentParameters dependentParameters = new DependentParameters();
        
        DependentTaskModel taskModel = new DependentTaskModel();
        taskModel.setRelation(DependentRelation.AND);
        
        DependentItem dependentItem = new DependentItem();
        dependentItem.setProjectCode(1L);
        dependentItem.setDefinitionCode(1000L);
        dependentItem.setDepTaskCode(3000L); // Specific task dependency
        dependentItem.setCycle("day");
        dependentItem.setDateValue("today");
        
        taskModel.setDependItemList(Collections.singletonList(dependentItem));
        dependentParameters.setDependTaskList(Collections.singletonList(taskModel));
        
        JsonObject dependenceJson = GsonUtils.toJsonObject(dependentParameters);
        JsonObject taskParamsJson = new JsonObject();
        taskParamsJson.add("dependence", dependenceJson);
        taskDefinition.setTaskParams(GsonUtils.toJsonString(taskParamsJson));
        
        DependentParameterConverter converter = new DependentParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-1");
        specNode.setName("dependent-node");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        Assert.assertEquals("VIRTUAL", script.getRuntime().getCommand());
        Assert.assertEquals(CodeProgramType.VIRTUAL.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
        
        List<Long> deps = context.getSpecNodeProcessCodeMap().get(specNode);
        Assert.assertNotNull("Dependencies should not be null", deps);
        Assert.assertEquals("Should have 1 dependency", 1, deps.size());
        Assert.assertTrue("Should contain task code 3000", deps.contains(3000L));
    }

    @Test
    public void testConvertParameterWithAllDependencies() {
        DependentParameters dependentParameters = new DependentParameters();
        
        DependentTaskModel taskModel = new DependentTaskModel();
        taskModel.setRelation(DependentRelation.OR);
        
        DependentItem dependentItem = new DependentItem();
        dependentItem.setProjectCode(1L);
        dependentItem.setDefinitionCode(1000L);
        dependentItem.setDepTaskCode(0L);
        dependentItem.setCycle("day");
        dependentItem.setDateValue("today");
        
        taskModel.setDependItemList(Collections.singletonList(dependentItem));
        dependentParameters.setDependTaskList(Collections.singletonList(taskModel));
        
        JsonObject dependenceJson = GsonUtils.toJsonObject(dependentParameters);
        JsonObject taskParamsJson = new JsonObject();
        taskParamsJson.add("dependence", dependenceJson);
        taskDefinition.setTaskParams(GsonUtils.toJsonString(taskParamsJson));
        
        DependentParameterConverter converter = new DependentParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-2");
        specNode.setName("dependent-all-node");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        
        List<Long> deps = context.getSpecNodeProcessCodeMap().get(specNode);
        Assert.assertNotNull("Dependencies should not be null", deps);
        Assert.assertEquals("Should have 2 dependencies", 2, deps.size());
        Assert.assertTrue("Should contain task code 3000", deps.contains(3000L));
        Assert.assertTrue("Should contain task code 4000", deps.contains(4000L));
    }

    @Test
    public void testConvertParameterWithMultipleDependencies() {
        DependentParameters dependentParameters = new DependentParameters();
        
        DependentTaskModel taskModel = new DependentTaskModel();
        taskModel.setRelation(DependentRelation.AND);
        
        DependentItem dependentItem1 = new DependentItem();
        dependentItem1.setProjectCode(1L);
        dependentItem1.setDefinitionCode(1000L);
        dependentItem1.setDepTaskCode(3000L);
        dependentItem1.setCycle("day");
        dependentItem1.setDateValue("today");
        
        DependentItem dependentItem2 = new DependentItem();
        dependentItem2.setProjectCode(1L);
        dependentItem2.setDefinitionCode(1000L);
        dependentItem2.setDepTaskCode(4000L);
        dependentItem2.setCycle("day");
        dependentItem2.setDateValue("today");
        
        taskModel.setDependItemList(Arrays.asList(dependentItem1, dependentItem2));
        dependentParameters.setDependTaskList(Collections.singletonList(taskModel));
        
        JsonObject dependenceJson = GsonUtils.toJsonObject(dependentParameters);
        JsonObject taskParamsJson = new JsonObject();
        taskParamsJson.add("dependence", dependenceJson);
        taskDefinition.setTaskParams(GsonUtils.toJsonString(taskParamsJson));
        
        DependentParameterConverter converter = new DependentParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-3");
        specNode.setName("dependent-multiple-node");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        
        List<Long> deps = context.getSpecNodeProcessCodeMap().get(specNode);
        Assert.assertNotNull("Dependencies should not be null", deps);
        Assert.assertEquals("Should have 2 dependencies", 2, deps.size());
        Assert.assertTrue("Should contain task code 3000", deps.contains(3000L));
        Assert.assertTrue("Should contain task code 4000", deps.contains(4000L));
    }

    @Test
    public void testConvertParameterWithEmptyDependencies() {
        JsonObject taskParamsJson = new JsonObject();
        taskDefinition.setTaskParams(GsonUtils.toJsonString(taskParamsJson));
        
        DependentParameterConverter converter = new DependentParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-4");
        specNode.setName("dependent-empty-node");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        
        List<Long> deps = context.getSpecNodeProcessCodeMap().get(specNode);
        Assert.assertNotNull("Dependencies should not be null", deps);
        Assert.assertTrue("Should have no dependencies", deps.isEmpty());
    }

    @Test
    public void testConvertParameterWithMultipleTaskModels() {
        DependentParameters dependentParameters = new DependentParameters();
        
        DependentTaskModel taskModel1 = new DependentTaskModel();
        taskModel1.setRelation(DependentRelation.AND);
        
        DependentItem dependentItem1 = new DependentItem();
        dependentItem1.setProjectCode(1L);
        dependentItem1.setDefinitionCode(1000L);
        dependentItem1.setDepTaskCode(3000L);
        dependentItem1.setCycle("day");
        dependentItem1.setDateValue("today");
        
        taskModel1.setDependItemList(Collections.singletonList(dependentItem1));
        
        DependentTaskModel taskModel2 = new DependentTaskModel();
        taskModel2.setRelation(DependentRelation.OR);
        
        DependentItem dependentItem2 = new DependentItem();
        dependentItem2.setProjectCode(1L);
        dependentItem2.setDefinitionCode(1000L);
        dependentItem2.setDepTaskCode(4000L);
        dependentItem2.setCycle("day");
        dependentItem2.setDateValue("today");
        
        taskModel2.setDependItemList(Collections.singletonList(dependentItem2));
        
        dependentParameters.setDependTaskList(Arrays.asList(taskModel1, taskModel2));
        
        JsonObject dependenceJson = GsonUtils.toJsonObject(dependentParameters);
        JsonObject taskParamsJson = new JsonObject();
        taskParamsJson.add("dependence", dependenceJson);
        taskDefinition.setTaskParams(GsonUtils.toJsonString(taskParamsJson));
        
        DependentParameterConverter converter = new DependentParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-5");
        specNode.setName("dependent-multiple-models-node");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        
        List<Long> deps = context.getSpecNodeProcessCodeMap().get(specNode);
        Assert.assertNotNull("Dependencies should not be null", deps);
        Assert.assertEquals("Should have 2 dependencies", 2, deps.size());
        Assert.assertTrue("Should contain task code 3000", deps.contains(3000L));
        Assert.assertTrue("Should contain task code 4000", deps.contains(4000L));
    }
} 