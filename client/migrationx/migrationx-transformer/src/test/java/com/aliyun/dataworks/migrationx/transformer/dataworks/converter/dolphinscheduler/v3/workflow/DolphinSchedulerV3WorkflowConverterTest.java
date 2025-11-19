/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */


package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.interfaces.Output;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.ResourceComponent;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.UdfFunc;

import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class DolphinSchedulerV3WorkflowConverterTest {

    @Mock
    private DolphinSchedulerPackage<Project, DagData, DataSource, ResourceComponent, UdfFunc> mockPackage;
    
    @Mock
    private V3ProcessDefinitionConverter mockProcessConverter;
    
    @Mock
    private DolphinSchedulerV3Context mockContext;
    
    private Properties converterProperties;
    private List<DagData> dagDataList;
    private Map<String, List<DagData>> processDefinitionsMap;
    
    @Before
    public void setUp() {
        converterProperties = new Properties();
        dagDataList = new ArrayList<>();
        processDefinitionsMap = new HashMap<>();
    }

    private DagData createMockDagData(String name, long code) {
        ProcessDefinition processDefinition = new ProcessDefinition();
        processDefinition.setName(name);
        processDefinition.setCode(code);
        
        DagData dagData = new DagData();
        dagData.setProcessDefinition(processDefinition);
        dagData.setTaskDefinitionList(new ArrayList<>());
        
        return dagData;
    }

    private TaskDefinition createMockTaskDefinition(long code, String name, String taskType, String taskParams) {
        TaskDefinition taskDefinition = new TaskDefinition();
        taskDefinition.setCode(code);
        taskDefinition.setName(name);
        taskDefinition.setTaskType(taskType);
        taskDefinition.setTaskParams(taskParams);
        return taskDefinition;
    }
    
    @Test
    public void testConvertEmptyProcessList() {
        when(mockPackage.getProcessDefinitions()).thenReturn(Collections.emptyMap());
        try {
            DolphinSchedulerV3WorkflowConverter converter = new DolphinSchedulerV3WorkflowConverter(mockPackage, converterProperties);
            Assert.fail("Expected RuntimeException but none was thrown");
        } catch (RuntimeException e) {
            Assert.assertEquals("process list empty", e.getMessage());
        }
    }
    
    @Test
    public void testConvertBasicWorkflow() {
        DagData dagData = createMockDagData("test-workflow", 1001L);
        dagDataList.add(dagData);
        processDefinitionsMap.put("test-project", dagDataList);
        
        when(mockPackage.getProcessDefinitions()).thenReturn(processDefinitionsMap);

        SpecWorkflow mockWorkflow = new SpecWorkflow();
        mockWorkflow.setId("workflow-1");
        mockWorkflow.setName("test-workflow");
        mockWorkflow.setNodes(new ArrayList<>());
        mockWorkflow.setDependencies(new ArrayList<>());
        mockWorkflow.setOutputs(new ArrayList<>());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class);
             MockedConstruction<V3ProcessDefinitionConverter> mockedConstruction = 
                 Mockito.mockConstruction(V3ProcessDefinitionConverter.class,
                     (mock, context) -> {
                         when(mock.convert()).thenReturn(mockWorkflow);
                     })) {

            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            when(mockContext.getSubProcessCodeWorkflowMap()).thenReturn(new HashMap<>());
            when(mockContext.getSubProcessCodeNodeMap()).thenReturn(new HashMap<>());
            when(mockContext.getTaskCodeSpecNodeMap()).thenReturn(new HashMap<>());
            when(mockContext.getSpecNodeProcessCodeMap()).thenReturn(new HashMap<>());

            DolphinSchedulerV3WorkflowConverter converter = new DolphinSchedulerV3WorkflowConverter(mockPackage, converterProperties);
            List<Specification<DataWorksWorkflowSpec>> result = converter.convert();

            Assert.assertNotNull(result);
            Assert.assertEquals(1, result.size());
            
            Specification<DataWorksWorkflowSpec> spec = result.get(0);
            Assert.assertEquals(SpecKind.CYCLE_WORKFLOW.getLabel(), spec.getKind());
            Assert.assertEquals(DolphinSchedulerV3WorkflowConverter.SPEC_VERSION, spec.getVersion());
            
            DataWorksWorkflowSpec workflowSpec = spec.getSpec();
            Assert.assertEquals("test-workflow", workflowSpec.getName());
            Assert.assertEquals(1, workflowSpec.getWorkflows().size());
            Assert.assertEquals("workflow-1", workflowSpec.getWorkflows().get(0).getId());
            
            Assert.assertEquals(1, mockedConstruction.constructed().size());
            V3ProcessDefinitionConverter constructedConverter = mockedConstruction.constructed().get(0);
            verify(constructedConverter).convert();
        }
    }
    
    @Test
    public void testConvertWithSubProcess() {
        DagData mainProcess = createMockDagData("main-workflow", 1001L);
        DagData subProcess = createMockDagData("sub-workflow", 1002L);
        
        JsonObject subProcessParams = new JsonObject();
        subProcessParams.addProperty("processDefinitionCode", 1002L);
        TaskDefinition subProcessTask = createMockTaskDefinition(
            2001L, "sub-task", TaskType.SUB_PROCESS.name(), subProcessParams.toString());
        
        mainProcess.getTaskDefinitionList().add(subProcessTask);
        
        dagDataList.add(mainProcess);
        dagDataList.add(subProcess);
        processDefinitionsMap.put("test-project", dagDataList);
        
        when(mockPackage.getProcessDefinitions()).thenReturn(processDefinitionsMap);
        
        SpecWorkflow mainWorkflow = new SpecWorkflow();
        mainWorkflow.setId("workflow-main");
        mainWorkflow.setName("main-workflow");
        mainWorkflow.setNodes(new ArrayList<>());
        mainWorkflow.setDependencies(new ArrayList<>());
        mainWorkflow.setOutputs(new ArrayList<>());
        
        SpecNode subProcessNode = new SpecNode();
        subProcessNode.setId("node-subprocess");
        subProcessNode.setName("sub-task");
        mainWorkflow.getNodes().add(subProcessNode);
        
        SpecWorkflow subWorkflow = new SpecWorkflow();
        subWorkflow.setId("workflow-sub");
        subWorkflow.setName("sub-workflow");
        subWorkflow.setNodes(new ArrayList<>());
        subWorkflow.setDependencies(new ArrayList<>());
        subWorkflow.setOutputs(new ArrayList<>());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class);
             MockedConstruction<V3ProcessDefinitionConverter> mockedConstruction = 
                 Mockito.mockConstruction(V3ProcessDefinitionConverter.class,
                     (mock, context) -> {
                         List arguments = context.arguments();
                         if (arguments.get(0) == mainProcess) {
                             when(mock.convert()).thenReturn(mainWorkflow);
                         } else if (arguments.get(0) == subProcess) {
                             when(mock.convert()).thenReturn(subWorkflow);
                         }
                     })) {
            
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            Map<Long, Object> codeWorkflowMap = new HashMap<>();
            codeWorkflowMap.put(1002L, subWorkflow);
            
            Map<Long, Object> codeNodeMap = new HashMap<>();
            codeNodeMap.put(1002L, subProcessNode);
            
            when(mockContext.getSubProcessCodeWorkflowMap()).thenReturn(codeWorkflowMap);
            when(mockContext.getSubProcessCodeNodeMap()).thenReturn(codeNodeMap);
            when(mockContext.getTaskCodeSpecNodeMap()).thenReturn(new HashMap<>());
            when(mockContext.getSpecNodeProcessCodeMap()).thenReturn(new HashMap<>());
            
            DolphinSchedulerV3WorkflowConverter converter = new DolphinSchedulerV3WorkflowConverter(mockPackage, converterProperties);
            List<Specification<DataWorksWorkflowSpec>> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(2, result.size());
            
            verify(mockContext).getSubProcessCodeWorkflowMap();
            verify(mockContext).getSubProcessCodeNodeMap();
            
            Assert.assertEquals(2, mockedConstruction.constructed().size());
        }
    }
    
    @Test
    public void testConvertWithDependencies() {
        DagData dagData = createMockDagData("workflow-with-deps", 1001L);
        dagDataList.add(dagData);
        processDefinitionsMap.put("test-project", dagDataList);
        
        when(mockPackage.getProcessDefinitions()).thenReturn(processDefinitionsMap);
        
        SpecWorkflow workflow = new SpecWorkflow();
        workflow.setId("workflow-1");
        workflow.setName("workflow-with-deps");
        
        SpecNode node1 = new SpecNode();
        node1.setId("node-1");
        node1.setName("task-1");
        
        SpecNode node2 = new SpecNode();
        node2.setId("node-2");
        node2.setName("task-2");
        
        List<SpecNode> nodes = new ArrayList<>();
        nodes.add(node1);
        nodes.add(node2);
        workflow.setNodes(nodes);
        workflow.setDependencies(new ArrayList<>());
        workflow.setOutputs(new ArrayList<>());
        
        SpecNodeOutput output1 = new SpecNodeOutput();
        output1.setData("output-data-1");
        List<Output> outputs1 = new ArrayList<>();
        outputs1.add(output1);
        node1.setOutputs(outputs1);
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class);
             MockedConstruction<V3ProcessDefinitionConverter> mockedConstruction = 
                 Mockito.mockConstruction(V3ProcessDefinitionConverter.class,
                     (mock, context) -> {
                         when(mock.convert()).thenReturn(workflow);
                     })) {
            
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            Map<Long, Object> taskCodeSpecNodeMap = new HashMap<>();
            taskCodeSpecNodeMap.put(2001L, node1);
            
            Map<Object, List<Long>> specNodeProcessCodeMap = new HashMap<>();
            List<Long> dependentCodes = new ArrayList<>();
            dependentCodes.add(2001L);
            specNodeProcessCodeMap.put(node2, dependentCodes);
            
            when(mockContext.getSubProcessCodeWorkflowMap()).thenReturn(new HashMap<>());
            when(mockContext.getSubProcessCodeNodeMap()).thenReturn(new HashMap<>());
            when(mockContext.getTaskCodeSpecNodeMap()).thenReturn(taskCodeSpecNodeMap);
            when(mockContext.getSpecNodeProcessCodeMap()).thenReturn(specNodeProcessCodeMap);
            
            DolphinSchedulerV3WorkflowConverter converter = new DolphinSchedulerV3WorkflowConverter(mockPackage, converterProperties);
            List<Specification<DataWorksWorkflowSpec>> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(1, result.size());
            
            verify(mockContext).getTaskCodeSpecNodeMap();
            verify(mockContext).getSpecNodeProcessCodeMap();
            
            Assert.assertEquals(1, mockedConstruction.constructed().size());
        }
    }
    
    @Test
    public void testConvertWithNullWorkflow() {
        DagData dagData = createMockDagData("null-workflow", 1001L);
        dagDataList.add(dagData);
        processDefinitionsMap.put("test-project", dagDataList);
        
        when(mockPackage.getProcessDefinitions()).thenReturn(processDefinitionsMap);
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class);
             MockedConstruction<V3ProcessDefinitionConverter> mockedConstruction = 
                 Mockito.mockConstruction(V3ProcessDefinitionConverter.class,
                     (mock, context) -> {
                         when(mock.convert()).thenReturn(null);
                     })) {
            
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            when(mockContext.getSubProcessCodeWorkflowMap()).thenReturn(new HashMap<>());
            when(mockContext.getSubProcessCodeNodeMap()).thenReturn(new HashMap<>());
            when(mockContext.getTaskCodeSpecNodeMap()).thenReturn(new HashMap<>());
            when(mockContext.getSpecNodeProcessCodeMap()).thenReturn(new HashMap<>());
            
            DolphinSchedulerV3WorkflowConverter converter = new DolphinSchedulerV3WorkflowConverter(mockPackage, converterProperties);
            List<Specification<DataWorksWorkflowSpec>> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertTrue(result.isEmpty());
            
            Assert.assertEquals(1, mockedConstruction.constructed().size());
            V3ProcessDefinitionConverter constructedConverter = mockedConstruction.constructed().get(0);
            verify(constructedConverter).convert();
        }
    }
}