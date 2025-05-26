/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */


package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.enums.DependencyType;
import com.aliyun.dataworks.common.spec.domain.interfaces.Output;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessTaskRelation;

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
public class SpecFlowDependConverterTest {

    @Mock
    private DataWorksWorkflowSpec mockSpec;
    
    @Mock
    private DolphinSchedulerV3Context mockContext;
    
    private SpecWorkflow specWorkflow;
    private List<ProcessTaskRelation> processTaskRelations;
    private Map<Long, String> taskCodeNodeDataMap;
    private Map<Long, String> taskCodeNodeIdMap;
    
    @Before
    public void setUp() {
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("Test Workflow");
        specWorkflow.setDependencies(new ArrayList<>());
        specWorkflow.setNodes(new ArrayList<>());
        
        SpecNode node1 = createSpecNode("node-1", "Node 1", "output-data-1");
        SpecNode node2 = createSpecNode("node-2", "Node 2", "output-data-2");
        SpecNode node3 = createSpecNode("node-3", "Node 3", "output-data-3");
        
        specWorkflow.getNodes().add(node1);
        specWorkflow.getNodes().add(node2);
        specWorkflow.getNodes().add(node3);
        
        processTaskRelations = new ArrayList<>();
        
        ProcessTaskRelation relation1 = new ProcessTaskRelation();
        relation1.setPreTaskCode(101L);  // node1
        relation1.setPostTaskCode(102L); // node2
        
        ProcessTaskRelation relation2 = new ProcessTaskRelation();
        relation2.setPreTaskCode(102L);  // node2
        relation2.setPostTaskCode(103L); // node3
        
        processTaskRelations.add(relation1);
        processTaskRelations.add(relation2);
        
        taskCodeNodeDataMap = new HashMap<>();
        taskCodeNodeDataMap.put(101L, "output-data-1");
        taskCodeNodeDataMap.put(102L, "output-data-2");
        taskCodeNodeDataMap.put(103L, "output-data-3");
        
        taskCodeNodeIdMap = new HashMap<>();
        taskCodeNodeIdMap.put(101L, "node-1");
        taskCodeNodeIdMap.put(102L, "node-2");
        taskCodeNodeIdMap.put(103L, "node-3");
        
        when(mockSpec.getFlow()).thenReturn(new ArrayList<>());
    }
    
    /**
     * Helper method to create a SpecNode with an output
     */
    private SpecNode createSpecNode(String id, String name, String outputData) {
        SpecNode node = new SpecNode();
        node.setId(id);
        node.setName(name);
        
        SpecNodeOutput output = new SpecNodeOutput();
        output.setData(outputData);
        output.setIsDefault(true);
        
        List<Output> outputs = new ArrayList<>();
        outputs.add(output);
        node.setOutputs(outputs);
        
        return node;
    }
    
    @Test
    public void testConvertWithSpecWorkflow() {
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getTaskCodeNodeDataMap()).thenReturn(taskCodeNodeDataMap);
            when(mockContext.getTaskCodeNodeIdMap()).thenReturn(taskCodeNodeIdMap);
            
            SpecFlowDependConverter converter = new SpecFlowDependConverter(null, specWorkflow, processTaskRelations);
            List<SpecFlowDepend> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(2, result.size());
            
            verifyDependency(result, "node-2", "node-1", "output-data-1");
            verifyDependency(result, "node-3", "node-2", "output-data-2");
        }
    }
    
    @Test
    public void testConvertWithDataWorksWorkflowSpec() {
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getTaskCodeNodeDataMap()).thenReturn(taskCodeNodeDataMap);
            when(mockContext.getTaskCodeNodeIdMap()).thenReturn(taskCodeNodeIdMap);
            
            SpecWorkflow specWorkflow1 = new SpecWorkflow();
            specWorkflow1.setNodes(new ArrayList<>());
            SpecFlowDependConverter converter = new SpecFlowDependConverter(mockSpec, specWorkflow1, processTaskRelations);
            List<SpecFlowDepend> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(0, result.size()); // No dependencies should be created without a workflow
        }
    }
    
    @Test
    public void testConvertWithEmptyRelations() {
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getTaskCodeNodeDataMap()).thenReturn(taskCodeNodeDataMap);
            when(mockContext.getTaskCodeNodeIdMap()).thenReturn(taskCodeNodeIdMap);
            
            SpecFlowDependConverter converter = new SpecFlowDependConverter(null, specWorkflow, new ArrayList<>());
            List<SpecFlowDepend> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(0, result.size()); // No dependencies should be created
        }
    }
    
    @Test
    public void testConvertWithMissingNodeId() {
        ProcessTaskRelation invalidRelation = new ProcessTaskRelation();
        invalidRelation.setPreTaskCode(101L);
        invalidRelation.setPostTaskCode(999L); // This code doesn't exist in the map
        
        List<ProcessTaskRelation> relations = new ArrayList<>();
        relations.add(invalidRelation);
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getTaskCodeNodeDataMap()).thenReturn(taskCodeNodeDataMap);
            when(mockContext.getTaskCodeNodeIdMap()).thenReturn(taskCodeNodeIdMap);
            
            SpecFlowDependConverter converter = new SpecFlowDependConverter(null, specWorkflow, relations);
            List<SpecFlowDepend> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(0, result.size()); // No dependencies should be created for invalid relations
        }
    }
    
    @Test
    public void testConvertWithMissingNodeData() {
        ProcessTaskRelation relation = new ProcessTaskRelation();
        relation.setPreTaskCode(999L); // This code doesn't exist in the data map
        relation.setPostTaskCode(102L);
        
        List<ProcessTaskRelation> relations = new ArrayList<>();
        relations.add(relation);
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getTaskCodeNodeDataMap()).thenReturn(taskCodeNodeDataMap);
            when(mockContext.getTaskCodeNodeIdMap()).thenReturn(taskCodeNodeIdMap);
            
            SpecFlowDependConverter converter = new SpecFlowDependConverter(null, specWorkflow, relations);
            List<SpecFlowDepend> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(0, result.size()); // No dependencies should be created for invalid relations
        }
    }
    
    @Test
    public void testConvertWithZeroPreTaskCode() {
        ProcessTaskRelation relation = new ProcessTaskRelation();
        relation.setPreTaskCode(0L); // Zero pre-task code should be skipped
        relation.setPostTaskCode(102L);
        
        List<ProcessTaskRelation> relations = new ArrayList<>();
        relations.add(relation);
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getTaskCodeNodeDataMap()).thenReturn(taskCodeNodeDataMap);
            when(mockContext.getTaskCodeNodeIdMap()).thenReturn(taskCodeNodeIdMap);
            
            SpecFlowDependConverter converter = new SpecFlowDependConverter(null, specWorkflow, relations);
            List<SpecFlowDepend> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(0, result.size()); // No dependencies should be created for zero pre-task code
        }
    }

    private void verifyDependency(List<SpecFlowDepend> dependencies, String nodeId, String dependsOnNodeId, String outputData) {
        boolean found = false;
        
        for (SpecFlowDepend flowDepend : dependencies) {
            if (flowDepend.getNodeId().getId().equals(nodeId)) {
                for (SpecDepend depend : flowDepend.getDepends()) {
                    SpecNodeOutput output = (SpecNodeOutput)depend.getOutput();
                    if (output.getData().equals(outputData)) {
                        Assert.assertEquals(DependencyType.NORMAL, depend.getType());
                        found = true;
                        break;
                    }
                }
            }
        }
        
        Assert.assertTrue("Dependency from " + nodeId + " to " + dependsOnNodeId + " not found", found);
    }
} 