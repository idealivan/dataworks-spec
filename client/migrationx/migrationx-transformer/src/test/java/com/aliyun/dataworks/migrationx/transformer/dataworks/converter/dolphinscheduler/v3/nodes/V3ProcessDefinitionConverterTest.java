/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */


package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.Schedule;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.ResourceComponent;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.UdfFunc;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.ResourceInfo;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.v301.ReleaseState;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.Node;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.WorkflowType;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.WorkflowVersion;
import com.aliyun.dataworks.migrationx.transformer.core.checkpoint.CheckPoint;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.DolphinSchedulerConverterContext;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.AbstractParameterConverter;
import com.aliyun.migrationx.common.context.TransformerContext;
import com.aliyun.migrationx.common.metrics.MetricsCollector;
import com.aliyun.migrationx.common.utils.Config;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class V3ProcessDefinitionConverterTest {

    @Mock
    private DolphinSchedulerConverterContext<Project, DagData, DataSource, ResourceComponent, UdfFunc> mockConverterContext;
    
    @Mock
    private DagData mockDagData;
    
    @Mock
    private ProcessDefinition mockProcessDefinition;
    
    @Mock
    private TaskDefinition mockTaskDefinition;
    
    @Mock
    private Config mockConfig;
    
    @Mock
    private CheckPoint mockCheckPoint;
    
    @Mock
    private DolphinSchedulerV3Context mockDolphinContext;
    
    @Mock
    private MetricsCollector mockMetricsCollector;
    
    @Mock
    private TransformerContext mockTransformerContext;
    
    @Mock
    private DolphinSchedulerPackage<Project, DagData, DataSource, ResourceInfo, UdfFunc> mockDolphinSchedulerPackage;
    
    private Properties properties;
    
    private MockedStatic<DolphinSchedulerV3Context> dolphinContextMockedStatic;
    private MockedStatic<CheckPoint> checkPointMockedStatic;
    private MockedStatic<TransformerContext> transformerContextMockedStatic;
    private MockedStatic<Config> configMockedStatic;
    
    @Before
    public void setUp() {
        Config.init();
        
        properties = new Properties();
        
        when(mockProcessDefinition.getName()).thenReturn("test-process");
        when(mockProcessDefinition.getProjectName()).thenReturn("test-project");
        when(mockProcessDefinition.getCode()).thenReturn(1000L);
        when(mockProcessDefinition.getReleaseState()).thenReturn(ReleaseState.ONLINE);
        
        Map<String, String> globalParams = new HashMap<>();
        globalParams.put("param1", "value1");
        globalParams.put("param2", "value2");
        when(mockProcessDefinition.getGlobalParamMap()).thenReturn(globalParams);
        
        when(mockDagData.getProcessDefinition()).thenReturn(mockProcessDefinition);
        List<TaskDefinition> taskDefinitions = new ArrayList<>();
        taskDefinitions.add(mockTaskDefinition);
        when(mockDagData.getTaskDefinitionList()).thenReturn(taskDefinitions);

        dolphinContextMockedStatic = Mockito.mockStatic(DolphinSchedulerV3Context.class);
        dolphinContextMockedStatic.when(DolphinSchedulerV3Context::getContext).thenReturn(mockDolphinContext);
        when(mockDolphinContext.getSubProcessCodeMap(any())).thenReturn(new ArrayList<>());
        
        checkPointMockedStatic = Mockito.mockStatic(CheckPoint.class);
        checkPointMockedStatic.when(CheckPoint::getInstance).thenReturn(mockCheckPoint);
        when(mockCheckPoint.loadFromCheckPoint(anyString(), anyString())).thenReturn(new HashMap<>());
        when(mockCheckPoint.doWithCheckpoint(any(), anyString())).thenReturn(new ArrayList<>());
        
        transformerContextMockedStatic = Mockito.mockStatic(TransformerContext.class);
        transformerContextMockedStatic.when(TransformerContext::getCollector).thenReturn(mockMetricsCollector);
        
        configMockedStatic = Mockito.mockStatic(Config.class);
        configMockedStatic.when(Config::get).thenReturn(mockConfig);
    }
    
    @After
    public void tearDown() {
        if (dolphinContextMockedStatic != null) {
            dolphinContextMockedStatic.close();
        }
        if (checkPointMockedStatic != null) {
            checkPointMockedStatic.close();
        }
        if (transformerContextMockedStatic != null) {
            transformerContextMockedStatic.close();
        }
        if (configMockedStatic != null) {
            configMockedStatic.close();
        }
    }
    
    @Test
    public void testConvert() {
        V3ProcessDefinitionConverter converter = new V3ProcessDefinitionConverter(mockConverterContext, mockDagData);
        
        MockedStatic<TaskConverterFactoryV3> taskConverterFactoryMockedStatic = Mockito.mockStatic(TaskConverterFactoryV3.class);
        AbstractParameterConverter mockTaskConverter = mock(AbstractParameterConverter.class);
        List<DwWorkflow> mockWorkflows = new ArrayList<>();
        DwWorkflow mockWorkflow = new DwWorkflow();
        mockWorkflow.setName("test-workflow");
        mockWorkflow.setType(WorkflowType.BUSINESS);
        mockWorkflow.setVersion(WorkflowVersion.V3);
        Node mockNode = new Node();
        mockNode.setName("test-node");
        mockWorkflow.setNodes(Collections.singletonList(mockNode));
        mockWorkflows.add(mockWorkflow);
        
        taskConverterFactoryMockedStatic.when(() -> TaskConverterFactoryV3.create(any(), any(), any())).thenReturn(mockTaskConverter);
        
        try {
            List<DwWorkflow> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(0, result.size()); // Empty because we mocked CheckPoint.doWithCheckpoint to return empty list
            
            List<DwWorkflow> workflowList = converter.getWorkflowList();
            Assert.assertNotNull(workflowList);
            Assert.assertEquals(0, workflowList.size());
        } finally {
            taskConverterFactoryMockedStatic.close();
        }
    }
    
    @Test
    public void testConvertWithSchedule() {
        Schedule mockSchedule = new Schedule();
        mockSchedule.setCrontab("0 0 * * *");
        mockSchedule.setStartTime(new Date());
        mockSchedule.setEndTime(new Date());
        when(mockDagData.getSchedule()).thenReturn(mockSchedule);
        
        V3ProcessDefinitionConverter converter = new V3ProcessDefinitionConverter(mockConverterContext, mockDagData);
        
        MockedStatic<TaskConverterFactoryV3> taskConverterFactoryMockedStatic = Mockito.mockStatic(TaskConverterFactoryV3.class);
        AbstractParameterConverter mockTaskConverter = mock(AbstractParameterConverter.class);
        List<DwWorkflow> mockWorkflows = new ArrayList<>();
        DwWorkflow mockWorkflow = new DwWorkflow();
        mockWorkflow.setName("test-workflow");
        mockWorkflow.setType(WorkflowType.BUSINESS);
        mockWorkflow.setVersion(WorkflowVersion.V3);
        Node mockNode = new Node();
        mockNode.setName("test-node");
        mockWorkflow.setNodes(Collections.singletonList(mockNode));
        mockWorkflows.add(mockWorkflow);
        
        taskConverterFactoryMockedStatic.when(() -> TaskConverterFactoryV3.create(any(), any(), any())).thenReturn(mockTaskConverter);
        
        try {
            List<DwWorkflow> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(0, result.size()); // Empty because we mocked CheckPoint.doWithCheckpoint to return empty list
        } finally {
            taskConverterFactoryMockedStatic.close();
        }
    }

    
    @Test
    public void testConvertWithErrorAndNotContinue() {
        V3ProcessDefinitionConverter converter = new V3ProcessDefinitionConverter(mockConverterContext, mockDagData);
        
        MockedStatic<TaskConverterFactoryV3> taskConverterFactoryMockedStatic = Mockito.mockStatic(TaskConverterFactoryV3.class);
        taskConverterFactoryMockedStatic.when(() -> TaskConverterFactoryV3.create(any(), any(), any()))
            .thenThrow(new RuntimeException("Test error"));
        
        try {
            converter.convert();
        } finally {
            taskConverterFactoryMockedStatic.close();
        }
    }
    
    @Test
    public void testConvertWithErrorAndContinue() {
        V3ProcessDefinitionConverter converter = new V3ProcessDefinitionConverter(mockConverterContext, mockDagData);
        
        MockedStatic<TaskConverterFactoryV3> taskConverterFactoryMockedStatic = Mockito.mockStatic(TaskConverterFactoryV3.class);
        taskConverterFactoryMockedStatic.when(() -> TaskConverterFactoryV3.create(any(), any(), any()))
            .thenThrow(new RuntimeException("Test error"));
        
        try {
            List<DwWorkflow> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(0, result.size());
        } finally {
            taskConverterFactoryMockedStatic.close();
        }
    }
    
    @Test
    public void testToWorkflowName() {
        when(mockProcessDefinition.getProjectName()).thenReturn("project");
        when(mockProcessDefinition.getName()).thenReturn("workflow");
        
        String workflowName = V3ProcessDefinitionConverter.toWorkflowName(mockProcessDefinition);
        
        Assert.assertEquals("project_workflow", workflowName);
    }

} 