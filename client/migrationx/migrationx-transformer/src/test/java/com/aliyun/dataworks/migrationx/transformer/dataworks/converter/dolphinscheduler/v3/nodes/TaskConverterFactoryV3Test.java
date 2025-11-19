/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */


package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes;

import java.util.Collections;
import java.util.Properties;

import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.ResourceComponent;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.UdfFunc;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.DolphinSchedulerConverterContext;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.AbstractParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.ConditionsParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.CustomParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.DLCParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.DataxParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.DependentParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.FlinkParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.HiveCliParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.HttpParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.MrParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.ProcedureParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.PythonParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.ShellParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.SparkParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.SqlParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.SqoopParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.SubProcessParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.nodes.parameters.SwitchParameterConverter;
import com.aliyun.migrationx.common.context.TransformerContext;
import com.aliyun.migrationx.common.exception.UnSupportedTypeException;
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
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TaskConverterFactoryV3Test {

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
    private DolphinSchedulerV3Context mockDolphinContext;
    
    @Mock
    private MetricsCollector mockMetricsCollector;
    
    @Mock
    private TransformerContext mockTransformerContext;
    
    private Properties properties;
    
    private MockedStatic<DolphinSchedulerV3Context> dolphinContextMockedStatic;
    private MockedStatic<TransformerContext> transformerContextMockedStatic;
    private MockedStatic<Config> configMockedStatic;
    
    @Before
    public void setUp() {
        Config.init();
        
        properties = new Properties();
        
        when(mockProcessDefinition.getName()).thenReturn("test-process");
        when(mockProcessDefinition.getProjectName()).thenReturn("test-project");
        when(mockProcessDefinition.getCode()).thenReturn(1000L);
        when(mockProcessDefinition.getProjectCode()).thenReturn(2000L);
        
        when(mockDagData.getProcessDefinition()).thenReturn(mockProcessDefinition);
        
        when(mockTaskDefinition.getName()).thenReturn("test-task");
        when(mockTaskDefinition.getCode()).thenReturn(3000L);
        
        when(mockConverterContext.getProperties()).thenReturn(properties);
        
        dolphinContextMockedStatic = Mockito.mockStatic(DolphinSchedulerV3Context.class);
        dolphinContextMockedStatic.when(DolphinSchedulerV3Context::getContext).thenReturn(mockDolphinContext);

        transformerContextMockedStatic = Mockito.mockStatic(TransformerContext.class);
        transformerContextMockedStatic.when(TransformerContext::getCollector).thenReturn(mockMetricsCollector);
        
        configMockedStatic = Mockito.mockStatic(Config.class);
        configMockedStatic.when(Config::get).thenReturn(mockConfig);
        when(mockConfig.getTempTaskTypes()).thenReturn(Collections.emptyList());
    }
    
    @After
    public void tearDown() {
        if (dolphinContextMockedStatic != null) {
            dolphinContextMockedStatic.close();
        }
        if (transformerContextMockedStatic != null) {
            transformerContextMockedStatic.close();
        }
        if (configMockedStatic != null) {
            configMockedStatic.close();
        }
    }
    
    @Test
    public void testCreateShellConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SHELL.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof ShellParameterConverter);
    }
    
    @Test
    public void testCreateSqlConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SQL.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof SqlParameterConverter);
    }
    
    @Test
    public void testCreatePythonConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.PYTHON.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof PythonParameterConverter);
    }
    
    @Test
    public void testCreateConditionsConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.CONDITIONS.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof ConditionsParameterConverter);
    }
    
    @Test
    public void testCreateSubProcessConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SUB_PROCESS.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof SubProcessParameterConverter);
    }
    
    @Test
    public void testCreateDependentConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.DEPENDENT.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof DependentParameterConverter);
    }
    
    @Test
    public void testCreateSwitchConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SWITCH.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof SwitchParameterConverter);
    }
    
    @Test
    public void testCreateHttpConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.HTTP.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof HttpParameterConverter);
    }
    
    @Test
    public void testCreateSparkConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SPARK.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof SparkParameterConverter);
    }
    
    @Test
    public void testCreateMrConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.MR.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof MrParameterConverter);
    }
    
    @Test
    public void testCreateHiveCliConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.HIVECLI.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof HiveCliParameterConverter);
    }
    
    @Test
    public void testCreateDataxConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.DATAX.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof DataxParameterConverter);
    }
    
    @Test
    public void testCreateSqoopConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SQOOP.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof SqoopParameterConverter);
    }
    
    @Test
    public void testCreateFlinkConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.FLINK.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof FlinkParameterConverter);
    }
    
    @Test
    public void testCreateDLCConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.DLC.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof DLCParameterConverter);
    }
    
    @Test
    public void testCreateProcedureConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.PROCEDURE.name());
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof ProcedureParameterConverter);
    }
    
    @Test
    public void testCreateCustomConverter() throws Throwable {
        String customType = "CUSTOM_TYPE";
        when(mockTaskDefinition.getTaskType()).thenReturn(customType);
        when(mockConfig.getTempTaskTypes()).thenReturn(Collections.singletonList(customType));
        
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof CustomParameterConverter);
    }
    
    @Test(expected = UnSupportedTypeException.class)
    public void testCreateWithUnsupportedType() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn("UNSUPPORTED_TYPE");
        
        TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
    }
    
    @Test(expected = UnSupportedTypeException.class)
    public void testCreateWithNullType() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(null);
        
        TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
    }
    
    @Test
    public void testMetricsCollectorCalled() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SHELL.name());
        
        TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        
        Mockito.verify(mockMetricsCollector).incrementType(TaskType.SHELL.name());
    }
    
    @Test
    public void testMarkUnSupportedTaskCalled() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn("UNSUPPORTED_TYPE");

        try {
            TaskConverterFactoryV3.create(mockDagData, mockTaskDefinition, mockConverterContext);
        } catch (UnSupportedTypeException e) {
        }
        
        Mockito.verify(mockMetricsCollector).markUnSupportedSpecProcess(any());
    }
} 