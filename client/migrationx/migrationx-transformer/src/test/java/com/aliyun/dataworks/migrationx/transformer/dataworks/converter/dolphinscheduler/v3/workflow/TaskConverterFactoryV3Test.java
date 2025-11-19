/*
/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */


package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.AbstractParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.CustomParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.DataxParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.DependentParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.FlinkParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.HiveCliParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.MrParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.ProcedureParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.PythonParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.ShellParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.SparkParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.SqlParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.SqoopParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.SubProcessParameterConverter;
import com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow.parameters.SwitchParameterConverter;
import com.aliyun.migrationx.common.utils.Config;

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
public class TaskConverterFactoryV3Test {

    @Mock
    private SpecWorkflow mockSpecWorkflow;
    
    @Mock
    private DagData mockDagData;
    
    @Mock
    private TaskDefinition mockTaskDefinition;
    
    @Mock
    private Config mockConfig;
    
    private Properties properties;
    
    @Before
    public void setUp() {
        properties = new Properties();
        properties.setProperty("test.key", "test.value");
    }
    
    @Test
    public void testCreateShellConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SHELL.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        // Verify
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof ShellParameterConverter);
    }
    
    @Test
    public void testCreatePythonConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.PYTHON.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof PythonParameterConverter);
    }
    
    @Test
    public void testCreateSqlConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SQL.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof SqlParameterConverter);
    }
    
    @Test
    public void testCreateMrConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.MR.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof MrParameterConverter);
    }
    
    @Test
    public void testCreateSparkConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SPARK.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof SparkParameterConverter);
    }
    
    @Test
    public void testCreateSubProcessConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SUB_PROCESS.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof SubProcessParameterConverter);
    }
    
    @Test
    public void testCreateHiveCliConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.HIVECLI.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof HiveCliParameterConverter);
    }
    
    @Test
    public void testCreateDependentConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.DEPENDENT.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof DependentParameterConverter);
    }
    
    @Test
    public void testCreateSwitchConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SWITCH.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof SwitchParameterConverter);
    }
    
    @Test
    public void testCreateSqoopConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.SQOOP.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof SqoopParameterConverter);
    }
    
    @Test
    public void testCreateDataxConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.DATAX.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof DataxParameterConverter);
    }
    
    @Test
    public void testCreateProcedureConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.PROCEDURE.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof ProcedureParameterConverter);
    }
    
    @Test
    public void testCreateFlinkConverter() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(TaskType.FLINK.name());
        Config.init(new Config());
        AbstractParameterConverter converter = TaskConverterFactoryV3.create(
            properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        
        Assert.assertNotNull(converter);
        Assert.assertTrue(converter instanceof FlinkParameterConverter);
    }
    
    @Test(expected = RuntimeException.class)
    public void testCreateWithUnsupportedType() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn("UNSUPPORTED_TYPE");
        
        TaskConverterFactoryV3.create(properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
    }
    
    @Test
    public void testCreateCustomConverter() throws Throwable {
        String customTaskType = "SHELL";
        when(mockTaskDefinition.getTaskType()).thenReturn(customTaskType);
        
        List<String> tempTaskTypes = new ArrayList<>(Arrays.asList(customTaskType));
        
        try (MockedStatic<Config> configMock = Mockito.mockStatic(Config.class)) {
            configMock.when(Config::get).thenReturn(mockConfig);
            when(mockConfig.getTempTaskTypes()).thenReturn(tempTaskTypes);
            
            AbstractParameterConverter converter = TaskConverterFactoryV3.create(
                properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
            
            Assert.assertNotNull(converter);
            Assert.assertTrue(converter instanceof CustomParameterConverter);
        }
    }
    
    @Test(expected = RuntimeException.class)
    public void testCreateWithNullTaskType() throws Throwable {
        when(mockTaskDefinition.getTaskType()).thenReturn(null);
        
        try (MockedStatic<Config> configMock = Mockito.mockStatic(Config.class)) {
            configMock.when(Config::get).thenReturn(mockConfig);
            when(mockConfig.getTempTaskTypes()).thenReturn(new ArrayList<>());
            
            TaskConverterFactoryV3.create(properties, mockSpecWorkflow, mockDagData, mockTaskDefinition);
        }
    }
} 