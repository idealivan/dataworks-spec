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
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DataType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DbType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.Direct;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.Property;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.procedure.ProcedureParameters;
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

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProcedureParameterConverterTest {

    @Mock
    private DolphinSchedulerV3Context mockContext;

    private Properties properties;
    private SpecWorkflow specWorkflow;
    private DagData dagData;
    private TaskDefinition taskDefinition;
    private ObjectMapper objectMapper;
    private List<DataSource> dataSources;

    @Before
    public void setUp() {
        Config.init();
        
        properties = new Properties();
        properties.setProperty(Constants.CONVERTER_TARGET_SQL_NODE_TYPE_MAP, 
            "{\"MYSQL\":\"MySQL\",\"POSTGRESQL\":\"PostgreSQL\",\"ORACLE\":\"Oracle\"}");
        
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("test-workflow");
        
        dagData = new DagData();
        dagData.setProcessDefinition(new ProcessDefinition());
        
        taskDefinition = new TaskDefinition();
        taskDefinition.setCode(1001L);
        taskDefinition.setTaskType(TaskType.PROCEDURE.name());
        taskDefinition.setName("procedure-task");
        
        objectMapper = new ObjectMapper();
        
        dataSources = new ArrayList<>();
        DataSource mysqlDs = new DataSource();
        mysqlDs.setId(1);
        mysqlDs.setName("mysql_source");
        mysqlDs.setType(DbType.MYSQL);
        
        DataSource oracleDs = new DataSource();
        oracleDs.setId(2);
        oracleDs.setName("oracle_source");
        oracleDs.setType(DbType.ORACLE);
        
        DataSource postgresDs = new DataSource();
        postgresDs.setId(3);
        postgresDs.setName("postgres_source");
        postgresDs.setType(DbType.POSTGRESQL);
        
        dataSources.add(mysqlDs);
        dataSources.add(oracleDs);
        dataSources.add(postgresDs);
    }

    @Test
    public void testConvertMySqlProcedure() {
        ProcedureParameters procParams = new ProcedureParameters();
        procParams.setType(DbType.MYSQL.name());
        procParams.setDatasource(1);
        procParams.setMethod("CALL my_procedure(${param1}, ${param2})");

        List<Property> localParams = new ArrayList<>();
        Property param1 = new Property();
        param1.setProp("param1");
        param1.setValue("100");
        param1.setDirect(Direct.IN);
        param1.setType(DataType.VARCHAR);
        localParams.add(param1);

        Property param2 = new Property();
        param2.setProp("param2");
        param2.setValue("test");
        param2.setDirect(Direct.IN);
        param2.setType(DataType.VARCHAR);
        localParams.add(param2);

        procParams.setLocalParams(localParams);
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(procParams).toString());
        taskDefinition.setTaskParamList(localParams);
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getDataSources()).thenReturn(dataSources);
            
            ProcedureParameterConverter converter = new ProcedureParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setFileResources(new ArrayList<>());
            specNode.setId("node-1");
            specNode.setName("mysql-proc-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals("MySQL", script.getRuntime().getCommand());

            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("CALL my_procedure"));
            
            Assert.assertEquals(2, script.getParameters().size());
            Assert.assertEquals("param1", script.getParameters().get(0).getName());
            Assert.assertEquals("param2", script.getParameters().get(1).getName());
            Assert.assertEquals("100", script.getParameters().get(0).getValue());
            Assert.assertEquals("test", script.getParameters().get(1).getValue());
        }
    }

    @Test
    public void testConvertOracleProcedure() {
        ProcedureParameters procParams = new ProcedureParameters();
        procParams.setType(DbType.ORACLE.name());
        procParams.setDatasource(2);
        procParams.setMethod("BEGIN\n  pkg_name.proc_name(${param1});\nEND;");
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(procParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getDataSources()).thenReturn(dataSources);
            
            ProcedureParameterConverter converter = new ProcedureParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setFileResources(new ArrayList<>());
            specNode.setId("node-1");
            specNode.setName("oracle-proc-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.Oracle.getName(), script.getRuntime().getCommand());
            Assert.assertEquals(CodeProgramType.Oracle.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("pkg_name.proc_name(${param1})"));
            
            SpecDatasource specDatasource = specNode.getDatasource();
            Assert.assertNotNull(specDatasource);
            Assert.assertEquals("oracle_source", specDatasource.getName());
            Assert.assertEquals("oracle", specDatasource.getType());
        }
    }

    @Test
    public void testConvertPostgreSqlProcedure() {
        ProcedureParameters procParams = new ProcedureParameters();
        procParams.setType(DbType.POSTGRESQL.name());
        procParams.setDatasource(3);
        procParams.setMethod("SELECT my_function(${param1}, ${param2})");
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(procParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getDataSources()).thenReturn(dataSources);
            
            ProcedureParameterConverter converter = new ProcedureParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setFileResources(new ArrayList<>());
            specNode.setId("node-1");
            specNode.setName("postgresql-proc-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.POSTGRESQL.getName(), script.getRuntime().getCommand());
            Assert.assertEquals(CodeProgramType.POSTGRESQL.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("SELECT my_function(${param1}, ${param2})"));
            
            SpecDatasource specDatasource = specNode.getDatasource();
            Assert.assertNotNull(specDatasource);
            Assert.assertEquals("postgres_source", specDatasource.getName());
            Assert.assertEquals("postgresql", specDatasource.getType());
        }
    }

    @Test(expected = RuntimeException.class)
    public void testConvertWithInvalidTaskParams() {
        taskDefinition.setTaskParams("invalid-json");
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            
            ProcedureParameterConverter converter = new ProcedureParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("invalid-node");
            
            converter.convertParameter(specNode);
        }
    }
} 