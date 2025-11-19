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
import java.util.NoSuchElementException;
import java.util.Properties;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableScopeType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DbType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.sql.SqlParameters;
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
public class SqlParameterConverterTest {

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
            "{\"MYSQL\":\"MYSQL\",\"POSTGRESQL\":\"POSTGRESQL\",\"HIVE\":\"EMR_HIVE\"}");
        
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("test-workflow");
        
        dagData = new DagData();
        
        taskDefinition = new TaskDefinition();
        taskDefinition.setCode(1001L);
        taskDefinition.setTaskType(TaskType.SQL.name());
        taskDefinition.setName("sql-task");
        
        objectMapper = new ObjectMapper();
        
        dataSources = new ArrayList<>();
        DataSource mysqlDs = new DataSource();
        mysqlDs.setId(1);
        mysqlDs.setName("mysql_source");
        mysqlDs.setType(DbType.MYSQL);
        
        DataSource hiveDs = new DataSource();
        hiveDs.setId(2);
        hiveDs.setName("hive_source");
        hiveDs.setType(DbType.HIVE);
        
        DataSource postgresDs = new DataSource();
        postgresDs.setId(3);
        postgresDs.setName("postgres_source");
        postgresDs.setType(DbType.POSTGRESQL);
        
        dataSources.add(mysqlDs);
        dataSources.add(hiveDs);
        dataSources.add(postgresDs);
    }

    @Test
    public void testConvertMySqlParameter() {
        SqlParameters sqlParams = new SqlParameters();
        sqlParams.setType(DbType.MYSQL.name());
        sqlParams.setDatasource(1);
        sqlParams.setSql("SELECT * FROM test_table WHERE id = ${id}");
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(sqlParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getDataSources()).thenReturn(dataSources);
            
            SqlParameterConverter converter = new SqlParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("mysql-node");
            
            SpecVariable var = new SpecVariable();
            var.setId("id-var");
            var.setName("id");
            var.setValue("1");
            var.setType(VariableType.CONSTANT);
            var.setScope(VariableScopeType.NODE_PARAMETER);
            List<SpecVariable> variables = new ArrayList<>();
            variables.add(var);
            //specNode.setVariables(variables);
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.MYSQL.getName(), script.getRuntime().getCommand());
            Assert.assertEquals(CodeProgramType.MYSQL.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("SELECT * FROM test_table WHERE id = ${id}"));
            
            SpecDatasource specDatasource = specNode.getDatasource();
            Assert.assertNotNull(specDatasource);
            Assert.assertEquals("mysql_source", specDatasource.getName());
            Assert.assertEquals("mysql", specDatasource.getType());
        }
    }

    @Test
    public void testConvertHiveParameter() {
        SqlParameters sqlParams = new SqlParameters();
        sqlParams.setType(DbType.HIVE.name());
        sqlParams.setDatasource(2);
        sqlParams.setSql("SELECT * FROM hive_table WHERE dt = '${bizdate}'");
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(sqlParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getDataSources()).thenReturn(dataSources);
            
            SqlParameterConverter converter = new SqlParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("hive-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.EMR_HIVE.getName(), script.getRuntime().getCommand());
            Assert.assertEquals(CodeProgramType.EMR_HIVE.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("SELECT * FROM hive_table WHERE dt = '${bizdate}'"));
            
            SpecDatasource specDatasource = specNode.getDatasource();
            Assert.assertNotNull(specDatasource);
            Assert.assertEquals("hive_source", specDatasource.getName());
            Assert.assertEquals("emr", specDatasource.getType());
        }
    }

    @Test
    public void testConvertPostgreSqlParameter() {
        SqlParameters sqlParams = new SqlParameters();
        sqlParams.setType(DbType.POSTGRESQL.name());
        sqlParams.setDatasource(3);
        sqlParams.setSql("SELECT * FROM pg_table WHERE create_time >= '${start_time}'");
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(sqlParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getDataSources()).thenReturn(dataSources);
            
            SqlParameterConverter converter = new SqlParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("postgresql-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.POSTGRESQL.getName(), script.getRuntime().getCommand());
            Assert.assertEquals(CodeProgramType.POSTGRESQL.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
            
            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("SELECT * FROM pg_table WHERE create_time >= '${start_time}'"));
            
            SpecDatasource specDatasource = specNode.getDatasource();
            Assert.assertNotNull(specDatasource);
            Assert.assertEquals("postgres_source", specDatasource.getName());
            Assert.assertEquals("postgresql", specDatasource.getType());
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void testConvertWithUnsupportedType() {
        SqlParameters sqlParams = new SqlParameters();
        sqlParams.setType("UNSUPPORTED_DB");
        sqlParams.setDatasource(1);
        sqlParams.setSql("SELECT * FROM test_table");
        
        taskDefinition.setTaskParams(objectMapper.valueToTree(sqlParams).toString());
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);

            SqlParameterConverter converter = new SqlParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("unsupported-node");
            
            converter.convertParameter(specNode);
            
            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.SQL_COMPONENT.getName(), script.getRuntime().getCommand());
        }
    }

    @Test(expected = RuntimeException.class)
    public void testConvertWithInvalidTaskParams() {
        taskDefinition.setTaskParams("invalid-json");
        
        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);

            SqlParameterConverter converter = new SqlParameterConverter(properties, specWorkflow, dagData, taskDefinition);
            
            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("invalid-node");
            
            converter.convertParameter(specNode);
        }
    }
} 