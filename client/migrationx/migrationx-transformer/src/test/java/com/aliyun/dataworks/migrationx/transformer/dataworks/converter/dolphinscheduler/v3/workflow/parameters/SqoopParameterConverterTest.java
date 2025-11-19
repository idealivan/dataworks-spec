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
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.TaskType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.DolphinSchedulerV3Context;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DbType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.sqoop.SqoopParameters;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.sqoop.sources.SourceHdfsParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.sqoop.sources.SourceHiveParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.sqoop.sources.SourceMysqlParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.sqoop.targets.TargetHdfsParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.sqoop.targets.TargetHiveParameter;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.sqoop.targets.TargetMysqlParameter;
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
public class SqoopParameterConverterTest {

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
        
        specWorkflow = new SpecWorkflow();
        specWorkflow.setId("workflow-1");
        specWorkflow.setName("test-workflow");
        
        dagData = new DagData();
        
        taskDefinition = new TaskDefinition();
        taskDefinition.setCode(1001L);
        taskDefinition.setTaskType(TaskType.SQOOP.name());
        taskDefinition.setName("sqoop-task");
        
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
        
        dataSources.add(mysqlDs);
        dataSources.add(hiveDs);
    }

    @Test
    public void testConvertMysqlToHive() {
        SourceMysqlParameter sourceMysql = new SourceMysqlParameter();
        sourceMysql.setSrcDatasource(1);
        sourceMysql.setSrcTable("source_table");
        sourceMysql.setSrcColumns("id,name,age");
        sourceMysql.setSrcQuerySql("SELECT * FROM source_table WHERE age > 18");

        TargetHiveParameter targetHive = new TargetHiveParameter();
        targetHive.setHiveDatabase("2");
        targetHive.setHiveTable("target_table");
        targetHive.setHivePartitionKey("dt,region");
        targetHive.setHivePartitionValue("20240515,cn-hangzhou");

        SqoopParameters sqoopParams = new SqoopParameters();
        sqoopParams.setSourceType("MYSQL");
        sqoopParams.setTargetType("HIVE");
        sqoopParams.setSourceParams(objectMapper.valueToTree(sourceMysql).toString());
        sqoopParams.setTargetParams(objectMapper.valueToTree(targetHive).toString());

        taskDefinition.setTaskParams(objectMapper.valueToTree(sqoopParams).toString());

        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getDataSources()).thenReturn(dataSources);

            SqoopParameterConverter converter = new SqoopParameterConverter(properties, specWorkflow, dagData, taskDefinition);

            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("sqoop-node");

            converter.convertParameter(specNode);

            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.DI.getName(), script.getRuntime().getCommand());
            Assert.assertEquals(CodeProgramType.DI.getCalcEngineType().getLabel(), script.getRuntime().getEngine());

            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("mysql_source")); // Source datasource name
            Assert.assertTrue(content.contains("source_table")); // Source table
            Assert.assertTrue(content.contains("hive_source")); // Target datasource name
            Assert.assertTrue(content.contains("target_table")); // Target table
            Assert.assertTrue(content.contains("dt=20240515,region=cn-hangzhou")); // Partition info
        }
    }

    @Test
    public void testConvertHdfsToMysql() {
        SourceHdfsParameter sourceHdfs = new SourceHdfsParameter();
        sourceHdfs.setExportDir("/data/source/path");

        TargetMysqlParameter targetMysql = new TargetMysqlParameter();
        targetMysql.setTargetDatasource(1);
        targetMysql.setTargetTable("target_table");
        targetMysql.setTargetColumns("id,name,age");
        targetMysql.setTargetUpdateMode("updateonly");
        targetMysql.setPreQuery("TRUNCATE TABLE target_table");

        SqoopParameters sqoopParams = new SqoopParameters();
        sqoopParams.setSourceType("HDFS");
        sqoopParams.setTargetType("MYSQL");
        sqoopParams.setSourceParams(objectMapper.valueToTree(sourceHdfs).toString());
        sqoopParams.setTargetParams(objectMapper.valueToTree(targetMysql).toString());

        taskDefinition.setTaskParams(objectMapper.valueToTree(sqoopParams).toString());

        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getDataSources()).thenReturn(dataSources);

            SqoopParameterConverter converter = new SqoopParameterConverter(properties, specWorkflow, dagData, taskDefinition);

            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("sqoop-node");

            converter.convertParameter(specNode);

            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);
            Assert.assertEquals(CodeProgramType.DI.getName(), script.getRuntime().getCommand());

            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("/data/source/path")); // Source HDFS path
            Assert.assertTrue(content.contains("mysql_source")); // Target datasource name
            Assert.assertTrue(content.contains("target_table")); // Target table
            Assert.assertTrue(content.contains("update")); // Update mode
            Assert.assertTrue(content.contains("TRUNCATE TABLE target_table")); // Pre-SQL
        }
    }

    @Test
    public void testConvertHiveToHdfs() {
        SourceHiveParameter sourceHive = new SourceHiveParameter();
        sourceHive.setHiveDatabase("2");
        sourceHive.setHiveTable("source_table");
        sourceHive.setHivePartitionKey("dt");
        sourceHive.setHivePartitionValue("20240515");

        TargetHdfsParameter targetHdfs = new TargetHdfsParameter();
        targetHdfs.setTargetPath("/data/target/path");
        targetHdfs.setCompressionCodec("gzip");
        targetHdfs.setFileType("parquet");

        SqoopParameters sqoopParams = new SqoopParameters();
        sqoopParams.setSourceType("HIVE");
        sqoopParams.setTargetType("HDFS");
        sqoopParams.setSourceParams(objectMapper.valueToTree(sourceHive).toString());
        sqoopParams.setTargetParams(objectMapper.valueToTree(targetHdfs).toString());

        taskDefinition.setTaskParams(objectMapper.valueToTree(sqoopParams).toString());

        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getDataSources()).thenReturn(dataSources);

            SqoopParameterConverter converter = new SqoopParameterConverter(properties, specWorkflow, dagData, taskDefinition);

            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("sqoop-node");

            converter.convertParameter(specNode);

            SpecScript script = specNode.getScript();
            Assert.assertNotNull(script);

            String content = script.getContent();
            Assert.assertNotNull(content);
            Assert.assertTrue(content.contains("hive_source")); // Source datasource name
            Assert.assertTrue(content.contains("source_table")); // Source table
            Assert.assertTrue(content.contains("dt=20240515")); // Partition info
        }
    }

    @Test(expected = RuntimeException.class)
    public void testConvertWithInvalidTaskParams() {
        taskDefinition.setTaskParams("invalid-json");

        try (MockedStatic<DolphinSchedulerV3Context> contextMock = Mockito.mockStatic(DolphinSchedulerV3Context.class)) {
            contextMock.when(DolphinSchedulerV3Context::getContext).thenReturn(mockContext);
            when(mockContext.getDataSources()).thenReturn(dataSources);

            SqoopParameterConverter converter = new SqoopParameterConverter(properties, specWorkflow, dagData, taskDefinition);

            SpecNode specNode = new SpecNode();
            specNode.setId("node-1");
            specNode.setName("sqoop-node");

            converter.convertParameter(specNode);
        }
    }
} 