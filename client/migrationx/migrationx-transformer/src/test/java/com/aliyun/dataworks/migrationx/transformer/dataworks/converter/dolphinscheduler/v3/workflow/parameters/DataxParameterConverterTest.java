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
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.ProcessDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.entity.Project;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DbType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.Flag;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.task.datax.DataxParameters;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.migrationx.common.utils.Config;
import com.aliyun.migrationx.common.utils.GsonUtils;

import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataxParameterConverterTest {

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
        taskDefinition.setTaskType(TaskType.DATAX.name());
        taskDefinition.setName("datax-task");
        
        List<Project> projects = new ArrayList<>();
        List<DagData> dagDatas = new ArrayList<>();
        List<DataSource> dataSources = new ArrayList<>();
        
        DataSource mysqlSource = new DataSource();
        mysqlSource.setId(1);
        mysqlSource.setType(DbType.MYSQL);
        mysqlSource.setName("mysql_source");
        
        DataSource postgresTarget = new DataSource();
        postgresTarget.setId(2);
        postgresTarget.setType(DbType.POSTGRESQL);
        postgresTarget.setName("postgres_target");
        
        dataSources.add(mysqlSource);
        dataSources.add(postgresTarget);
        
        dagDatas.add(dagData);
        DolphinSchedulerV3Context.initContext(projects, dagDatas, dataSources, new ArrayList<>(), new ArrayList<>());
        context = DolphinSchedulerV3Context.getContext();
    }

    @Test
    public void testConvertParameterWithCustomConfig() {
        DataxParameters dataxParameters = new DataxParameters();
        dataxParameters.setCustomConfig(Flag.YES.ordinal());
        dataxParameters.setJson("{\"job\":{\"content\":[{\"reader\":{\"name\":\"mysqlreader\",\"parameter\":{\"username\":\"root\",\"password\":\"root\",\"column\":[\"id\",\"name\"],\"connection\":[{\"table\":[\"user\"],\"jdbcUrl\":[\"jdbc:mysql://localhost:3306/test\"]}]}},\"writer\":{\"name\":\"postgreswriter\",\"parameter\":{\"username\":\"postgres\",\"password\":\"postgres\",\"column\":[\"id\",\"name\"],\"connection\":[{\"table\":[\"user\"],\"jdbcUrl\":\"jdbc:postgresql://localhost:5432/test\"}]}}}],\"setting\":{\"speed\":{\"channel\":1}}}}");
        
        JsonObject taskParamsJson = GsonUtils.toJsonObject(dataxParameters);
        taskDefinition.setTaskParams(GsonUtils.toJsonString(taskParamsJson));
        
        DataxParameterConverter converter = new DataxParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-1");
        specNode.setName("datax-custom-node");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        Assert.assertEquals("DI", script.getRuntime().getCommand());
        Assert.assertEquals(CodeProgramType.DI.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
        Assert.assertTrue("Script content should contain the custom JSON", script.getContent().contains("mysqlreader"));
    }

    @Test
    public void testConvertParameterWithAutoGenConfig() {
        DataxParameters dataxParameters = new DataxParameters();
        dataxParameters.setCustomConfig(Flag.NO.ordinal());
        dataxParameters.setDataSource(1); // MySQL source
        dataxParameters.setDataTarget(2); // PostgreSQL target
        dataxParameters.setSql("SELECT id, name FROM source_table");
        dataxParameters.setTargetTable("target_table");
        dataxParameters.setJobSpeedByte(1024);
        dataxParameters.setJobSpeedRecord(100);
        
        JsonObject taskParamsJson = GsonUtils.toJsonObject(dataxParameters);
        taskDefinition.setTaskParams(GsonUtils.toJsonString(taskParamsJson));
        
        DataxParameterConverter converter = new DataxParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-2");
        specNode.setName("datax-auto-node");
        
        converter.convertParameter(specNode);

        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        Assert.assertEquals("DI", script.getRuntime().getCommand());
        Assert.assertEquals(CodeProgramType.DI.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
        
        String content = script.getContent();
        Assert.assertTrue("Script content should contain job configuration", content.contains("\"type\":\"job\""));
        Assert.assertTrue("Script content should contain reader configuration", content.contains("\"category\":\"reader\""));
        Assert.assertTrue("Script content should contain writer configuration", content.contains("\"category\":\"writer\""));
        Assert.assertTrue("Script content should contain speed settings", content.contains("\"speed\""));
    }

    @Test
    public void testConvertParameterWithComplexSql() {
        DataxParameters dataxParameters = new DataxParameters();
        dataxParameters.setCustomConfig(Flag.NO.ordinal());
        dataxParameters.setDataSource(1); // MySQL source
        dataxParameters.setDataTarget(2); // PostgreSQL target
        dataxParameters.setSql("SELECT a.id as user_id, a.name as user_name, b.dept_name FROM user a JOIN department b ON a.dept_id = b.id");
        dataxParameters.setTargetTable("user_department");
        
        JsonObject taskParamsJson = GsonUtils.toJsonObject(dataxParameters);
        taskDefinition.setTaskParams(GsonUtils.toJsonString(taskParamsJson));
        
        DataxParameterConverter converter = new DataxParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-3");
        specNode.setName("datax-complex-sql-node");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        
        Assert.assertNotNull("Script content should not be null", script.getContent());
    }

    @Test
    public void testConvertParameterWithSelectAll() {
        DataxParameters dataxParameters = new DataxParameters();
        dataxParameters.setCustomConfig(Flag.NO.ordinal());
        dataxParameters.setDataSource(1); // MySQL source
        dataxParameters.setDataTarget(2); // PostgreSQL target
        dataxParameters.setSql("SELECT * FROM user");
        dataxParameters.setTargetTable("user_copy");
        
        JsonObject taskParamsJson = GsonUtils.toJsonObject(dataxParameters);
        taskDefinition.setTaskParams(GsonUtils.toJsonString(taskParamsJson));
        
        DataxParameterConverter converter = new DataxParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-4");
        specNode.setName("datax-select-all-node");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        
        String content = script.getContent();
        Assert.assertTrue("Script content should contain column configuration", content.contains("\"column\""));
    }

    @Test
    public void testConvertParameterWithCustomNodeType() {
        properties.setProperty(Constants.CONVERTER_TARGET_DATAX_NODE_TYPE_AS, CodeProgramType.ODPS_SQL.name());
        
        DataxParameters dataxParameters = new DataxParameters();
        dataxParameters.setCustomConfig(Flag.NO.ordinal());
        dataxParameters.setDataSource(1);
        dataxParameters.setDataTarget(2);
        dataxParameters.setSql("SELECT id, name FROM source_table");
        dataxParameters.setTargetTable("target_table");
        
        JsonObject taskParamsJson = GsonUtils.toJsonObject(dataxParameters);
        taskDefinition.setTaskParams(GsonUtils.toJsonString(taskParamsJson));
        
        DataxParameterConverter converter = new DataxParameterConverter(properties, specWorkflow, dagData, taskDefinition);
        SpecNode specNode = new SpecNode();
        specNode.setFileResources(new ArrayList<>());
        specNode.setId("node-5");
        specNode.setName("datax-custom-type-node");
        
        converter.convertParameter(specNode);
        
        SpecScript script = specNode.getScript();
        Assert.assertNotNull("Script should not be null", script);
        Assert.assertEquals("ODPS_SQL", script.getRuntime().getCommand());
        Assert.assertEquals(CodeProgramType.ODPS_SQL.getCalcEngineType().getLabel(), script.getRuntime().getEngine());
    }
} 