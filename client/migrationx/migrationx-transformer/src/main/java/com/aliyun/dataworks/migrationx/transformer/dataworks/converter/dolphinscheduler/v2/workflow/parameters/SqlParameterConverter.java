/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v2.workflow.parameters;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.DagData;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.entity.DataSource;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.enums.DbType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v2.task.sql.SqlParameters;
import com.aliyun.dataworks.migrationx.transformer.core.common.Constants;
import com.aliyun.migrationx.common.utils.GsonUtils;

import com.google.gson.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;

@Slf4j
public class SqlParameterConverter extends AbstractParameterConverter<SqlParameters> {

    public SqlParameterConverter(Properties properties, SpecWorkflow specWorkflow, DagData processMeta, TaskDefinition taskDefinition) {
        super(properties, specWorkflow, processMeta, taskDefinition);
    }

    @Override
    protected void convertParameter(SpecNode specNode) {
        List<SpecVariable> specVariableList = convertSpecNodeParam(specNode);
        String sqlNodeMapStr = properties.getProperty(
                Constants.CONVERTER_TARGET_SQL_NODE_TYPE_MAP, "{}");
        Map<String, String> sqlTypeNodeTypeMapping = GsonUtils.fromJsonString(sqlNodeMapStr,
                new TypeToken<Map<String, String>>() {}.getType());

        String type = Optional.ofNullable(sqlTypeNodeTypeMapping)
                .map(s -> s.get(parameter.getType()))
                .orElseGet(() -> {
                    if (DbType.HIVE.name().equalsIgnoreCase(parameter.getType())) {
                        return CodeProgramType.EMR_HIVE.name();
                    } else if (DbType.SPARK.name().equalsIgnoreCase(parameter.getType())) {
                        return CodeProgramType.EMR_SPARK.name();
                    } else if (DbType.ofType(parameter.getType()) != null) {
                        return parameter.getType();
                    } else {
                        String defaultNodeTypeIfNotSupport = getSQLConverterType();
                        log.warn("using default node Type {} for node {}", defaultNodeTypeIfNotSupport, taskDefinition.getName());
                        return defaultNodeTypeIfNotSupport;
                    }
                });

        CodeProgramType codeProgramType = CodeProgramType.getNodeTypeByName(type);
        SpecScript script = new SpecScript();
        String language = codeToLanguageIdentifier(codeProgramType);
        script.setLanguage(language);
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setEngine(codeProgramType.getCalcEngineType().getLabel());
        runtime.setCommand(codeProgramType.getName());
        script.setRuntime(runtime);

        script.setPath(getScriptPath(specNode));
        String content = parameter.getSql();
        content = replaceCodeWithParams(content, specVariableList);
        script.setContent(content);
        script.setParameters(ListUtils.emptyIfNull(specVariableList).stream().filter(v -> !VariableType.NODE_OUTPUT.equals(v.getType()))
                .collect(Collectors.toList()));
        specNode.setScript(script);
        SpecDatasource datasource = getDataSource(codeProgramType);
        specNode.setDatasource(datasource);
        postHandle("SQL", script);
    }

    private SpecDatasource getDataSource(CodeProgramType codeProgramType) {
        DataSource dataSource = getDataSourceById(parameter.getDatasource());
        String connName = null;
        if (dataSource != null) {
            connName = dataSource.getName();
        }
        String type = null;
        switch (codeProgramType) {
            case MYSQL:
                type = "mysql";
                break;
            case POSTGRESQL:
                type = "postgresql";
                break;
            case EMR_HIVE:
                type = "emr";
                break;
            case CLICK_SQL:
                type = "clickhouse";
                break;
            case Oracle:
                type = "oracle";
                break;
            case ODPS_SQL:
                type = "odps";
        }
        if (connName != null) {
            SpecDatasource datasource = new SpecDatasource();
            datasource.setName(connName);
            datasource.setType(type);
            return datasource;
        }
        return null;
    }

    private String getSQLConverterType() {
        String convertType = properties.getProperty(Constants.CONVERTER_TARGET_COMMAND_SQL_TYPE_AS);
        return getConverterType(convertType, CodeProgramType.SQL_COMPONENT.name());
    }
}
