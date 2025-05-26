package com.aliyun.dataworks.migrationx.domain.dataworks.service.spec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.alibaba.fastjson2.JSON;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.FunctionType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecEmbeddedResourceType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.enums.SpecVersion;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFileResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFunction;
import com.aliyun.dataworks.common.spec.domain.ref.SpecRuntimeResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.common.spec.utils.UuidUtils;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity.DwNodeEntity;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 云异
 * @date 2025/4/20
 */
@Slf4j
public class FunctionSpecUpdateAdapter {

    /**
     * update info in specification by dwNode
     *
     * @param dwNode        dwNode
     * @param specification specification
     */
    public void updateSpecification(DwNodeEntity dwNode, Specification<DataWorksWorkflowSpec> specification) {
        specification.setVersion(SpecVersion.V_1_1_0.getLabel());
        specification.setKind(SpecKind.FUNCTION.getLabel());

        Map<String, Object> metadata = ObjectUtils.defaultIfNull(specification.getMetadata(), new LinkedHashMap<>());
        Optional.ofNullable(dwNode.getOwner()).ifPresent(owner -> metadata.put("owner", owner));
        Optional.ofNullable(dwNode.getUuid()).ifPresent(uuid -> metadata.put("uuid", uuid));
        Optional.ofNullable(dwNode.getParentId()).ifPresent(containerId -> metadata.put("containerId", String.valueOf(containerId)));
        specification.setMetadata(metadata);

        DataWorksWorkflowSpec spec = ObjectUtils.defaultIfNull(specification.getSpec(), new DataWorksWorkflowSpec());
        Optional.ofNullable(dwNode.getBizId()).filter(id -> id > 0).map(String::valueOf).ifPresent(spec::setId);
        Optional.ofNullable(dwNode.getBizName()).ifPresent(spec::setName);
        specification.setSpec(spec);

        SpecFunction specFunction = ListUtils.emptyIfNull(spec.getFunctions()).stream()
            .findFirst()
            .orElseGet(SpecFunction::new);
        spec.setFunctions(Collections.singletonList(specFunction));

        fillSpecFunction(dwNode, specFunction, spec);

        log.info("specification: {}", JSON.toJSONString(specification));
    }

    private void fillSpecFunction(DwNodeEntity dwNode, SpecFunction specFunction, DataWorksWorkflowSpec spec) {
        SpecScript specScript = ObjectUtils.defaultIfNull(specFunction.getScript(), new SpecScript());
        specFunction.setScript(specScript);

        // parse basic info
        fillSpecFunctionBasicInfo(dwNode, specFunction);

        // parse script
        fillSpecScript(dwNode, specScript, specFunction);

        // parse resource group and datasource
        fillResourceGroup(dwNode, specFunction);
        fillDatasource(dwNode, specFunction);

        fillContentAttr(dwNode, specFunction, spec);
    }

    private void fillSpecFunctionBasicInfo(DwNodeEntity dwNode, SpecFunction specFunction) {
        String id = Optional.ofNullable(dwNode.getUuid()).orElseGet(UuidUtils::genUuidWithoutHorizontalLine);
        specFunction.setId(id);
        Optional.ofNullable(dwNode.getName()).ifPresent(specFunction::setName);
    }

    private void fillSpecScript(DwNodeEntity dwNode, SpecScript script, SpecFunction specFunction) {
        Optional.ofNullable(dwNode.getFolder())
            .map(path -> FilenameUtils.concat(path, specFunction.getName()))
            .ifPresent(script::setPath);

        // check file path and node name are logically consistent
        if (script.getPath() == null || !StringUtils.equals(FilenameUtils.getName(script.getPath()), specFunction.getName())) {
            String parentPath = FilenameUtils.getFullPathNoEndSeparator(script.getPath());
            script.setPath(FilenameUtils.concat(parentPath, specFunction.getName()));
        }

        fillSpecScriptRuntime(dwNode, script);
    }

    /**
     * fill script runtime
     *
     * @param dwNode dwNode
     * @param script script
     */
    private void fillSpecScriptRuntime(DwNodeEntity dwNode, SpecScript script) {
        SpecScriptRuntime runtime = ObjectUtils.defaultIfNull(script.getRuntime(), new SpecScriptRuntime());
        script.setRuntime(runtime);

        Optional.ofNullable(dwNode.getType())
            .map(CodeProgramType::getNodeTypeByName)
            .ifPresent(codeProgramType -> {
                runtime.setCommand(codeProgramType.getName());
                runtime.setEngine(codeProgramType.getCalcEngineType().getLabel());
            });

        Optional.ofNullable(dwNode.getCu())
            .ifPresent(runtime::setCu);
    }

    /**
     * fill resource group
     *
     * @param dwNode       dwNode
     * @param specFunction spec function
     */
    private void fillResourceGroup(DwNodeEntity dwNode, SpecFunction specFunction) {
        // parse resource group
        if (StringUtils.isNotBlank(dwNode.getResourceGroup())) {
            SpecRuntimeResource specRuntimeResource = new SpecRuntimeResource();
            specRuntimeResource.setResourceGroup(dwNode.getResourceGroup());
            specFunction.setRuntimeResource(specRuntimeResource);
        }
    }

    /**
     * fill datasource
     *
     * @param dwNode       dwNode
     * @param specFunction spec function
     */
    private void fillDatasource(DwNodeEntity dwNode, SpecFunction specFunction) {
        // parse datasource
        if (StringUtils.isNotBlank(dwNode.getConnection())) {
            SpecDatasource specDatasource = new SpecDatasource();
            specDatasource.setName(dwNode.getConnection());
            specFunction.setDatasource(specDatasource);
        }
    }

    private void fillContentAttr(DwNodeEntity dwNode, SpecFunction specFunction, DataWorksWorkflowSpec spec) {
        if (StringUtils.isBlank(dwNode.getCode())) {
            return;
        }
        UdfDefinition udfDefinition = JSON.parseObject(dwNode.getCode(), UdfDefinition.class);
        Optional.ofNullable(udfDefinition.getFunctionType()).ifPresent(specFunction::setType);
        Optional.ofNullable(udfDefinition.getClassName()).ifPresent(specFunction::setClassName);
        Optional.ofNullable(udfDefinition.getCmdDesc()).ifPresent(specFunction::setUsageDescription);
        Optional.ofNullable(udfDefinition.getDescription()).ifPresent(spec::setDescription);
        Optional.ofNullable(udfDefinition.getParamDesc()).ifPresent(specFunction::setArgumentsDescription);
        Optional.ofNullable(udfDefinition.getReturnValue()).ifPresent(specFunction::setReturnValueDescription);
        Optional.ofNullable(udfDefinition.getExample()).ifPresent(specFunction::setUsageExample);
        Optional.ofNullable(udfDefinition.getResources()).ifPresent(x -> {
            if (StringUtils.isNotBlank(x)) {
                String[] resourceArr = x.split(",");
                List<SpecFileResource> fileResources = new ArrayList<>();
                for (String resource : resourceArr) {
                    SpecFileResource fileResource = new SpecFileResource();
                    fileResource.setName(resource);
                    fileResources.add(fileResource);
                }
                specFunction.setFileResources(fileResources);
            }
        });
        // api 不支持嵌入式函数
        specFunction.setResourceType(SpecEmbeddedResourceType.FILE);
    }

    @Data
    public static final class UdfDefinition {
        private FunctionType functionType;
        private String className;
        private String name;
        private String resources;
        private String description;
        private String cmdDesc;
        private String paramDesc;
        private String returnValue;
        private String example;
        private String schema;
    }
}
