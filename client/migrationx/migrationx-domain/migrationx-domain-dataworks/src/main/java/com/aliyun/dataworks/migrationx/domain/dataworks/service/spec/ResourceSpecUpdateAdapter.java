package com.aliyun.dataworks.migrationx.domain.dataworks.service.spec;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import com.alibaba.fastjson.JSON;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.enums.SpecStorageType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecVersion;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFileResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecRuntimeResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.file.SpecObjectStorageFile;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity.DwNodeEntity;
import com.aliyun.dataworks.migrationx.domain.dataworks.utils.SpecFileResourceTypeUtils;
import com.aliyun.migrationx.common.utils.UuidUtils;
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
public class ResourceSpecUpdateAdapter {

    /**
     * update info in specification by dwNode
     *
     * @param dwNode        dwNode
     * @param specification specification
     */
    public void updateSpecification(DwNodeEntity dwNode, Specification<DataWorksWorkflowSpec> specification) {
        specification.setVersion(SpecVersion.V_1_1_0.getLabel());
        specification.setKind(SpecKind.RESOURCE.getLabel());

        Map<String, Object> metadata = ObjectUtils.defaultIfNull(specification.getMetadata(), new LinkedHashMap<>());
        Optional.ofNullable(dwNode.getOwner()).ifPresent(owner -> metadata.put("owner", owner));
        Optional.ofNullable(dwNode.getUuid()).ifPresent(uuid -> metadata.put("uuid", uuid));
        Optional.ofNullable(dwNode.getParentId()).ifPresent(containerId -> metadata.put("containerId", String.valueOf(containerId)));
        specification.setMetadata(metadata);

        DataWorksWorkflowSpec spec = ObjectUtils.defaultIfNull(specification.getSpec(), new DataWorksWorkflowSpec());
        Optional.ofNullable(dwNode.getBizId()).filter(id -> id > 0).map(String::valueOf).ifPresent(spec::setId);
        Optional.ofNullable(dwNode.getBizName()).ifPresent(spec::setName);
        specification.setSpec(spec);

        SpecFileResource specFileResource = ListUtils.emptyIfNull(spec.getFileResources()).stream()
            .findFirst()
            .orElseGet(SpecFileResource::new);
        spec.setFileResources(Collections.singletonList(specFileResource));

        fillSpecFileResource(dwNode, specFileResource);

        log.info("specification: {}", JSON.toJSONString(specification));
    }

    private void fillSpecFileResource(DwNodeEntity dwNode, SpecFileResource specFileResource) {
        SpecScript script = ObjectUtils.defaultIfNull(specFileResource.getScript(), new SpecScript());
        specFileResource.setScript(script);

        // parse spec file resource
        fillSpecFileResourceBasicInfo(dwNode, specFileResource);

        // parse script
        fillSpecScript(dwNode, script, specFileResource);

        // parse resource group and datasource
        fillResourceGroup(dwNode, specFileResource);
        fillDatasource(dwNode, specFileResource);

        // parse file
        fillFile(dwNode, specFileResource);
    }

    /**
     * fill spec file resource basic info
     *
     * @param dwNode           dwNode
     * @param specFileResource spec file resource need to fill
     */
    private void fillSpecFileResourceBasicInfo(DwNodeEntity dwNode, SpecFileResource specFileResource) {
        String id = Optional.ofNullable(dwNode.getUuid()).orElseGet(UuidUtils::genUuidWithoutHorizontalLine);
        specFileResource.setId(id);
        Optional.ofNullable(dwNode.getName()).ifPresent(specFileResource::setName);
        specFileResource.setType(SpecFileResourceTypeUtils.getResourceTypeBySuffix(specFileResource.getName()));
    }

    /**
     * fill script
     *
     * @param dwNode           node
     * @param script           script
     * @param specFileResource spec file resource
     */
    private void fillSpecScript(DwNodeEntity dwNode, SpecScript script, SpecFileResource specFileResource) {
        Optional.ofNullable(dwNode.getFolder())
            .map(path -> FilenameUtils.concat(path, specFileResource.getName()))
            .ifPresent(script::setPath);

        // check file path and node name are logically consistent
        if (script.getPath() == null || !StringUtils.equals(FilenameUtils.getName(script.getPath()), specFileResource.getName())) {
            String parentPath = FilenameUtils.getFullPathNoEndSeparator(script.getPath());
            script.setPath(FilenameUtils.concat(parentPath, specFileResource.getName()));
        }

        Optional.ofNullable(dwNode.getCode()).ifPresent(script::setContent);

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
                runtime.setCommand(codeProgramType.name());
                runtime.setEngine(codeProgramType.getCalcEngineType().getLabel());
            });
        Optional.ofNullable(dwNode.getTypeId())
            .ifPresent(runtime::setCommandTypeId);

        Optional.ofNullable(dwNode.getCu())
            .ifPresent(runtime::setCu);
    }

    /**
     * fill resource group
     *
     * @param dwNode           dwNode
     * @param specFileResource spec file resource
     */
    private void fillResourceGroup(DwNodeEntity dwNode, SpecFileResource specFileResource) {
        // parse resource group
        if (StringUtils.isNotBlank(dwNode.getResourceGroup())) {
            SpecRuntimeResource specRuntimeResource = new SpecRuntimeResource();
            specRuntimeResource.setResourceGroup(dwNode.getResourceGroup());
            specFileResource.setRuntimeResource(specRuntimeResource);
        }
    }

    /**
     * fill datasource
     *
     * @param dwNode           dwNode
     * @param specFileResource spec file resource
     */
    private void fillDatasource(DwNodeEntity dwNode, SpecFileResource specFileResource) {
        // parse datasource
        if (StringUtils.isNotBlank(dwNode.getConnection())) {
            SpecDatasource specDatasource = new SpecDatasource();
            specDatasource.setName(dwNode.getConnection());
            specFileResource.setDatasource(specDatasource);
        }
    }

    /**
     * fill file
     *
     * @param dwNode           dwNode
     * @param specFileResource spec node
     */
    private void fillFile(DwNodeEntity dwNode, SpecFileResource specFileResource) {
        // pop做了一层转换，会把客户上传的文件托管到我们自己的oss，所以到我们这里都认为是本地上传
        specFileResource.setFile(SpecObjectStorageFile.newInstanceOf(SpecStorageType.LOCAL));
        Optional.ofNullable(dwNode.getStorageUri()).ifPresent(specFileResource.getFile()::setPath);
    }
}
