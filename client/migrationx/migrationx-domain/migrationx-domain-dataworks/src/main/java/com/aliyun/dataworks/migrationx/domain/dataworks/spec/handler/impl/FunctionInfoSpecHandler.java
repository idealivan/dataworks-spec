package com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFunction;
import com.aliyun.dataworks.common.spec.domain.ref.SpecRuntimeResource;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.BaseSpecInfoHandler;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-08-20
 */
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class FunctionInfoSpecHandler extends BaseSpecInfoHandler<SpecFunction> {

    @Override
    public boolean support(Specification<DataWorksWorkflowSpec> specification) {
        SpecKind specKind = getSpecKind(specification);
        return SpecKind.FUNCTION.equals(specKind) || isOldVersion(specification, specKind);
    }

    private boolean isOldVersion(Specification<DataWorksWorkflowSpec> specification, SpecKind specKind) {
        List<SpecFunction> specFunctions = Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getFunctions)
            .orElse(null);
        return SpecKind.CYCLE_WORKFLOW.equals(specKind) && CollectionUtils.isNotEmpty(specFunctions);
    }

    @Override
    protected SpecFunction get(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getFunctions)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }

    @Override
    public String getSpecName(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecFunction::getName)
            .orElse(null);
    }

    @Override
    public String getOwner(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(this::getOwner)
            .orElse(null);
    }

    @Override
    public String getDescription(Specification<DataWorksWorkflowSpec> specification) {
        return null;
    }

    @Override
    public String getDataSourceName(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecFunction::getDatasource)
            .map(SpecDatasource::getName)
            .orElse(null);
    }

    @Override
    public String getResourceGroupIdentifier(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecFunction::getRuntimeResource)
            .map(SpecRuntimeResource::getResourceGroup)
            .orElse(null);
    }

    @Override
    public String getResourceGroupId(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecFunction::getRuntimeResource)
            .map(SpecRuntimeResource::getResourceGroupId)
            .orElse(null);
    }

    @Override
    public void resetUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getFunctions)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .forEach(function -> {
                resetUuid4Entity(function, uuidMap);
            });
    }

    @Override
    public void replaceUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        super.replaceUuid(specification, uuidMap);
    }
}
