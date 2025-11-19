package com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.component.SpecComponent;
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
public class ComponentSpecInfoHandler extends BaseSpecInfoHandler<SpecComponent> {

    @Override
    public boolean support(Specification<DataWorksWorkflowSpec> specification) {
        SpecKind specKind = getSpecKind(specification);
        return SpecKind.COMPONENT.equals(specKind) || isOldVersion(specification, specKind);
    }

    private boolean isOldVersion(Specification<DataWorksWorkflowSpec> specification, SpecKind specKind) {
        List<SpecComponent> specComponents = Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getComponents)
            .orElse(null);
        return SpecKind.CYCLE_WORKFLOW.equals(specKind) && CollectionUtils.isNotEmpty(specComponents);
    }

    @Override
    protected SpecComponent get(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getComponents)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }

    @Override
    public String getSpecName(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecComponent::getName)
            .orElse(null);
    }

    @Override
    public String getOwner(Specification<DataWorksWorkflowSpec> specification) {
        SpecComponent specComponent = get(specification);
        return Optional.ofNullable(specComponent)
            .map(SpecComponent::getOwner)
            .orElseGet(() -> getOwner(specComponent));
    }

    @Override
    public String getDescription(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecComponent::getDescription)
            .orElse(null);
    }

    @Override
    public String getDataSourceName(Specification<DataWorksWorkflowSpec> specification) {
        return null;
    }

    @Override
    public String getResourceGroupIdentifier(Specification<DataWorksWorkflowSpec> specification) {
        return null;
    }

    @Override
    public String getResourceGroupId(Specification<DataWorksWorkflowSpec> specification) {
        return null;
    }

    @Override
    public SpecScript getSpecScript(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecComponent::getScript)
            .orElse(null);
    }

    @Override
    public void resetUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getComponents)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .forEach(component -> {
                resetUuid4Entity(component, uuidMap);
            });
    }

    @Override
    public void replaceUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        super.replaceUuid(specification, uuidMap);
    }
}
