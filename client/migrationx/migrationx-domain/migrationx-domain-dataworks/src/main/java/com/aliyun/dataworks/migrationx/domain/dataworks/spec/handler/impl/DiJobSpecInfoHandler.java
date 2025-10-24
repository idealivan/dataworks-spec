package com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDataIntegrationJob;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.BaseSpecInfoHandler;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-08-20
 */
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class DiJobSpecInfoHandler extends BaseSpecInfoHandler<SpecDataIntegrationJob> {

    @Override
    public boolean support(Specification<DataWorksWorkflowSpec> specification) {
        return SpecKind.DATA_INTEGRATION_JOB.equals(getSpecKind(specification));
    }

    @Override
    protected SpecDataIntegrationJob get(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getDataIntegrationJobs)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }

    @Override
    public String getSpecName(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecDataIntegrationJob::getName)
            .orElse(null);
    }

    @Override
    public String getOwner(Specification<DataWorksWorkflowSpec> specification) {
        SpecDataIntegrationJob specDataIntegrationJob = get(specification);
        return Optional.ofNullable(specDataIntegrationJob)
            .map(SpecDataIntegrationJob::getOwner)
            .orElseGet(() -> getOwner(specDataIntegrationJob));
    }

    @Override
    public String getDescription(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecDataIntegrationJob::getDescription)
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
            .map(SpecDataIntegrationJob::getScript)
            .orElse(null);
    }

    @Override
    public void resetUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getDataIntegrationJobs)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .forEach(specDataIntegrationJob -> {
                resetUuid4Entity(specDataIntegrationJob, uuidMap);
            });
    }

    @Override
    public void replaceUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        super.replaceUuid(specification, uuidMap);
    }
}
