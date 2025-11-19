package com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.BaseSpecInfoHandler;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.ObjectUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-08-20
 */
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class FlowSpecInfoHandler extends BaseSpecInfoHandler<SpecWorkflow> {

    private final NodeSpecInfoHandler nodeSpecInfoHandler = new NodeSpecInfoHandler();

    @Override
    public boolean support(Specification<DataWorksWorkflowSpec> specification) {
        SpecKind specKind = getSpecKind(specification);
        List<SpecWorkflow> specWorkflows = Optional.ofNullable(specification).map(Specification::getSpec).map(DataWorksWorkflowSpec::getWorkflows)
            .orElse(Collections.emptyList());
        return supportKind(specKind) && CollectionUtils.isNotEmpty(specWorkflows);
    }

    private boolean supportKind(SpecKind specKind) {
        return SpecKind.CYCLE_WORKFLOW.equals(specKind)
            || SpecKind.MANUAL_WORKFLOW.equals(specKind)
            || SpecKind.TRIGGER_WORKFLOW.equals(specKind);
    }

    @Override
    protected SpecWorkflow get(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getWorkflows)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }

    @Override
    public List<Specification<DataWorksWorkflowSpec>> getInnerSpecifications(Specification<DataWorksWorkflowSpec> specification) {
        List<Specification<DataWorksWorkflowSpec>> res = super.getInnerSpecifications(specification);
        SpecWorkflow specWorkflow = ObjectUtils.getIfNull(get(specification), SpecWorkflow::new);
        res.addAll(ListUtils.emptyIfNull(splitSpecNodeInContainer(specification, specWorkflow, specWorkflow.getId())));
        log.info("getInnerSpecifications: {}", res);
        return res;
    }

    @Override
    public String getSpecName(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecWorkflow::getName)
            .orElse(null);
    }

    @Override
    public String getOwner(Specification<DataWorksWorkflowSpec> specification) {
        SpecWorkflow specWorkflow = get(specification);
        return Optional.ofNullable(specWorkflow)
            .map(SpecWorkflow::getOwner)
            .orElseGet(() -> getOwner(specWorkflow));
    }

    @Override
    public String getDescription(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecWorkflow::getDescription)
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
    public void resetUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        if (uuidMap == null) {
            return;
        }
        List<SpecWorkflow> workflows = Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getWorkflows)
            .orElse(Collections.emptyList());
        Deque<SpecNode> deque = new ArrayDeque<>();

        // reset workflow id
        workflows.stream()
            .filter(Objects::nonNull)
            .forEach(workflow -> {
                resetUuid4Entity(workflow, uuidMap);
                deque.addAll(workflow.getInnerNodes());
            });

        // reset node id
        while (!deque.isEmpty()) {
            SpecNode poll = deque.poll();
            if (poll == null) {
                continue;
            }
            resetUuid4Entity(poll, uuidMap);
            deque.addAll(ListUtils.emptyIfNull(poll.getInnerNodes()));
        }
    }

    @Override
    public void replaceUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        if (uuidMap == null) {
            return;
        }
        super.replaceUuid(specification, uuidMap);
        // replace all value refer to id
        Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getFlow)
            .ifPresent(flows -> replaceIdInDependencies(flows, uuidMap));

        Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getWorkflows)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .map(SpecWorkflow::getDependencies)
            .filter(Objects::nonNull)
            .forEach(flows -> replaceIdInDependencies(flows, uuidMap));

        Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getWorkflows)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .forEach(workflow -> {
                replaceIdInInputOutput(workflow, uuidMap);
                replaceIdInScriptWired(workflow, uuidMap);
            });
        ListUtils.emptyIfNull(getInnerSpecifications(specification))
            .forEach(spec -> nodeSpecInfoHandler.replaceUuid(spec, uuidMap));
    }
}
