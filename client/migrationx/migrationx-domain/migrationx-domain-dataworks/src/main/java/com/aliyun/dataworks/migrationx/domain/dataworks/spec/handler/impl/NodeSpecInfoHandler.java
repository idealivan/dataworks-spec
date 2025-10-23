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
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecRuntimeResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.BaseSpecInfoHandler;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.ObjectUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-08-20
 */
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class NodeSpecInfoHandler extends BaseSpecInfoHandler<SpecNode> {

    @Override
    public boolean support(Specification<DataWorksWorkflowSpec> specification) {
        SpecKind specKind = getSpecKind(specification);
        return SpecKind.NODE.equals(specKind) || SpecKind.MANUAL_NODE.equals(specKind) || isOldVersion(specification, specKind);
    }

    private boolean isOldVersion(Specification<DataWorksWorkflowSpec> specification, SpecKind specKind) {
        List<SpecNode> nodes = Optional.ofNullable(specification).map(Specification::getSpec).map(DataWorksWorkflowSpec::getNodes)
            .orElse(Lists.newArrayList());
        List<SpecWorkflow> workflows = Optional.ofNullable(specification).map(Specification::getSpec).map(DataWorksWorkflowSpec::getWorkflows)
            .orElse(Lists.newArrayList());
        return SpecKind.CYCLE_WORKFLOW.equals(specKind) && nodes.size() == 1 && workflows.isEmpty();
    }

    @Override
    protected SpecNode get(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getNodes)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .findFirst()
            .orElse(null);
    }

    @Override
    public List<Specification<DataWorksWorkflowSpec>> getInnerSpecifications(Specification<DataWorksWorkflowSpec> specification) {
        List<Specification<DataWorksWorkflowSpec>> res = super.getInnerSpecifications(specification);
        SpecNode specNode = ObjectUtils.getIfNull(get(specification), SpecNode::new);
        res.addAll(ListUtils.emptyIfNull(splitSpecNodeInContainer(specification, specNode, specNode.getId())));
        log.info("getInnerSpecifications: {}", res);
        return res;
    }

    @Override
    public String getSpecName(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecNode::getName)
            .orElse(null);
    }

    @Override
    public String getOwner(Specification<DataWorksWorkflowSpec> specification) {
        SpecNode specNode = get(specification);
        return Optional.ofNullable(specNode)
            .map(SpecNode::getOwner)
            .orElseGet(() -> getOwner(specNode));
    }

    @Override
    public String getDescription(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecNode::getDescription)
            .orElse(null);
    }

    @Override
    public String getDataSourceName(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecNode::getDatasource)
            .map(SpecDatasource::getName)
            .orElse(null);
    }

    @Override
    public String getResourceGroupIdentifier(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecNode::getRuntimeResource)
            .map(SpecRuntimeResource::getResourceGroup)
            .orElse(null);
    }

    @Override
    public String getResourceGroupId(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(SpecNode::getRuntimeResource)
            .map(SpecRuntimeResource::getResourceGroupId)
            .orElse(null);
    }

    @Override
    public void resetUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        if (uuidMap == null) {
            return;
        }
        Deque<SpecNode> deque = new ArrayDeque<>();
        Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getNodes)
            .ifPresent(deque::addAll);
        // reset id
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
        super.replaceUuid(specification, uuidMap);
        // replace all value refer to id
        Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getFlow)
            .ifPresent(flows -> replaceIdInDependencies(flows, uuidMap));

        Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getNodes)
            .orElse(Lists.newArrayList()).stream()
            .filter(Objects::nonNull)
            .forEach(node -> {
                replaceIdInInputOutput(node, uuidMap);
                replaceIdInScriptWired(node, uuidMap);
                Optional.ofNullable(node.getSubflow()).ifPresent(subflow -> subflow.setId(getMappingId(uuidMap, subflow.getId())));
            });

        ListUtils.emptyIfNull(getInnerSpecifications(specification))
            .forEach(spec -> replaceUuid(spec, uuidMap));
    }
}
