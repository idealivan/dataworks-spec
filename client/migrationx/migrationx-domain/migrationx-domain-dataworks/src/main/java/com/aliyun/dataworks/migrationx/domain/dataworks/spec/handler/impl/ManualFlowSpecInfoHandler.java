package com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecRuntimeResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.utils.UuidUtils;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.BaseSpecInfoHandler;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-08-20
 */
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class ManualFlowSpecInfoHandler extends BaseSpecInfoHandler<DataWorksWorkflowSpec> {

    private final NodeSpecInfoHandler nodeSpecInfoHandler = new NodeSpecInfoHandler();

    @Override
    public boolean support(Specification<DataWorksWorkflowSpec> specification) {
        SpecKind specKind = getSpecKind(specification);
        List<SpecWorkflow> specWorkflows = Optional.ofNullable(specification).map(Specification::getSpec).map(DataWorksWorkflowSpec::getWorkflows)
            .orElse(Collections.emptyList());
        return SpecKind.MANUAL_WORKFLOW.equals(specKind) && CollectionUtils.isEmpty(specWorkflows);
    }

    @Override
    protected DataWorksWorkflowSpec get(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .orElse(null);
    }

    @Override
    public List<Specification<DataWorksWorkflowSpec>> getInnerSpecifications(Specification<DataWorksWorkflowSpec> specification) {
        List<Specification<DataWorksWorkflowSpec>> res = super.getInnerSpecifications(specification);
        DataWorksWorkflowSpec spec = ObjectUtils.getIfNull(get(specification), DataWorksWorkflowSpec::new);
        // the map is used to find the node dependence for the single node
        Map<String, SpecFlowDepend> nodeDependMap = ListUtils.emptyIfNull(spec.getFlow()).stream().collect(
            Collectors.toMap(fd -> Optional.ofNullable(fd.getNodeId()).map(SpecNode::getId).orElse(NULL_KEY),
                Function.identity(),
                (d1, d2) -> {
                    Set<SpecDepend> set = new HashSet<>();
                    set.addAll(ListUtils.emptyIfNull(d1.getDepends()));
                    set.addAll(ListUtils.emptyIfNull(d2.getDepends()));
                    d1.setDepends(new ArrayList<>(set));
                    return d1;
                }));

        res.addAll(ListUtils.emptyIfNull(spec.getNodes()).stream().map(node -> {
                Specification<DataWorksWorkflowSpec> templateSpecification = buildTemplateSpecification(specification);
                Map<String, Object> metaData = new HashMap<>();
                metaData.put(UUID, node.getId());
                metaData.put(CONTAINER_ID, Optional.ofNullable(specification).map(Specification::getSpec).map(SpecRefEntity::getId).orElse(null));
                templateSpecification.setMetadata(metaData);
                templateSpecification.setKind(SpecKind.NODE.getLabel());
                DataWorksWorkflowSpec tempSpec = new DataWorksWorkflowSpec();
                tempSpec.setNodes(Collections.singletonList(node));
                tempSpec.setFlow(Optional.ofNullable(nodeDependMap.get(StringUtils.defaultString(node.getId(), NULL_KEY)))
                    .map(Collections::singletonList)
                    .orElse(null));
                templateSpecification.setSpec(tempSpec);
                return templateSpecification;
            })
            .collect(Collectors.toList()));
        log.info("getInnerSpecifications: {}", res);
        return res;
    }

    @Override
    public String getSpecId(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(super.getSpecId(specification))
            .orElseGet(() -> Optional.ofNullable(specification)
                .map(Specification::getSpec)
                .map(DataWorksWorkflowSpec::getWorkflows)
                .flatMap(find -> find.stream().findFirst())
                .map(SpecRefEntity::getId)
                .filter(BaseSpecInfoHandler::checkUuidValid)
                .orElse(null));
    }

    @Override
    public String getSpecCommand(Specification<DataWorksWorkflowSpec> specification) {
        return SpecKind.MANUAL_WORKFLOW.name();
    }

    @Override
    public Integer getSpecCommandTypeId(Specification<DataWorksWorkflowSpec> specification) {
        return null;
    }

    @Override
    public String getSpecName(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(DataWorksWorkflowSpec::getName)
            .orElse(null);
    }

    @Override
    public String getSpecPath(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .map(DataWorksWorkflowSpec::getNodes)
            .orElse(Lists.newArrayList()).stream()
            .map(node -> Optional.ofNullable(node).map(SpecNode::getScript).map(SpecScript::getPath).orElse(null))
            .filter(StringUtils::isNotBlank)
            .findFirst()
            .map(FilenameUtils::getPathNoEndSeparator)
            .orElse(null);
    }

    @Override
    public String getSpecLanguage(Specification<DataWorksWorkflowSpec> specification) {
        return null;
    }

    @Override
    public String getScriptContent(Specification<DataWorksWorkflowSpec> specification) {
        return null;
    }

    @Override
    public String getOwner(Specification<DataWorksWorkflowSpec> specification) {
        DataWorksWorkflowSpec spec = get(specification);
        return Optional.ofNullable(spec)
            .map(DataWorksWorkflowSpec::getOwner)
            .orElseGet(() -> Optional.ofNullable(specification).map(Specification::getSpec)
                .map(DataWorksWorkflowSpec::getWorkflows)
                .flatMap(find -> find.stream().findFirst())
                .map(SpecWorkflow::getOwner)
                .orElseGet(() -> getOwner(spec))
            );
    }

    @Override
    public String getDescription(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getDescription)
            .orElseGet(() -> Optional.ofNullable(specification).map(Specification::getSpec)
                .map(DataWorksWorkflowSpec::getWorkflows)
                .flatMap(find -> find.stream().findFirst())
                .map(SpecWorkflow::getDescription)
                .orElse(null));
    }

    @Override
    public String getDataSourceName(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getDatasources)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .map(SpecDatasource::getName)
            .filter(StringUtils::isNotBlank)
            .findFirst()
            .orElse(null);
    }

    @Override
    public String getResourceGroupIdentifier(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getRuntimeResources)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .map(SpecRuntimeResource::getResourceGroup)
            .filter(StringUtils::isNotBlank)
            .findFirst()
            .orElse(null);
    }

    @Override
    public String getResourceGroupId(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getRuntimeResources)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .map(SpecRuntimeResource::getResourceGroupId)
            .filter(StringUtils::isNotBlank)
            .findFirst()
            .orElse(null);
    }

    @Override
    public void resetUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        if (uuidMap == null) {
            return;
        }
        String originUuid = Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(SpecRefEntity::getId)
            .orElse(null);
        AtomicReference<String> workflowUuid = new AtomicReference<>();
        workflowUuid.set(UuidUtils.genUuidWithoutHorizontalLine());
        if (StringUtils.isBlank(originUuid)) {
            uuidMap.put(workflowUuid.get(), workflowUuid.get());
        } else {
            workflowUuid.set(uuidMap.getOrDefault(originUuid, workflowUuid.get()));
            uuidMap.put(originUuid, workflowUuid.get());
        }

        // reset workflow id
        Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .ifPresent(spec -> spec.setId(workflowUuid.get()));

        // reset node id
        Optional.ofNullable(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getNodes)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .forEach(node -> {
                resetUuid4Entity(node, uuidMap);
            });
    }

    @Override
    public void replaceUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        super.replaceUuid(specification, uuidMap);

        // replace all value refer to id
        Optional.of(specification)
            .map(Specification::getSpec)
            .map(DataWorksWorkflowSpec::getFlow)
            .ifPresent(flows -> replaceIdInDependencies(flows, uuidMap));
        ListUtils.emptyIfNull(getInnerSpecifications(specification))
            .forEach(spec -> nodeSpecInfoHandler.replaceUuid(spec, uuidMap));
    }
}
