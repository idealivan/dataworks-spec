package com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Spec;
import com.aliyun.dataworks.common.spec.domain.SpecEntity;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.interfaces.LabelEnum;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.Container;
import com.aliyun.dataworks.common.spec.domain.ref.InputOutputWired;
import com.aliyun.dataworks.common.spec.domain.ref.ScriptWired;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.common.spec.utils.UuidUtils;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-08-20
 */
@Slf4j
public abstract class BaseSpecInfoHandler<T> implements SpecExtractInfoHandler {

    protected static final String UUID = "uuid";

    protected static final String CONTAINER_ID = "containerId";

    protected static final String OWNER = "owner";

    protected static final String NULL_KEY = "null";

    /**
     * @param specification specification
     * @return domain
     */
    protected abstract T get(Specification<DataWorksWorkflowSpec> specification);

    @Override
    public List<Specification<DataWorksWorkflowSpec>> getInnerSpecifications(Specification<DataWorksWorkflowSpec> specification) {
        return Lists.newArrayList();
    }

    public SpecRefEntity getSpecRefEntity(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .filter(SpecRefEntity.class::isInstance)
            .map(SpecRefEntity.class::cast)
            .orElse(null);
    }

    @Override
    public String getSpecId(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification).map(SpecEntity::getMetadata)
            .map(this::getSpecIdFromMetadata)
            .orElseGet(() -> Optional.ofNullable(getSpecRefEntity(specification))
                .map(SpecRefEntity::getId)
                .filter(BaseSpecInfoHandler::checkUuidValid)
                .orElse(null)
            );
    }

    @Override
    public String getSpecCommand(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(getSpecScriptRuntime(specification))
            .map(SpecScriptRuntime::getCommand)
            .orElse(null);
    }

    @Override
    public Integer getSpecCommandTypeId(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(getSpecScriptRuntime(specification))
            .map(SpecScriptRuntime::getCommandTypeId)
            .orElseGet(() -> {
                try {
                    return CodeProgramType.of(getSpecCommand(specification)).getCode();
                } catch (Exception e) {
                    return null;
                }
            });
    }

    @Override
    public String getSpecPath(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(getSpecScript(specification))
            .map(SpecScript::getPath)
            .orElse(null);
    }

    @Override
    public String getSpecLanguage(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(getSpecScript(specification))
            .map(SpecScript::getLanguage)
            .orElse(null);
    }

    @Override
    public String getScriptContent(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(getSpecScript(specification))
            .map(SpecScript::getContent)
            .orElse(null);
    }

    @Override
    public SpecScript getSpecScript(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(getScriptWired(specification))
            .map(ScriptWired::getScript)
            .orElse(null);
    }

    @Override
    public void replaceUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        Optional.ofNullable(specification)
            .map(SpecEntity::getMetadata)
            .filter(map -> map.containsKey(UUID))
            .ifPresent(map -> {
                String originUuid = (String)map.get(UUID);
                map.put(UUID, getMappingId(uuidMap, originUuid));
            });
        Optional.ofNullable(specification)
            .map(SpecEntity::getMetadata)
            .filter(map -> map.containsKey(CONTAINER_ID))
            .ifPresent(map -> {
                String originUuid = (String)map.get(CONTAINER_ID);
                map.put(CONTAINER_ID, getMappingId(uuidMap, originUuid));
            });
    }

    protected String getSpecIdFromMetadata(Map<String, Object> metadata) {
        return Optional.ofNullable(metadata)
            .map(m -> m.get(UUID))
            .filter(String.class::isInstance)
            .map(String.class::cast)
            .filter(BaseSpecInfoHandler::checkUuidValid)
            .orElse(null);
    }

    protected SpecScriptRuntime getSpecScriptRuntime(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(getSpecScript(specification))
            .map(SpecScript::getRuntime)
            .orElse(null);
    }

    protected ScriptWired getScriptWired(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(get(specification))
            .filter(ScriptWired.class::isInstance)
            .map(ScriptWired.class::cast)
            .orElse(null);
    }

    protected SpecKind getSpecKind(Specification<DataWorksWorkflowSpec> specification) {
        return Optional.ofNullable(specification).map(Specification::getKind)
            .map(kind -> LabelEnum.getByLabel(SpecKind.class, kind))
            .orElse(null);
    }

    protected String getOwner(SpecEntity specEntity) {
        return Optional.ofNullable(specEntity)
            .map(SpecEntity::getMetadata)
            .map(metadata -> metadata.get(OWNER))
            .filter(String.class::isInstance)
            .map(String.class::cast)
            .orElse(null);
    }

    protected List<Specification<DataWorksWorkflowSpec>> splitSpecNodeInContainer(Specification<DataWorksWorkflowSpec> specification,
                                                                                  Container container, String containerId) {
        if (container == null) {
            return null;
        }
        log.info("splitSpecNodeInContainer, containerId: {}, container: {}", containerId, container);
        Map<String, SpecFlowDepend> nodeDependMap = ListUtils.emptyIfNull(container.getInnerDependencies()).stream().collect(
            Collectors.toMap(fd -> Optional.ofNullable(fd.getNodeId()).map(SpecNode::getId).orElse(NULL_KEY), Function.identity(),
                (d1, d2) -> {
                    Set<SpecDepend> set = new HashSet<>();
                    set.addAll(ListUtils.emptyIfNull(d1.getDepends()));
                    set.addAll(ListUtils.emptyIfNull(d2.getDepends()));
                    d1.setDepends(new ArrayList<>(set));
                    return d1;
                }));
        log.info("splitSpecNodeInContainer, nodeDependMap: {}", nodeDependMap);
        return ListUtils.emptyIfNull(container.getInnerNodes()).stream()
            .map(node -> {
                Specification<DataWorksWorkflowSpec> templateSpecification = buildTemplateSpecification(specification);
                Map<String, Object> metaData = new HashMap<>();
                metaData.put(UUID, node.getId());
                metaData.put(CONTAINER_ID, containerId);
                templateSpecification.setMetadata(metaData);
                templateSpecification.setKind(SpecKind.NODE.getLabel());
                DataWorksWorkflowSpec tempSpec = new DataWorksWorkflowSpec();
                tempSpec.setNodes(Collections.singletonList(node));
                tempSpec.setFlow(Optional.ofNullable(nodeDependMap.get(StringUtils.defaultString(node.getId(), NULL_KEY)))
                    .map(Collections::singletonList)
                    .orElse(null));
                templateSpecification.setSpec(tempSpec);
                return templateSpecification;
            }).collect(Collectors.toList());
    }

    /**
     * Build a template specification, which means the specification.spec is null and others is filled with param
     *
     * @param specification specification
     * @return template specification
     */
    public <S extends Spec> Specification<S> buildTemplateSpecification(Specification<S> specification) {
        Specification<S> templateSpecification = new Specification<>();
        Optional.ofNullable(specification).ifPresent(s -> {
            templateSpecification.setContext(s.getContext());
            templateSpecification.setKind(s.getKind());
            templateSpecification.setVersion(s.getVersion());
        });
        return templateSpecification;
    }

    public static boolean checkUuidValid(String uuid) {
        if (StringUtils.isNumeric(uuid)) {
            try {
                long l = Long.parseLong(uuid);
                return l > 0;
            } catch (Exception ignore) {
            }
        }
        return false;
    }

    /**
     * reset uuid for ref entity and put id mapping in uuid uuidMap
     *
     * @param specRefEntity specRefEntity
     * @param uuidMap       uuid uuidMap
     */
    protected void resetUuid4Entity(SpecRefEntity specRefEntity, Map<String, String> uuidMap) {
        if (uuidMap == null || specRefEntity == null) {
            return;
        }
        String originUuid = specRefEntity.getId();
        String newUuid = UuidUtils.genUuidWithoutHorizontalLine();
        /*
        If the original uuid is not empty, try to find a mapped uuid. If found, use the existing one; otherwise, use the randomly generated one.
        However, in any case, multiple level mappings are not allowed. After a mapping exists, the value must be self-mapped
         */
        if (StringUtils.isNotBlank(originUuid)) {
            newUuid = uuidMap.getOrDefault(originUuid, newUuid);
            uuidMap.put(originUuid, newUuid);
        }
        // avoid multi level mapping
        uuidMap.put(newUuid, newUuid);
        specRefEntity.setId(newUuid);
    }

    /**
     * reset input and output value after reset uuid
     *
     * @param inputOutputWired input output wired
     * @param uuidMap          uuid uuidMap
     */
    protected void replaceIdInInputOutput(InputOutputWired inputOutputWired, Map<String, String> uuidMap) {
        ListUtils.emptyIfNull(inputOutputWired.getInputs()).stream()
            .filter(SpecVariable.class::isInstance)
            .map(SpecVariable.class::cast)
            .forEach(variable -> replaceUuidInVariable(variable, uuidMap));

        ListUtils.emptyIfNull(inputOutputWired.getOutputs()).stream()
            .filter(SpecVariable.class::isInstance)
            .map(SpecVariable.class::cast)
            .forEach(variable -> replaceUuidInVariable(variable, uuidMap));
    }

    /**
     * reset id in script wired after reset uuid
     *
     * @param scriptWired script wired
     * @param uuidMap     uuidMap
     */
    protected void replaceIdInScriptWired(ScriptWired scriptWired, Map<String, String> uuidMap) {
        Optional.ofNullable(scriptWired)
            .map(ScriptWired::getScript)
            .map(SpecScript::getParameters)
            .orElse(Collections.emptyList()).stream()
            .filter(Objects::nonNull)
            .forEach(parameter -> replaceUuidInVariable(parameter, uuidMap));
    }

    private void replaceUuidInVariable(SpecVariable specVariable, Map<String, String> uuidMap) {
        Optional.ofNullable(specVariable)
            .map(SpecVariable::getNode)
            .map(SpecDepend::getNodeId)
            .ifPresent(node -> node.setId(getMappingId(uuidMap, node.getId())));

        Optional.ofNullable(specVariable)
            .map(SpecVariable::getReferenceVariable)
            .ifPresent(variable -> replaceUuidInVariable(variable, uuidMap));
    }

    /**
     * reset uuid reference in dependencies after reset uuid
     *
     * @param specFlowDepends spec flow depend
     * @param uuidMap         uuid uuidMap
     */
    protected void replaceIdInDependencies(List<SpecFlowDepend> specFlowDepends, Map<String, String> uuidMap) {
        ListUtils.emptyIfNull(specFlowDepends).stream()
            .filter(Objects::nonNull)
            .forEach(flowDepend -> flowDepend.replaceNodeId(uuidMap));
    }

    /**
     * Get mapping id from uuid uuidMap
     *
     * @param uuidMap uuid uuidMap
     * @param id      origin id
     * @return mapping id
     */
    protected String getMappingId(Map<String, String> uuidMap, String id) {
        if (StringUtils.isBlank(id)) {
            return null;
        }
        return MapUtils.emptyIfNull(uuidMap).getOrDefault(id, id);
    }
}
