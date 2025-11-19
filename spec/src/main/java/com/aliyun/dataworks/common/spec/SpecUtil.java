/*
 * Copyright (c) 2024, Alibaba Cloud;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.dataworks.common.spec;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Spec;
import com.aliyun.dataworks.common.spec.domain.SpecEntity;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.FlowType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.interfaces.LabelEnum;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFileResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFunction;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.exception.SpecErrorCode;
import com.aliyun.dataworks.common.spec.exception.SpecException;
import com.aliyun.dataworks.common.spec.parser.Parser;
import com.aliyun.dataworks.common.spec.parser.SpecParserContext;
import com.aliyun.dataworks.common.spec.parser.SpecParserFactory;
import com.aliyun.dataworks.common.spec.parser.ToDomainRootParser;
import com.aliyun.dataworks.common.spec.utils.ParserUtil;
import com.aliyun.dataworks.common.spec.utils.SpecDevUtil;
import com.aliyun.dataworks.common.spec.writer.SpecWriterContext;
import com.aliyun.dataworks.common.spec.writer.WriterFactory;
import com.aliyun.dataworks.common.spec.writer.impl.SpecificationWriter;
import com.google.common.base.Preconditions;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author yiwei.qyw
 * @date 2023/7/6
 */
public class SpecUtil {

    private SpecUtil() {

    }

    /**
     * Parse json to spec Domain object
     *
     * @param spec Json string
     * @return spec Domain object
     */
    @SuppressWarnings("unchecked")
    public static <T extends Spec> Specification<T> parseToDomain(String spec) {
        if (spec == null) {
            return null;
        }
        return (Specification<T>)new ToDomainRootParser().parseToDomain(spec);
    }

    public static <T extends Spec> String writeToSpec(Specification<T> specification) {
        if (specification == null) {
            return null;
        }

        Preconditions.checkNotNull(specification.getVersion(), "version is null");
        Preconditions.checkNotNull(specification.getSpec(), "spec is null");
        Preconditions.checkNotNull(specification.getKind(), "kind is null");

        SpecWriterContext context = new SpecWriterContext();
        context.setVersion(specification.getVersion());
        SpecificationWriter writer = (SpecificationWriter)WriterFactory.getWriter(specification.getClass(), context);
        if (writer == null) {
            throw new SpecException(SpecErrorCode.PARSER_LOAD_ERROR, "no available registered writer found for type: " + specification.getClass());
        }
        return JSON.toJSONString(writer.write(specification, context), Feature.PrettyFormat, Feature.WriteEnumsUsingName, Feature.LargeObject);
    }

    @SuppressWarnings("unchecked")
    public static <T> Object write(T specObject, SpecWriterContext context) {
        if (specObject == null) {
            return null;
        }

        return Optional.ofNullable(WriterFactory.getWriter(specObject.getClass(), context))
            .map(writer -> writer.write(specObject, context))
            .orElse(SpecDevUtil.writeJsonObject(specObject, false));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <T extends SpecEntity> T parse(String json, Class specCls, SpecParserContext context) {
        Parser<T> parser = (Parser<T>)SpecParserFactory.getParser(specCls.getSimpleName());
        Preconditions.checkNotNull(parser, specCls.getSimpleName() + " parser not found");
        return parser.parse(ParserUtil.jsonToMap(JSON.parseObject(json)), context);
    }

    public static SpecRefEntity getMatchIdSpecRefEntity(Specification<DataWorksWorkflowSpec> specification) {
        return getMatchIdSpecRefEntity(specification, null);
    }

    /**
     * Get spec subject entity by uuid
     *
     * @param specification spec
     * @return spec subject entity
     */
    public static SpecRefEntity getMatchIdSpecRefEntity(Specification<DataWorksWorkflowSpec> specification,
        Consumer<List<SpecFlowDepend>> dependenciesConsumer) {
        String uuid = Optional.ofNullable(specification).map(SpecEntity::getMetadata).map(m -> m.get("uuid")).map(String::valueOf).orElse(null);
        if (StringUtils.isBlank(uuid)) {
            return getDefaultSpecEntity(specification, dependenciesConsumer);
        }

        DataWorksWorkflowSpec spec = specification.getSpec();

        for (SpecWorkflow workflow : ListUtils.emptyIfNull(spec.getWorkflows())) {
            if (uuid.equals(workflow.getId())) {
                consumeDependencies(dependenciesConsumer, spec.getFlow());
                return workflow;
            }

            for (SpecNode node : ListUtils.emptyIfNull(workflow.getNodes())) {
                if (uuid.equals(node.getId())) {
                    consumeDependencies(dependenciesConsumer, workflow.getDependencies());
                    return node;
                }

                for (SpecNode innerNode : ListUtils.emptyIfNull(node.getInnerNodes())) {
                    if (uuid.equals(innerNode.getId())) {
                        consumeDependencies(dependenciesConsumer, node.getInnerDependencies());
                        return innerNode;
                    }
                }
            }
        }

        for (SpecNode node : ListUtils.emptyIfNull(spec.getNodes())) {
            if (uuid.equals(node.getId())) {
                consumeDependencies(dependenciesConsumer, spec.getDependencies());
                return node;
            }

            for (SpecNode innerNode : ListUtils.emptyIfNull(node.getInnerNodes())) {
                if (uuid.equals(innerNode.getId())) {
                    consumeDependencies(dependenciesConsumer, node.getInnerDependencies());
                    return innerNode;
                }
            }
        }

        for (SpecFileResource fileResource : ListUtils.emptyIfNull(spec.getFileResources())) {
            if (uuid.equals(fileResource.getId())) {
                return fileResource;
            }
        }

        for (SpecFunction function : ListUtils.emptyIfNull(spec.getFunctions())) {
            if (uuid.equals(function.getId())) {
                return function;
            }
        }

        if (Optional.ofNullable(specification.getKind()).map(kind -> SpecKind.MANUAL_WORKFLOW.getLabel().equalsIgnoreCase(kind))
            .orElse(false)) {
            SpecWorkflow workflow = new SpecWorkflow();
            workflow.setName(spec.getName());
            workflow.setId(spec.getId());
            workflow.setType(FlowType.MANUAL_WORKFLOW.getLabel());
            workflow.setDescription(spec.getDescription());
            workflow.setOwner(spec.getOwner());
            workflow.setNodes(spec.getNodes());
            workflow.setDependencies(spec.getFlow());
            return workflow;
        }

        return null;
    }

    private static SpecRefEntity getDefaultSpecEntity(Specification<DataWorksWorkflowSpec> specification,
        Consumer<List<SpecFlowDepend>> specFlowDependConsumer) {
        SpecKind specKind = Optional.ofNullable(specification)
            .map(Specification::getKind)
            .map(kind -> LabelEnum.getByLabel(SpecKind.class, kind))
            .orElse(null);
        DataWorksWorkflowSpec spec = Optional.ofNullable(specification).map(Specification::getSpec).orElse(null);
        if (specKind == null || spec == null) {
            return null;
        }

        switch (specKind) {
            case RESOURCE:
                return ListUtils.emptyIfNull(spec.getFileResources()).stream().findFirst().orElse(null);
            case FUNCTION:
                return ListUtils.emptyIfNull(spec.getFunctions()).stream().findFirst().orElse(null);
            case COMPONENT:
                return ListUtils.emptyIfNull(spec.getComponents()).stream().findFirst().orElse(null);
        }

        List<SpecRefEntity> specRefEntityList = new ArrayList<>();
        // 默认返回第一个
        ListUtils.emptyIfNull(spec.getWorkflows()).stream().filter(Objects::nonNull).findFirst().ifPresent(w -> {
            specRefEntityList.add(w);
            consumeDependencies(specFlowDependConsumer, spec.getDependencies());
        });
        ListUtils.emptyIfNull(spec.getNodes()).stream().filter(Objects::nonNull).findFirst().ifPresent(n -> {
            specRefEntityList.add(n);
            consumeDependencies(specFlowDependConsumer, spec.getFlow());
        });
        // fileResource, function, component的spec，类型也可能是CycleWorkflow，此处兼容下
        ListUtils.emptyIfNull(spec.getFileResources()).stream().filter(Objects::nonNull).findFirst().ifPresent(specRefEntityList::add);
        ListUtils.emptyIfNull(spec.getFunctions()).stream().filter(Objects::nonNull).findFirst().ifPresent(specRefEntityList::add);
        ListUtils.emptyIfNull(spec.getComponents()).stream().filter(Objects::nonNull).findFirst().ifPresent(specRefEntityList::add);
        return specRefEntityList.stream().findFirst().orElse(null);
    }

    private static void consumeDependencies(Consumer<List<SpecFlowDepend>> specFlowDependConsumer, List<SpecFlowDepend> flowDepends) {
        if (specFlowDependConsumer != null) {
            specFlowDependConsumer.accept(flowDepends);
        }
    }
}