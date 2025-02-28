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

package com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.handler;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.adapter.SpecHandlerContext;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.noref.SpecSubFlow;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity.DwNodeEntity;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

/**
 * 组合节点Spec处理器
 *
 * @author 聿剑
 * @date 2024/02/24
 */
@Slf4j
public class CombinedNodeSpecHandler extends BasicNodeSpecHandler {
    @Override
    public boolean support(DwNodeEntity dwNode) {
        return matchNodeType(dwNode, CodeProgramType.COMBINED_NODE);
    }

    @Override
    public SpecNode handle(DwNodeEntity orcNode) {
        Preconditions.checkNotNull(orcNode, "node is null");
        SpecNode specNode = super.handle(orcNode);
        if (support(orcNode)) {
            List<DwNodeEntity> innerNodes = getInnerNodes(orcNode);
            specNode.setCombined(buildCombined(orcNode, specNode, innerNodes, context));
        }
        return specNode;
    }

    @Override
    public SpecNode handle(DwNodeEntity parentNode, List<DwNodeEntity> innerNodes) {
        Preconditions.checkNotNull(parentNode, "node is null");
        SpecNode specNode = super.handle(parentNode);
        if (support(parentNode)) {
            specNode.setCombined(buildCombined(parentNode, specNode, innerNodes, context));
        }
        return specNode;
    }

    private SpecSubFlow buildCombined(DwNodeEntity orcNode, SpecNode specNode, List<DwNodeEntity> innerNodes, SpecHandlerContext context) {
        SpecSubFlow combined = new SpecSubFlow();
        combined.setNodes(innerNodes.stream()
            .map(n -> getSpecAdapter().getHandler(n, context.getLocale()).handle(n))
            .collect(Collectors.toList()));
        combined.setDependencies(innerNodes.stream().map(node -> getSpecAdapter().toFlow(this, node, context)).flatMap(List::stream)
            .collect(Collectors.toList()));
        return combined;
    }
}
