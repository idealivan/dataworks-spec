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

package com.aliyun.dataworks.common.spec.domain.noref;

import java.util.List;
import java.util.Map;

import com.aliyun.dataworks.common.spec.domain.SpecNoRefEntity;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections4.ListUtils;

/**
 * @author yiwei.qyw
 * @date 2023/7/4
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SpecFlowDepend extends SpecNoRefEntity {
    @EqualsAndHashCode.Include
    private SpecNode nodeId;
    @EqualsAndHashCode.Include
    private List<SpecDepend> depends;
    @EqualsAndHashCode.Include
    private List<SpecVariableFlowDepend> variableDepends;

    /**
     * replace nodeId by replaceNodeIdMap
     *
     * @param replaceNodeIdMap replaceNodeIdMap
     */
    public void replaceNodeId(Map<String, String> replaceNodeIdMap) {
        if (null != nodeId && replaceNodeIdMap.containsKey(nodeId.getId())) {
            nodeId.setId(replaceNodeIdMap.get(nodeId.getId()));
        }

        ListUtils.emptyIfNull(depends).forEach(depend -> depend.replaceNodeId(replaceNodeIdMap));
        ListUtils.emptyIfNull(variableDepends).forEach(depend -> depend.replaceNodeId(replaceNodeIdMap));
    }
}