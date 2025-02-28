/*
 *
 *  * Copyright (c) 2025, Alibaba Cloud;
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.aliyun.dataworks.common.spec.domain.noref;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SpecVariableFlowDependTest {

    private SpecVariableFlowDepend specVariableFlowDepend;
    private SpecVariableDepend specVariableDepend;

    @Before
    public void setUp() {
        specVariableFlowDepend = new SpecVariableFlowDepend();
        specVariableDepend = new SpecVariableDepend();
        SpecNode specNode = new SpecNode();
        specNode.setId("oldId");
        specVariableDepend.setNodeId(specNode);
        List<SpecVariableDepend> depends = new ArrayList<>();
        depends.add(specVariableDepend);
        specVariableFlowDepend.setDepends(depends);
    }

    @Test
    public void replaceNodeId_EmptyDependsList_NoExceptionThrown() {
        Map<String, String> replaceNodeIdMap = new HashMap<>();
        specVariableFlowDepend.replaceNodeId(replaceNodeIdMap);
        Assert.assertEquals("oldId", specVariableDepend.getNodeId().getId());
    }

    @Test
    public void replaceNodeId_NonEmptyDependsList_ReplaceNodeIdCalled() {
        Map<String, String> replaceNodeIdMap = new HashMap<>();
        replaceNodeIdMap.put("oldId", "newId");
        specVariableFlowDepend.replaceNodeId(replaceNodeIdMap);
        Assert.assertEquals("newId", specVariableDepend.getNodeId().getId());
    }

    @Test
    public void replaceNodeId_NullDependsList_NoExceptionThrown() {
        specVariableFlowDepend.setDepends(null);
        Map<String, String> replaceNodeIdMap = new HashMap<>();
        specVariableFlowDepend.replaceNodeId(replaceNodeIdMap);
        Assert.assertNull(specVariableFlowDepend.getDepends());
    }
}
