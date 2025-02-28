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


import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SpecFlowDependTest {

    private SpecFlowDepend specFlowDepend;
    private Map<String, String> replaceNodeIdMap;

    @Before
    public void setUp() {
        specFlowDepend = new SpecFlowDepend();
        replaceNodeIdMap = new HashMap<>();
    }

    @Test
    public void replaceNodeId_NodeIdIsNull_NoChange() {
        specFlowDepend.setNodeId(null);
        specFlowDepend.setDepends(new ArrayList<>());
        specFlowDepend.setVariableDepends(new ArrayList<>());

        specFlowDepend.replaceNodeId(replaceNodeIdMap);

        assertNull(specFlowDepend.getNodeId());
        assertEquals(0, specFlowDepend.getDepends().size());
        assertEquals(0, specFlowDepend.getVariableDepends().size());
    }

    @Test
    public void replaceNodeId_NodeIdNotInMap_NoChange() {
        SpecNode specNode = new SpecNode();
        specNode.setId("oldId");
        specFlowDepend.setNodeId(specNode);

        specFlowDepend.replaceNodeId(replaceNodeIdMap);

        assertEquals("oldId", specFlowDepend.getNodeId().getId());
    }

    @Test
    public void replaceNodeId_NodeIdInMap_NodeReplaced() {
        SpecNode specNode = new SpecNode();
        specNode.setId("oldId");
        specFlowDepend.setNodeId(specNode);

        replaceNodeIdMap.put("oldId", "newId");

        specFlowDepend.replaceNodeId(replaceNodeIdMap);

        assertEquals("newId", specFlowDepend.getNodeId().getId());
    }

    @Test
    public void replaceNodeId_DependsListEmpty_NoChange() {
        specFlowDepend.setNodeId(new SpecNode());
        specFlowDepend.setDepends(new ArrayList<>());

        specFlowDepend.replaceNodeId(replaceNodeIdMap);

        assertEquals(0, specFlowDepend.getDepends().size());
    }

    @Test
    public void replaceNodeId_VariableDependsListEmpty_NoChange() {
        specFlowDepend.setNodeId(new SpecNode());
        specFlowDepend.setVariableDepends(new ArrayList<>());

        specFlowDepend.replaceNodeId(replaceNodeIdMap);

        assertEquals(0, specFlowDepend.getVariableDepends().size());
    }
}
