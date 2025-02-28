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

import java.util.HashMap;
import java.util.Map;

import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SpecDependTest {

    private SpecDepend specDepend;
    private Map<String, String> replaceNodeIdMap;

    @Before
    public void setUp() {
        specDepend = new SpecDepend();
        replaceNodeIdMap = new HashMap<>();
    }

    @Test
    public void replaceNodeId_NodeIdIsNull_NoChange() {
        specDepend.replaceNodeId(replaceNodeIdMap);
        assertNull(specDepend.getNodeId());
    }

    @Test
    public void replaceNodeId_NodeIdNotInMap_NoChange() {
        specDepend.setNodeId(new SpecNode());
        specDepend.getNodeId().setId("oldId");
        specDepend.replaceNodeId(replaceNodeIdMap);
        assertEquals("oldId", specDepend.getNodeId().getId());
    }

    @Test
    public void replaceNodeId_NodeIdInMap_IdReplaced() {
        specDepend.setNodeId(new SpecNode());
        specDepend.getNodeId().setId("oldId");
        replaceNodeIdMap.put("oldId", "newId");
        specDepend.replaceNodeId(replaceNodeIdMap);
        assertEquals("newId", specDepend.getNodeId().getId());
    }
}
