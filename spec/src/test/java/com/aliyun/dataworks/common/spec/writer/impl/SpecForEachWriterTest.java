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

package com.aliyun.dataworks.common.spec.writer.impl;

import java.util.Arrays;
import java.util.Collections;

import com.alibaba.fastjson2.JSONObject;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecForEach;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.writer.SpecWriterContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for SpecForEachWriter
 *
 * @author Qwen Code
 */
public class SpecForEachWriterTest {

    @Test
    public void testWriteSpecForEach() {
        // Create a SpecForEach object
        SpecForEach specForEach = new SpecForEach();
        specForEach.setMaxIterations(20);
        specForEach.setParallelism(3);

        // Create mock nodes
        SpecNode node1 = new SpecNode();
        node1.setId("foreach-node1");
        node1.setName("foreach-test-node-1");

        SpecNode node2 = new SpecNode();
        node2.setId("foreach-node2");
        node2.setName("foreach-test-node-2");

        specForEach.setNodes(Arrays.asList(node1, node2));

        // Create mock array variable
        SpecVariable arrayVar = new SpecVariable();
        arrayVar.setName("test-array");
        arrayVar.setValue("[1, 2, 3, 4, 5]");
        specForEach.setArray(arrayVar);

        // Create mock flow dependency
        SpecFlowDepend flowDepend = new SpecFlowDepend();
        flowDepend.setNodeId(node1);
        specForEach.setFlow(Collections.singletonList(flowDepend));

        // Create writer context
        SpecWriterContext context = new SpecWriterContext();

        // Create writer
        SpecForEachWriter writer = new SpecForEachWriter(context);

        // Write the object
        JSONObject result = writer.write(specForEach, context);

        // Assertions
        assertNotNull(result);
        assertEquals(20, (int) result.getInteger("maxIterations"));
        assertEquals(3, (int) result.getInteger("parallelism"));
        assertNotNull(result.getJSONObject("array"));
        assertTrue(result.containsKey("nodes"));
        assertTrue(result.containsKey("flow"));
    }

    @Test
    public void testWriteSpecForEachWithNullValues() {
        // Create a SpecForEach object with null values
        SpecForEach specForEach = new SpecForEach();
        specForEach.setMaxIterations(null);
        specForEach.setParallelism(null);

        // Create writer context
        SpecWriterContext context = new SpecWriterContext();

        // Create writer
        SpecForEachWriter writer = new SpecForEachWriter(context);

        // Write the object
        JSONObject result = writer.write(specForEach, context);

        // Assertions
        assertNotNull(result);
        assertEquals(null, result.getInteger("maxIterations"));
        assertEquals(null, result.getInteger("parallelism"));
    }
}