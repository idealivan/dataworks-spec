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

import com.aliyun.dataworks.common.spec.domain.enums.SpecVersion;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDoWhile;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.writer.SpecWriterContext;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for SpecDoWhileWriter
 *
 * @author Qwen Code
 */
public class SpecDoWhileWriterTest {

    @Test
    public void testWriteSpecDoWhile() {
        // Create a SpecDoWhile object
        SpecDoWhile specDoWhile = new SpecDoWhile();
        specDoWhile.setMaxIterations(10);
        specDoWhile.setParallelism(5);

        // Create mock nodes
        SpecNode node1 = new SpecNode();
        node1.setId("node1");
        node1.setName("test-node-1");

        SpecNode node2 = new SpecNode();
        node2.setId("node2");
        node2.setName("test-node-2");

        specDoWhile.setNodes(Arrays.asList(node1, node2));

        // Create mock while node
        SpecNode whileNode = new SpecNode();
        whileNode.setId("while-node");
        whileNode.setName("while-condition");
        specDoWhile.setSpecWhile(whileNode);

        // Create mock flow dependency
        SpecFlowDepend flowDepend = new SpecFlowDepend();
        flowDepend.setNodeId(node1);
        specDoWhile.setFlow(Collections.singletonList(flowDepend));

        // Create writer context
        SpecWriterContext context = new SpecWriterContext();
        context.setVersion(SpecVersion.V_2_0_0.getLabel());
        // Create writer
        SpecDoWhileWriter writer = new SpecDoWhileWriter(context);

        // Write the object
        JSONObject result = writer.write(specDoWhile, context);

        // Assertions
        assertNotNull(result);
        assertEquals(10, (int)result.getInteger("maxIterations"));
        assertEquals(5, (int)result.getInteger("parallelism"));
        assertNotNull(result.getJSONObject("while"));
        assertTrue(result.containsKey("nodes"));
        assertTrue(result.containsKey("dependencies"));
    }

    @Test
    public void testWriteSpecDoWhileWithNullValues() {
        // Create a SpecDoWhile object with null values
        SpecDoWhile specDoWhile = new SpecDoWhile();
        specDoWhile.setMaxIterations(null);
        specDoWhile.setParallelism(null);

        // Create writer context
        SpecWriterContext context = new SpecWriterContext();

        // Create writer
        SpecDoWhileWriter writer = new SpecDoWhileWriter(context);

        // Write the object
        JSONObject result = writer.write(specDoWhile, context);

        // Assertions
        assertNotNull(result);
        Assert.assertNull(result.getInteger("maxIterations"));
        Assert.assertNull(result.getInteger("parallelism"));
    }
}