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
import com.aliyun.dataworks.common.spec.domain.noref.SpecPaiflow;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.writer.SpecWriterContext;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Unit test for SpecPaiflowWriter
 *
 * @author Qwen Code
 */
public class SpecPaiflowWriterTest {

    @Test
    public void testWriteSpecPaiflow() {
        // Create a SpecPaiflow object
        SpecPaiflow specPaiflow = new SpecPaiflow();

        // Create mock nodes
        SpecNode node1 = new SpecNode();
        node1.setId("paiflow-node1");
        node1.setName("paiflow-test-node-1");

        SpecNode node2 = new SpecNode();
        node2.setId("paiflow-node2");
        node2.setName("paiflow-test-node-2");

        specPaiflow.setNodes(Arrays.asList(node1, node2));

        // Create mock flow dependency
        SpecFlowDepend flowDepend = new SpecFlowDepend();
        flowDepend.setNodeId(node1);
        specPaiflow.setFlow(Collections.singletonList(flowDepend));

        // Create writer context
        SpecWriterContext context = new SpecWriterContext();

        // Create writer
        SpecPaiflowWriter writer = new SpecPaiflowWriter(context);

        // Write the object
        JSONObject result = writer.write(specPaiflow, context);

        // Assertions
        assertNotNull(result);
    }

    @Test
    public void testWriteSpecPaiflowWithEmptyValues() {
        // Create a SpecPaiflow object with empty values
        SpecPaiflow specPaiflow = new SpecPaiflow();
        specPaiflow.setNodes(Collections.emptyList());
        specPaiflow.setFlow(Collections.emptyList());

        // Create writer context
        SpecWriterContext context = new SpecWriterContext();

        // Create writer
        SpecPaiflowWriter writer = new SpecPaiflowWriter(context);

        // Write the object
        JSONObject result = writer.write(specPaiflow, context);

        // Assertions
        assertNotNull(result);
    }
}