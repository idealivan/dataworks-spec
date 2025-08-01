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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import com.alibaba.fastjson2.JSONObject;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.PriorityWeightStrategy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author 聿剑
 * @date 2025/6/24
 */
@Slf4j
public class DataWorksWorkflowSpecTest {
    @Test
    public void testPriorityWeightStrategy() throws IOException {
        String spec = FileUtils.readFileToString(new File(DataWorksWorkflowSpecTest.class.getClassLoader().getResource(
                "spec/manual_workflow_spec_with_priority.json").getFile()),
            StandardCharsets.UTF_8);
        Specification<DataWorksWorkflowSpec> specification = SpecUtil.parseToDomain(spec);
        assertNotNull(specification);
        assertNotNull(specification.getSpec());
        assertNotNull(specification.getSpec().getStrategy());
        assertEquals(3, (int)specification.getSpec().getStrategy().getPriority());
        assertEquals(PriorityWeightStrategy.UPSTREAM, specification.getSpec().getStrategy().getPriorityWeightStrategy());
        assertEquals(100, (int)specification.getSpec().getStrategy().getMaxInternalConcurrency());

        String outSpec = SpecUtil.writeToSpec(specification);
        log.info("out spec: {}", outSpec);
        JSONObject obj = JSONObject.parseObject(outSpec);
        assertEquals(3, obj.getByPath("$.spec.strategy.priority"));
        assertEquals(PriorityWeightStrategy.UPSTREAM.getLabel(), obj.getByPath("$.spec.strategy.priorityWeightStrategy"));
        assertEquals(100, obj.getByPath("$.spec.strategy.maxInternalConcurrency"));
    }
}
