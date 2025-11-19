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

package com.aliyun.dataworks.common.spec.version;

import java.io.IOException;
import java.io.InputStream;

import com.alibaba.fastjson2.JSONObject;

import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.enums.SpecVersion;
import com.aliyun.dataworks.common.spec.writer.SpecWriterContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * @author 聿剑
 * @date 2025/9/7
 */
public class VersionConverterFactoryTest {
    private String loadSpecFromResources(String fileName) throws IOException {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(fileName)) {
            if (is == null) {
                throw new IOException("Resource not found: " + fileName);
            }
            return new String(is.readAllBytes());
        }
    }

    @Test
    public void testConvert() throws IOException {
        String specJson = loadSpecFromResources("version/1.x.y/single_cycle_node.json");
        Specification<DataWorksWorkflowSpec> spec = SpecUtil.parseToDomain(specJson);
        Specification<DataWorksWorkflowSpec> newSpec = VersionConverterFactory.convert(spec, SpecVersion.V_2_0_0);

        assertNotNull(newSpec);
        assertEquals(SpecVersion.V_2_0_0.getLabel(), newSpec.getVersion());
        assertEquals(SpecKind.NODE.getLabel(), newSpec.getKind());
        assertNotNull(newSpec.getSpec());
        assertNotNull(newSpec.getSpec().getNodes());
        assertEquals(1, newSpec.getSpec().getNodes().size());
        assertNotNull(newSpec.getSpec().getDependencies());
        SpecWriterContext ctx = new SpecWriterContext();
        ctx.setVersion(newSpec.getVersion());
        JSONObject newSpecJson = (JSONObject)SpecUtil.write(newSpec, ctx);
        assertNull(newSpecJson.getByPath("$.spec.flow"));
        assertNotNull(newSpecJson.getByPath("$.spec.dependencies"));
    }
}
