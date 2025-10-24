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

import java.util.Optional;

import com.alibaba.fastjson2.JSONObject;

import com.aliyun.dataworks.common.spec.annotation.SpecWriter;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.enums.SpecVersion;
import com.aliyun.dataworks.common.spec.writer.SpecWriterContext;

/**
 * DataWorksWorkflowSpec writer
 *
 * @author 聿剑
 * @date 2023/8/27
 */
@SpecWriter
public class DataWorksWorkflowSpecV2Writer extends DefaultJsonObjectWriter<DataWorksWorkflowSpec> {
    public DataWorksWorkflowSpecV2Writer(SpecWriterContext context) {
        super(context);
    }

    @Override
    public boolean support(SpecVersion version) {
        return Optional.ofNullable(version).map(v -> SpecVersion.V_2_0_0.getMajor().equals(v.getMajor())).orElse(false);
    }

    @Override
    public JSONObject write(DataWorksWorkflowSpec specObj, SpecWriterContext context) {
        DataWorksWorkflowSpecWriter writer = new DataWorksWorkflowSpecWriter(context);
        JSONObject jsonObject = writer.write(specObj, context);
        if (jsonObject.containsKey("flow")) {
            jsonObject.put("dependencies", jsonObject.getJSONArray("flow"));
            jsonObject.remove("flow");
        }
        return jsonObject;
    }
}