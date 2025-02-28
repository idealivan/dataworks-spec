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

package com.aliyun.dataworks.common.spec.writer.impl;

import java.util.Optional;

import com.alibaba.fastjson2.JSONObject;

import com.aliyun.dataworks.common.spec.annotation.SpecWriter;
import com.aliyun.dataworks.common.spec.domain.noref.SpecVariableDepend;
import com.aliyun.dataworks.common.spec.writer.SpecWriterContext;
import com.aliyun.dataworks.common.spec.writer.WriterFactory;

/**
 * @author 戒迷
 * @date 2025/2/11
 */
@SpecWriter
public class SpecVariableDependWriter extends DefaultJsonObjectWriter<SpecVariableDepend> {
    public SpecVariableDependWriter(SpecWriterContext context) {
        super(context);
    }

    @Override
    public JSONObject write(SpecVariableDepend specObj, SpecWriterContext context) {
        SpecDependWriter writer = (SpecDependWriter)WriterFactory.getWriter(SpecDependWriter.class, context);
        JSONObject json = writer.write(specObj, context);
        Optional.ofNullable(specObj.getVariableName()).ifPresent(variableName -> json.put("variableName", variableName));
        return json;
    }
}
