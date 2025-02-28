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

package com.aliyun.dataworks.common.spec.parser.impl;

import java.util.Map;

import com.aliyun.dataworks.common.spec.annotation.SpecParser;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecVariableDepend;
import com.aliyun.dataworks.common.spec.parser.SpecParserContext;
import com.aliyun.dataworks.common.spec.utils.SpecDevUtil;

/**
 * @author 戒迷
 * @date 2025/2/13
 */
@SpecParser
public class SpecVariableDependParser extends DefaultSpecParser<SpecVariableDepend> {
    @Override
    public SpecVariableDepend parse(Map<String, Object> rawContext, SpecParserContext specParserContext) {

        SpecDepend specDepend = (SpecDepend)SpecDevUtil.getObjectByParser(SpecDepend.class, rawContext, specParserContext);

        SpecVariableDepend specVariableDepend = new SpecVariableDepend();
        SpecDevUtil.setSameKeyField(rawContext, specVariableDepend, specParserContext);

        specVariableDepend.setNodeId(specDepend.getNodeId());
        specVariableDepend.setOutput(specDepend.getOutput());
        specVariableDepend.setType(specDepend.getType());
        specVariableDepend.setContext(specDepend.getContext());
        specVariableDepend.setMetadata(specDepend.getMetadata());
        return specVariableDepend;
    }
}
