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

package com.aliyun.dataworks.common.spec.parser.impl;

import java.util.Map;

import com.aliyun.dataworks.common.spec.annotation.SpecParser;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDataIntegrationJob;
import com.aliyun.dataworks.common.spec.parser.SpecParserContext;

/**
 * @author 世嘉
 * @date 2025/7/11
 */
@SpecParser
public class SpecDataIntegrationJobParser extends DefaultSpecParser<SpecDataIntegrationJob> {
    @Override
    public SpecDataIntegrationJob parse(Map<String, Object> rawContext, SpecParserContext specParserContext) {
        return super.parse(rawContext, specParserContext);
    }
}
