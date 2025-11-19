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

package com.aliyun.dataworks.common.spec.domain.enums;

import com.aliyun.dataworks.common.spec.domain.interfaces.LabelEnum;

/**
 * @author 聿剑
 * @date 2025/05/20
 */
public enum SourceType implements LabelEnum {
    /**
     * 系统自动
     */
    SYSTEM("System"),

    /**
     * 手动添加
     */
    MANUAL("Manual"),

    /**
     * 代码解析
     */
    CODE_PARSE("CodeParse");

    private final String label;

    SourceType(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }
}