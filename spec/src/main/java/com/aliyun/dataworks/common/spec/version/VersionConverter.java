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

import com.aliyun.dataworks.common.spec.domain.Spec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecVersion;

/**
 * Version Converter
 *
 * @author 聿剑
 * @date 2025/9/7
 */
public interface VersionConverter<T extends Spec> {
    /**
     * support  convert
     *
     * @param sourceVersion source version
     * @param targetVersion target version
     * @return true or false
     */
    boolean support(SpecVersion sourceVersion, SpecVersion targetVersion);

    /**
     * convert to target version
     *
     * @param sourceSpecification specification
     * @return specification
     */
    Specification<T> convert(Specification<T> sourceSpecification);
}
