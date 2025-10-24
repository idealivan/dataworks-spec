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

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecVersion;
import com.aliyun.dataworks.common.spec.domain.interfaces.LabelEnum;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.SetUtils;
import org.reflections.Reflections;

/**
 * @author 聿剑
 * @date 2025/9/7
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class VersionConverterFactory {
    private static final Set<Class<? extends AbstractVersionConverter>> converters = new CopyOnWriteArraySet<>();

    private synchronized static void scanVersionConverterByReflections() {
        Reflections reflections = new Reflections(VersionConverter.class.getPackage().getName());
        Set<Class<? extends AbstractVersionConverter>> clzSet = reflections.getSubTypesOf(AbstractVersionConverter.class);
        SetUtils.emptyIfNull(clzSet).stream()
            .filter(AbstractVersionConverter.class::isAssignableFrom)
            .forEach(converters::add);
    }

    private static AbstractVersionConverter getConverter(SpecVersion sourceVersion, SpecVersion targetVersion) {
        if (CollectionUtils.isEmpty(converters)) {
            scanVersionConverterByReflections();
        }

        return SetUtils.emptyIfNull(converters).stream()
            .map(clz -> {
                try {
                    return clz.getConstructor().newInstance();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).filter(c -> c.support(sourceVersion, targetVersion))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("No converter found"));
    }

    public static Specification<DataWorksWorkflowSpec> convert(Specification<DataWorksWorkflowSpec> specification, SpecVersion targetVersion) {
        SpecVersion sourceVersion = LabelEnum.getByLabel(SpecVersion.class, specification.getVersion());
        AbstractVersionConverter converter = getConverter(sourceVersion, targetVersion);
        return (Specification<DataWorksWorkflowSpec>)converter.convert(specification);
    }
}
