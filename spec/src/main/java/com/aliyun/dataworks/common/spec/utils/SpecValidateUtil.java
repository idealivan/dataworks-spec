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

package com.aliyun.dataworks.common.spec.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecRuntimeResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.exception.SpecErrorCode;
import com.aliyun.dataworks.common.spec.exception.SpecException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.networknt.schema.InputFormat;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.PathType;
import com.networknt.schema.SchemaLocation;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion.VersionFlag;
import com.networknt.schema.ValidationMessage;
import com.networknt.schema.serialization.DefaultJsonNodeReader;
import com.networknt.schema.serialization.JsonNodeReader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Spec校验器
 *
 * @author 宇洛
 * @date 2023/12/21
 */
public class SpecValidateUtil {

    private static volatile JsonSchemaFactory jsonSchemaFactory;

    private static final String ERROR_MESSAGE_KEY_WORD = "message";

    private static final String COMMON_MAIN_SCHEMA_PATH = "classpath:spec/schema/Specification.schema.json";

    private SpecValidateUtil() {

    }

    void validateDataworksWorkflowSpec(String specCode) {
        if (StringUtils.isBlank(specCode)) {
            throw new IllegalArgumentException("specCode is null");
        }
        Specification<DataWorksWorkflowSpec> spec = SpecUtil.parseToDomain(specCode);
        if (spec == null || spec.getSpec() == null) {
            throw new IllegalArgumentException("spec is null");
        }

        if (CollectionUtils.isEmpty(spec.getSpec().getNodes())) {
            throw new IllegalArgumentException("spec nodes is empty");
        }

        for (SpecNode node : spec.getSpec().getNodes()) {
            SpecScript script = node.getScript();
            if (script == null) {
                throw new IllegalArgumentException("script is null");
            }
            if (script.getParameters() != null) {
                for (SpecVariable variable : script.getParameters()) {
                    if (StringUtils.isBlank(variable.getValue())) {
                        throw new SpecException(SpecErrorCode.VALIDATE_ERROR, "variable {" + variable.getName() + "} value not config");
                    }
                }
            }

            if (node.getRuntimeResource() == null) {
                throw new IllegalArgumentException("runtimeResource is null");
            }

            SpecRuntimeResource runtimeResource = node.getRuntimeResource();
            if (StringUtils.isBlank(runtimeResource.getResourceGroupId())) {
                throw new SpecException(SpecErrorCode.VALIDATE_ERROR, "resourceGroupId not config");
            }
        }
    }

    private static synchronized void init() {
        if (jsonSchemaFactory == null) {
            jsonSchemaFactory = JsonSchemaFactory.getInstance(VersionFlag.V202012,
                builder -> builder.enableSchemaCache(true).jsonNodeReader(new IgnoreNullJsonNodeReader()));
        }
    }

    public static List<String> validate(String specJson) {
        return validate(COMMON_MAIN_SCHEMA_PATH, specJson);
    }

    public static List<String> validate(String schemaPath, String specJson) {
        if (StringUtils.isBlank(specJson)) {
            return List.of("spec json is blank");
        }
        if (jsonSchemaFactory == null) {
            init();
        }

        try {
            JsonSchema schema = jsonSchemaFactory.getSchema(SchemaLocation.of(schemaPath),
                SchemaValidatorsConfig.builder()
                    .pathType(PathType.LEGACY)
                    .errorMessageKeyword(ERROR_MESSAGE_KEY_WORD)
                    .nullableKeywordEnabled(true)
                    .cacheRefs(true)
                    .locale(Locale.ENGLISH)
                    .build());

            Set<ValidationMessage> errorMsgs = schema.validate(specJson, InputFormat.JSON);
            return SetUtils.emptyIfNull(errorMsgs).stream().map(ValidationMessage::getMessage).collect(Collectors.toList());
        } catch (Exception e) {
            throw new SpecException(SpecErrorCode.VALIDATE_ERROR, e.getMessage());
        }
    }

    static class IgnoreNullJsonNodeReader extends DefaultJsonNodeReader {

        private final JsonNodeReader jsonNodeReader;

        protected IgnoreNullJsonNodeReader() {
            super(null, null, null);
            this.jsonNodeReader = DefaultJsonNodeReader.builder().locationAware().build();
        }

        @Override
        public JsonNode readTree(String content, InputFormat inputFormat) throws IOException {
            JsonNode jsonNode = jsonNodeReader.readTree(content, inputFormat);
            removeNulls(jsonNode);
            return jsonNode;
        }

        @Override
        public JsonNode readTree(InputStream content, InputFormat inputFormat) throws IOException {
            JsonNode jsonNode = jsonNodeReader.readTree(content, inputFormat);
            removeNulls(jsonNode);
            return jsonNode;
        }

        private void removeNulls(JsonNode node) {
            if (node == null) {
                return;
            }
            if (node.isObject()) {
                ObjectNode objectNode = (ObjectNode)node;
                List<String> nullKeys = new ArrayList<>();
                objectNode.fields().forEachRemaining(entry -> {
                    if (entry.getValue() instanceof NullNode) {
                        nullKeys.add(entry.getKey());
                    }
                    removeNulls(entry.getValue());
                });
                if (CollectionUtils.isNotEmpty(nullKeys)) {
                    objectNode.remove(nullKeys);
                }
            } else if (node.isArray()) {
                // don't remove nulls in array, it will be identified as an error later
                node.forEach(this::removeNulls);
            }
        }
    }
}
