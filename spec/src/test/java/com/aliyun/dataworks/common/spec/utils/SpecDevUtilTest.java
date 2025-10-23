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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson2.JSONObject;

import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.interfaces.LabelEnum;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.exception.SpecException;
import com.aliyun.dataworks.common.spec.parser.SpecParserContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for {@link SpecDevUtil}
 *
 * @author yiwei.qyw
 * @date 2023/7/7
 */
public class SpecDevUtilTest {

    private SpecParserContext parserContext;

    @Before
    public void setUp() {
        parserContext = new SpecParserContext();
    }

    @Test
    public void testSetSimpleField() {
        // Prepare test data
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("id", "testId");
        ctxMap.put("name", "testName");
        ctxMap.put("description", "testDescription");
        ctxMap.put("timeout", 100);
        ctxMap.put("priority", 5);
        
        SpecNode specNode = new SpecNode();
        
        // Execute method
        SpecDevUtil.setSimpleField(ctxMap, specNode);
        
        // Verify results
        Assert.assertEquals("testId", specNode.getId());
        Assert.assertEquals("testName", specNode.getName());
        Assert.assertEquals("testDescription", specNode.getDescription());
        Assert.assertEquals(Integer.valueOf(100), specNode.getTimeout());
        Assert.assertEquals(Integer.valueOf(5), specNode.getPriority());
    }

    @Test
    public void testSetSimpleFieldWithPrimitiveTypes() {
        // Prepare test data
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("integerValue", 42);
        ctxMap.put("longValue", 1000L);
        ctxMap.put("doubleValue", 3.14);
        ctxMap.put("booleanValue", true);
        ctxMap.put("stringValue", "test");
        ctxMap.put("bigDecimalValue", "123.45");
        
        TestEntity testEntity = new TestEntity();
        
        // Execute method
        SpecDevUtil.setSimpleField(ctxMap, testEntity);
        
        // Verify results
        Assert.assertEquals(Integer.valueOf(42), testEntity.getIntegerValue());
        Assert.assertEquals(Long.valueOf(1000L), testEntity.getLongValue());
        Assert.assertEquals(Double.valueOf(3.14), testEntity.getDoubleValue());
        Assert.assertEquals(Boolean.TRUE, testEntity.getBooleanValue());
        Assert.assertEquals("test", testEntity.getStringValue());
        Assert.assertEquals("123.45", testEntity.getBigDecimalValue());
    }

    @Test
    public void testSetEnumField() {
        // Prepare test data
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("recurrence", "NORMAL");
        
        SpecNode specNode = new SpecNode();
        
        // Execute method
        SpecDevUtil.setEnumField(ctxMap, specNode);
        
        // Verify results
        Assert.assertEquals(com.aliyun.dataworks.common.spec.domain.enums.NodeRecurrenceType.NORMAL, specNode.getRecurrence());
    }

    @Test
    public void testSetMapField() {
        // Prepare test data
        Map<String, Object> ctxMap = new HashMap<>();
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("param1", "value1");
        paramMap.put("param2", "value2");
        ctxMap.put("metadata", paramMap);
        
        SpecScript specScript = new SpecScript();
        
        // Execute method
        SpecDevUtil.setMapField(ctxMap, specScript);
        
        // Verify results
        Assert.assertEquals(paramMap, specScript.getMetadata());
    }

    @Test
    public void testSetValue() throws NoSuchFieldException {
        // Prepare test data
        TestEntity testEntity = new TestEntity();
        String testValue = "testValue";
        
        // Execute method
        SpecDevUtil.setValue(testEntity, TestEntity.class.getDeclaredField("stringValue"), testValue);
        
        // Verify results
        Assert.assertEquals(testValue, testEntity.getStringValue());
    }

    @Test
    public void testParseRefId() {
        // Test case 1: Valid reference with type and id
        String refId1 = "{{node.testId}}";
        SpecDevUtil.RefMsg refMsg1 = SpecDevUtil.parseRefId(refId1);
        Assert.assertEquals("node", refMsg1.getType());
        Assert.assertEquals("testId", refMsg1.getId());
        
        // Test case 2: Valid reference with only id
        String refId2 = "{{testId}}";
        SpecDevUtil.RefMsg refMsg2 = SpecDevUtil.parseRefId(refId2);
        Assert.assertNull(refMsg2.getType());
        Assert.assertEquals("testId", refMsg2.getId());
        
        // Test case 3: Invalid reference format
        String refId3 = "testId";
        SpecDevUtil.RefMsg refMsg3 = SpecDevUtil.parseRefId(refId3);
        Assert.assertNull(refMsg3.getType());
        Assert.assertEquals("testId", refMsg3.getId());
    }

    @Test(expected = SpecException.class)
    public void testParseRefIdInvalidFormat() {
        // Test case: Invalid reference format with too many parts
        String refId = "{{type.id.extra}}";
        SpecDevUtil.parseRefId(refId);
    }

    @Test
    public void testGetPropertyFields() {
        // Prepare test data
        TestEntity testEntity = new TestEntity();
        
        // Execute method
        List<java.lang.reflect.Field> fields = SpecDevUtil.getPropertyFields(testEntity);
        
        // Verify results - should include all non-static, non-synthetic fields
        Assert.assertFalse(fields.isEmpty());
        Assert.assertTrue(fields.stream().anyMatch(f -> f.getName().equals("stringValue")));
        Assert.assertTrue(fields.stream().anyMatch(f -> f.getName().equals("integerValue")));
    }

    @Test
    public void testIsSimpleType() {
        // Test primitive types
        Assert.assertTrue("int should be simple type", SpecDevUtil.isSimpleType(int.class));
        Assert.assertTrue("boolean should be simple type", SpecDevUtil.isSimpleType(boolean.class));
        Assert.assertTrue("double should be simple type", SpecDevUtil.isSimpleType(double.class));
        
        // Test wrapper types
        Assert.assertTrue("Integer should be simple type", SpecDevUtil.isSimpleType(Integer.class));
        Assert.assertTrue("Boolean should be simple type", SpecDevUtil.isSimpleType(Boolean.class));
        Assert.assertTrue("Double should be simple type", SpecDevUtil.isSimpleType(Double.class));
        Assert.assertTrue("String should be simple type", SpecDevUtil.isSimpleType(String.class));
        
        // Test map type
        Assert.assertTrue("Map should be simple type", SpecDevUtil.isSimpleType(Map.class));
        
        // Test enum type
        Assert.assertTrue("Enum should be simple type", SpecDevUtil.isSimpleType(com.aliyun.dataworks.common.spec.domain.enums.NodeRecurrenceType.class));
        
        // Test complex type (should be false)
        Assert.assertFalse("SpecNode should not be simple type", SpecDevUtil.isSimpleType(SpecNode.class));
    }

    @Test
    public void testWriteJsonObject() {
        // Prepare test data
        SpecNode specNode = new SpecNode();
        specNode.setId("testId");
        specNode.setName("testName");
        specNode.setDescription("testDescription");
        specNode.setTimeout(100);
        
        // Execute method
        JSONObject jsonObject = SpecDevUtil.writeJsonObject(specNode, false);
        
        // Verify results
        Assert.assertNotNull(jsonObject);
        Assert.assertEquals("testId", jsonObject.getString("id"));
        Assert.assertEquals("testName", jsonObject.getString("name"));
        Assert.assertEquals("testDescription", jsonObject.getString("description"));
        Assert.assertEquals(100, jsonObject.getIntValue("timeout"));
    }

    @Test
    public void testWriteJsonObjectWithoutCollectionFields() {
        // Prepare test data
        SpecScript specScript = new SpecScript();
        specScript.setId("testId");
        
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("param1", "value1");
        specScript.setMetadata(metadata);
        
        // Execute method with withoutCollectionFields = true
        JSONObject jsonObject = SpecDevUtil.writeJsonObject(specScript, true);
        
        // Verify results - metadata should not be included
        Assert.assertNotNull(jsonObject);
        Assert.assertEquals("testId", jsonObject.getString("id"));
        Assert.assertFalse(jsonObject.containsKey("metadata"));
    }

    @Test
    public void testSetSimpleListField() {
        // Prepare test data
        Map<String, Object> ctxMap = new HashMap<>();
        List<String> stringList = Arrays.asList("value1", "value2", "value3");
        ctxMap.put("stringList", stringList);
        
        TestEntity testEntity = new TestEntity();
        
        // Execute method
        SpecDevUtil.setSimpleListField(ctxMap, testEntity);
        
        // Verify results
        Assert.assertEquals(stringList, testEntity.getStringList());
    }

    @Test
    public void testSetSameKeyField() {
        // Prepare test data
        Map<String, Object> ctxMap = new HashMap<>();
        ctxMap.put("id", "testId");
        ctxMap.put("recurrence", "NORMAL");
        
        SpecNode specNode = new SpecNode();
        
        // Execute method
        SpecDevUtil.setSameKeyField(ctxMap, specNode, parserContext);
        
        // Verify results
        Assert.assertEquals("testId", specNode.getId());
        Assert.assertEquals(com.aliyun.dataworks.common.spec.domain.enums.NodeRecurrenceType.NORMAL, specNode.getRecurrence());
    }

    @Test
    public void testSetSpecObjectWithStringReference() {
        // Prepare test data
        SpecNode ownerObject = new SpecNode();
        String fieldName = "script";
        String value = "{{script.testScript}}";
        
        // Execute method
        SpecDevUtil.setSpecObject(ownerObject, fieldName, value, parserContext);
        
        // Verify results - reference should be added to context
        Assert.assertFalse(parserContext.getRefEntityList().isEmpty());
        SpecParserContext.SpecEntityContext entityContext = parserContext.getRefEntityList().get(0);
        Assert.assertEquals("testScript", entityContext.getEntityValue());
    }

    @Test
    public void testSetSpecObjectWithMap() {
        // Prepare test data
        SpecNode ownerObject = new SpecNode();
        String fieldName = "script";
        Map<String, Object> value = new HashMap<>();
        value.put("id", "testScript");
        value.put("name", "testScriptName");
        
        // Execute method
        SpecDevUtil.setSpecObject(ownerObject, fieldName, value, parserContext);
        
        // Note: This test would require a proper parser to be registered
        // For now, we're just verifying the method doesn't throw an exception
    }

    @Test
    public void testGetFieldClz() throws NoSuchFieldException {
        // Prepare test data
        TestEntity testEntity = new TestEntity();
        java.lang.reflect.Field field = testEntity.getClass().getDeclaredField("stringList");
        
        // Execute method
        Class<?> fieldClz = SpecDevUtil.getFieldClz(field);
        
        // Verify results
        Assert.assertEquals(String.class, fieldClz);
    }

    // Test entity class for testing purposes
    public static class TestEntity {
        private String stringValue;
        private Integer integerValue;
        private Long longValue;
        private Double doubleValue;
        private Boolean booleanValue;
        private String bigDecimalValue;
        private List<String> stringList;
        
        // Getters and setters
        public String getStringValue() {
            return stringValue;
        }
        
        public void setStringValue(String stringValue) {
            this.stringValue = stringValue;
        }
        
        public Integer getIntegerValue() {
            return integerValue;
        }
        
        public void setIntegerValue(Integer integerValue) {
            this.integerValue = integerValue;
        }
        
        public Long getLongValue() {
            return longValue;
        }
        
        public void setLongValue(Long longValue) {
            this.longValue = longValue;
        }
        
        public Double getDoubleValue() {
            return doubleValue;
        }
        
        public void setDoubleValue(Double doubleValue) {
            this.doubleValue = doubleValue;
        }
        
        public Boolean getBooleanValue() {
            return booleanValue;
        }
        
        public void setBooleanValue(Boolean booleanValue) {
            this.booleanValue = booleanValue;
        }
        
        public String getBigDecimalValue() {
            return bigDecimalValue;
        }
        
        public void setBigDecimalValue(String bigDecimalValue) {
            this.bigDecimalValue = bigDecimalValue;
        }
        
        public List<String> getStringList() {
            return stringList;
        }
        
        public void setStringList(List<String> stringList) {
            this.stringList = stringList;
        }
    }
}