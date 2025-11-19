/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */


package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.aliyun.dataworks.common.spec.domain.enums.VariableScopeType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.DataType;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.Direct;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.model.Property;
import com.aliyun.migrationx.common.utils.UuidGenerators;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class ParamListConverterTest {

    @Mock
    private TaskDefinition mockTaskDefinition;
    
    private List<Property> paramList;
    
    @Before
    public void setUp() {
        paramList = new ArrayList<>();
    }

    private Property createProperty(String name, String value, Direct direct, DataType type) {
        Property property = new Property();
        property.setProp(name);
        property.setValue(value);
        property.setDirect(direct);
        property.setType(type);
        return property;
    }
    
    @Test
    public void testConvertEmptyParamList() {
        ParamListConverter converter = new ParamListConverter(Collections.emptyList());
        
        List<SpecVariable> result = converter.convert();

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }
    
    @Test
    public void testConvertNullParamList() {
        ParamListConverter converter = new ParamListConverter(null);

        List<SpecVariable> result = converter.convert();

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }
    
    @Test
    public void testConvertInputParameters() {
        Property property1 = createProperty("param1", "value1", Direct.IN, DataType.VARCHAR);
        Property property2 = createProperty("param2", "$SYSTEM_VAR", Direct.IN, DataType.VARCHAR);
        paramList.add(property1);
        paramList.add(property2);
        
        try (MockedStatic<UuidGenerators> uuidMock = Mockito.mockStatic(UuidGenerators.class)) {
            uuidMock.when(UuidGenerators::generateUuid).thenReturn("mock-uuid-1", "mock-uuid-2");
            
            ParamListConverter converter = new ParamListConverter(paramList);
            List<SpecVariable> result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(2, result.size());
            
            SpecVariable var1 = result.get(0);
            Assert.assertEquals("mock-uuid-1", var1.getId());
            Assert.assertEquals("param1", var1.getName());
            Assert.assertEquals("value1", var1.getValue());
            Assert.assertEquals("VARCHAR", var1.getDescription());
            Assert.assertEquals(VariableType.CONSTANT, var1.getType());
            Assert.assertEquals(VariableScopeType.FLOW, var1.getScope());
            
            SpecVariable var2 = result.get(1);
            Assert.assertEquals("mock-uuid-2", var2.getId());
            Assert.assertEquals("param2", var2.getName());
            Assert.assertEquals("$SYSTEM_VAR", var2.getValue());
            Assert.assertEquals("VARCHAR", var2.getDescription());
            Assert.assertEquals(VariableType.SYSTEM, var2.getType());
            Assert.assertEquals(VariableScopeType.FLOW, var2.getScope());
        }
    }
    
    @Test
    public void testConvertOutputParameters() {
        Property property = createProperty("output1", "output_value", Direct.OUT, DataType.VARCHAR);
        paramList.add(property);
        
        try (MockedStatic<UuidGenerators> uuidMock = Mockito.mockStatic(UuidGenerators.class)) {
            uuidMock.when(UuidGenerators::generateUuid).thenReturn("mock-uuid-output");
            
            ParamListConverter converter = new ParamListConverter(paramList, mockTaskDefinition);
            List<SpecVariable> result = converter.convert();

            Assert.assertNotNull(result);
            Assert.assertEquals(1, result.size());

            SpecVariable var = result.get(0);
            Assert.assertEquals("mock-uuid-output", var.getId());
            Assert.assertEquals("output1", var.getName());
            Assert.assertEquals("output_value", var.getValue());
            Assert.assertEquals("VARCHAR", var.getDescription());
            Assert.assertEquals(VariableType.NODE_OUTPUT, var.getType());
            Assert.assertEquals(VariableScopeType.NODE_CONTEXT, var.getScope());
        }
    }
    
    @Test
    public void testConvertMixedParameters() {
        Property inputProperty = createProperty("input", "input_value", Direct.IN,DataType.VARCHAR);
        Property outputProperty = createProperty("output", "output_value", Direct.OUT, DataType.VARCHAR);
        paramList.add(inputProperty);
        paramList.add(outputProperty);

        try (MockedStatic<UuidGenerators> uuidMock = Mockito.mockStatic(UuidGenerators.class)) {
            uuidMock.when(UuidGenerators::generateUuid).thenReturn("mock-uuid-input", "mock-uuid-output");

            ParamListConverter converter = new ParamListConverter(paramList, mockTaskDefinition);
            List<SpecVariable> result = converter.convert();

            Assert.assertNotNull(result);
            Assert.assertEquals(2, result.size());

            SpecVariable inputVar = result.get(0);
            Assert.assertEquals("mock-uuid-input", inputVar.getId());
            Assert.assertEquals("input", inputVar.getName());
            Assert.assertEquals("input_value", inputVar.getValue());
            Assert.assertEquals(VariableType.CONSTANT, inputVar.getType());
            Assert.assertEquals(VariableScopeType.NODE_PARAMETER, inputVar.getScope());

            SpecVariable outputVar = result.get(1);
            Assert.assertEquals("mock-uuid-output", outputVar.getId());
            Assert.assertEquals("output", outputVar.getName());
            Assert.assertEquals("output_value", outputVar.getValue());
            Assert.assertEquals(VariableType.NODE_OUTPUT, outputVar.getType());
            Assert.assertEquals(VariableScopeType.NODE_CONTEXT, outputVar.getScope());
        }
    }
    
    @Test
    public void testConvertGlobalOutputParameters() {
        Property outputProperty = createProperty("global_output", "output_value", Direct.OUT, DataType.VARCHAR);
        paramList.add(outputProperty);
        
        ParamListConverter converter = new ParamListConverter(paramList);
        
        List<SpecVariable> result = converter.convert();
        
        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }
    
    @Test
    public void testConvertWithDifferentPropertyTypes() {
        Property intProperty = createProperty("int_param", "123", Direct.IN, DataType.INTEGER);
        Property longProperty = createProperty("long_param", "123456789", Direct.IN, DataType.LONG);
        Property doubleProperty = createProperty("double_param", "123.45", Direct.IN, DataType.DOUBLE);
        Property dateProperty = createProperty("date_param", "2024-05-15", Direct.IN, DataType.DATE);
        
        paramList.addAll(Arrays.asList(intProperty, longProperty, doubleProperty, dateProperty));
        
        try (MockedStatic<UuidGenerators> uuidMock = Mockito.mockStatic(UuidGenerators.class)) {
            uuidMock.when(UuidGenerators::generateUuid).thenReturn(
                "mock-uuid-int", "mock-uuid-long", "mock-uuid-double", "mock-uuid-date");
            
            ParamListConverter converter = new ParamListConverter(paramList);
            List<SpecVariable> result = converter.convert();

            Assert.assertNotNull(result);
            Assert.assertEquals(4, result.size());
            
            Assert.assertEquals("INTEGER", result.get(0).getDescription());
            Assert.assertEquals("LONG", result.get(1).getDescription());
            Assert.assertEquals("DOUBLE", result.get(2).getDescription());
            Assert.assertEquals("DATE", result.get(3).getDescription());
        }
    }
} 