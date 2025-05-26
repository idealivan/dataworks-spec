/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */


package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.DolphinSchedulerPackage;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.DwWorkflow;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.Project;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


public class DolphinSchedulerConverterContextTest {
    
    @Mock
    private Project mockProject;
    
    @Mock
    private DwWorkflow mockDwWorkflow;
    
    @Mock
    private DolphinSchedulerPackage<Object, Object, Object, Object, Object> mockDolphinSchedulerPackage;
    
    private Properties properties;
    
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        properties = new Properties();
        properties.setProperty("test.key", "test.value");
    }
    
    @Test
    public void testConstructorAndGetters() {
        DolphinSchedulerConverterContext<Object, Object, Object, Object, Object> context =
            new DolphinSchedulerConverterContext<>();

        Assert.assertNull(context.getProject());
        Assert.assertNull(context.getDwWorkflow());
        Assert.assertNull(context.getDolphinSchedulerPackage());
        Assert.assertNull(context.getProperties());
    }
    
    @Test
    public void testSettersAndChaining() {
        DolphinSchedulerConverterContext<Object, Object, Object, Object, Object> context =
            new DolphinSchedulerConverterContext<>()
                .setProject(mockProject)
                .setDwWorkflow(mockDwWorkflow)
                .setDolphinSchedulerPackage(mockDolphinSchedulerPackage)
                .setProperties(properties);

        Assert.assertSame(mockProject, context.getProject());
        Assert.assertSame(mockDwWorkflow, context.getDwWorkflow());
        Assert.assertSame(mockDolphinSchedulerPackage, context.getDolphinSchedulerPackage());
        Assert.assertSame(properties, context.getProperties());
    }
    
    @Test
    public void testEqualsAndHashCode() {
        DolphinSchedulerConverterContext<Object, Object, Object, Object, Object> context1 =
            new DolphinSchedulerConverterContext<>()
                .setProject(mockProject)
                .setDwWorkflow(mockDwWorkflow)
                .setDolphinSchedulerPackage(mockDolphinSchedulerPackage)
                .setProperties(properties);
        
        DolphinSchedulerConverterContext<Object, Object, Object, Object, Object> context2 = 
            new DolphinSchedulerConverterContext<>()
                .setProject(mockProject)
                .setDwWorkflow(mockDwWorkflow)
                .setDolphinSchedulerPackage(mockDolphinSchedulerPackage)
                .setProperties(properties);
        
        Assert.assertEquals(context1, context2);
        Assert.assertEquals(context1.hashCode(), context2.hashCode());
        
        Properties differentProperties = new Properties();
        differentProperties.setProperty("different.key", "different.value");
        
        context2.setProperties(differentProperties);
        
        Assert.assertNotEquals(context1, context2);
        Assert.assertNotEquals(context1.hashCode(), context2.hashCode());
    }
    
    @Test
    public void testToString() {
        DolphinSchedulerConverterContext<Object, Object, Object, Object, Object> context =
            new DolphinSchedulerConverterContext<>()
                .setProject(mockProject)
                .setDwWorkflow(mockDwWorkflow)
                .setDolphinSchedulerPackage(mockDolphinSchedulerPackage)
                .setProperties(properties);
        
        String toString = context.toString();
        Assert.assertTrue(toString.contains("DolphinSchedulerConverterContext"));
        Assert.assertTrue(toString.contains("project"));
        Assert.assertTrue(toString.contains("dwWorkflow"));
        Assert.assertTrue(toString.contains("dolphinSchedulerPackage"));
        Assert.assertTrue(toString.contains("properties"));
    }
    
    @Test
    public void testWithRealObjects() {
        Project project = new Project();
        project.setName("TestProject");
        
        DwWorkflow dwWorkflow = new DwWorkflow();
        dwWorkflow.setName("TestWorkflow");
        
        DolphinSchedulerPackage<Object, Object, Object, Object, Object> dolphinSchedulerPackage = 
            new DolphinSchedulerPackage<>();
        dolphinSchedulerPackage.setPackageRoot(new File("test"));
        dolphinSchedulerPackage.setProjects(new ArrayList<>());
        dolphinSchedulerPackage.setProcessDefinitions(new HashMap<>());
        
        Properties props = new Properties();
        props.setProperty("env", "test");

        DolphinSchedulerConverterContext<Object, Object, Object, Object, Object> context =
            new DolphinSchedulerConverterContext<>();
        context.setProject(project);
        context.setDwWorkflow(dwWorkflow);
        context.setDolphinSchedulerPackage(dolphinSchedulerPackage);
        context.setProperties(props);
        
        Assert.assertEquals("TestProject", context.getProject().getName());
        Assert.assertEquals("TestWorkflow", context.getDwWorkflow().getName());
        Assert.assertEquals(new File("test"), context.getDolphinSchedulerPackage().getPackageRoot());
        Assert.assertEquals("test", context.getProperties().getProperty("env"));
    }
} 