/*
 * Copyright (c)  2024. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */


package com.aliyun.dataworks.migrationx.transformer.dataworks.converter.dolphinscheduler.v3.workflow;

import java.text.ParseException;
import java.util.Date;

import com.aliyun.dataworks.common.spec.domain.enums.NodeRecurrenceType;
import com.aliyun.dataworks.common.spec.domain.enums.TriggerType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecTrigger;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.Schedule;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.TaskDefinition;
import com.aliyun.dataworks.migrationx.domain.dataworks.dolphinscheduler.v3.enums.Flag;
import com.aliyun.dataworks.migrationx.domain.dataworks.utils.CronExpressUtil;
import com.aliyun.migrationx.common.utils.DateUtils;
import com.aliyun.migrationx.common.utils.UuidGenerators;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class TriggerConverterTest {

    @Mock
    private Schedule mockSchedule;
    
    @Mock
    private TaskDefinition mockTaskDefinition;
    
    private Date startTime;
    private Date endTime;
    
    @Before
    public void setUp() {
        startTime = new Date();
        endTime = new Date(startTime.getTime() + 86400000); // One day later
        
        when(mockSchedule.getId()).thenReturn(12345);
        when(mockSchedule.getStartTime()).thenReturn(startTime);
        when(mockSchedule.getEndTime()).thenReturn(endTime);
        when(mockSchedule.getCrontab()).thenReturn("0 0 12 * * ?");
        when(mockSchedule.getTimezoneId()).thenReturn("Asia/Shanghai");
        
        when(mockTaskDefinition.getDelayTime()).thenReturn(10); // 10 minutes delay
        when(mockTaskDefinition.getFlag()).thenReturn(Flag.YES);
    }
    
    @Test
    public void testConvertWithSchedule() {
        try (MockedStatic<UuidGenerators> uuidMock = Mockito.mockStatic(UuidGenerators.class);
             MockedStatic<DateUtils> dateUtilsMock = Mockito.mockStatic(DateUtils.class);
             MockedStatic<CronExpressUtil> cronMock = Mockito.mockStatic(CronExpressUtil.class)) {
            
            String mockUuid = "mock-uuid-12345";
            uuidMock.when(() -> UuidGenerators.generateUuid(anyLong())).thenReturn(mockUuid);
            
            String mockStartTimeStr = "2024-05-15 00:00:00";
            String mockEndTimeStr = "2024-05-16 00:00:00";
            dateUtilsMock.when(() -> DateUtils.convertDateToString(eq(startTime))).thenReturn(mockStartTimeStr);
            dateUtilsMock.when(() -> DateUtils.convertDateToString(eq(endTime))).thenReturn(mockEndTimeStr);
            
            String mockSpecCron = "00:00:12 * * *";
            cronMock.when(() -> CronExpressUtil.quartzCronExpressionToDwCronExpress(anyString())).thenReturn(mockSpecCron);
            
            TriggerConverter converter = new TriggerConverter(mockSchedule);
            SpecTrigger result = converter.convert();
            
            Assert.assertNotNull(result);
            Assert.assertEquals(mockUuid, result.getId());
            Assert.assertEquals(TriggerType.SCHEDULER, result.getType());
            Assert.assertEquals(mockStartTimeStr, result.getStartTime());
            Assert.assertEquals(mockEndTimeStr, result.getEndTime());
            Assert.assertEquals(mockSpecCron, result.getCron());
            Assert.assertEquals("Asia/Shanghai", result.getTimezone());
            Assert.assertEquals(Integer.valueOf(0), result.getDelaySeconds());
        }
    }
    
    @Test
    public void testConvertWithNullSchedule() {
        // Create converter with null schedule
        TriggerConverter converter = new TriggerConverter(null);
        SpecTrigger result = converter.convert();
        
        // Verify result is null since both schedule and taskDefinition are null
        Assert.assertNull(result);
    }
    
    @Test
    public void testConvertWithTaskDefinition() {
        // Setup existing trigger
        SpecTrigger existingTrigger = new SpecTrigger();
        existingTrigger.setType(TriggerType.SCHEDULER);
        existingTrigger.setCron("00:00:12 * * *");
        existingTrigger.setStartTime("2024-05-15 00:00:00");
        existingTrigger.setEndTime("2024-05-16 00:00:00");
        existingTrigger.setTimezone("Asia/Shanghai");
        existingTrigger.setDelaySeconds(0); // Will be overridden
        
        // Setup static mocks
        try (MockedStatic<UuidGenerators> uuidMock = Mockito.mockStatic(UuidGenerators.class)) {
            // Mock UUID generation
            String mockUuid = "mock-uuid-task";
            uuidMock.when(UuidGenerators::generateUuid).thenReturn(mockUuid);
            
            // Create converter and convert
            TriggerConverter converter = new TriggerConverter(existingTrigger, mockTaskDefinition);
            SpecTrigger result = converter.convert();
            
            // Verify result
            Assert.assertNotNull(result);
            Assert.assertEquals(mockUuid, result.getId());
            Assert.assertEquals(TriggerType.SCHEDULER, result.getType());
            Assert.assertEquals("2024-05-15 00:00:00", result.getStartTime());
            Assert.assertEquals("2024-05-16 00:00:00", result.getEndTime());
            Assert.assertEquals("00:00:12 * * *", result.getCron());
            Assert.assertEquals("Asia/Shanghai", result.getTimezone());
            
            // Verify delay seconds (10 minutes = 600 seconds)
            Assert.assertEquals(Integer.valueOf(600), result.getDelaySeconds());
            
            // Verify recurrence type
            Assert.assertEquals(NodeRecurrenceType.NORMAL, result.getRecurrence());
        }
    }
    
    @Test
    public void testConvertWithTaskDefinitionPaused() {
        // Setup existing trigger
        SpecTrigger existingTrigger = new SpecTrigger();
        existingTrigger.setType(TriggerType.SCHEDULER);
        
        // Change task definition flag to NO (PAUSE)
        when(mockTaskDefinition.getFlag()).thenReturn(Flag.NO);
        
        // Setup static mocks
        try (MockedStatic<UuidGenerators> uuidMock = Mockito.mockStatic(UuidGenerators.class)) {
            // Mock UUID generation
            uuidMock.when(UuidGenerators::generateUuid).thenReturn("mock-uuid-task");
            
            // Create converter and convert
            TriggerConverter converter = new TriggerConverter(existingTrigger, mockTaskDefinition);
            SpecTrigger result = converter.convert();
            
            // Verify recurrence type is PAUSE
            Assert.assertEquals(NodeRecurrenceType.PAUSE, result.getRecurrence());
        }
    }
    
    @Test
    public void testConvertWithCronParseException() {
        // Setup mock schedule with a cron that will cause parse exception
        when(mockSchedule.getCrontab()).thenReturn("invalid-cron");
        
        // Setup static mocks
        try (MockedStatic<UuidGenerators> uuidMock = Mockito.mockStatic(UuidGenerators.class);
             MockedStatic<DateUtils> dateUtilsMock = Mockito.mockStatic(DateUtils.class);
             MockedStatic<CronExpressUtil> cronMock = Mockito.mockStatic(CronExpressUtil.class)) {
            
            // Mock UUID generation
            uuidMock.when(() -> UuidGenerators.generateUuid(anyLong())).thenReturn("mock-uuid");
            
            // Mock date conversion
            dateUtilsMock.when(() -> DateUtils.convertDateToString(any(Date.class))).thenReturn("2024-05-15 00:00:00");
            
            // Mock cron conversion to throw exception
            cronMock.when(() -> CronExpressUtil.quartzCronExpressionToDwCronExpress(anyString()))
                .thenThrow(new ParseException("Invalid cron expression", 0));
            
            // Create converter and convert
            TriggerConverter converter = new TriggerConverter(mockSchedule);
            SpecTrigger result = converter.convert();
            
            // Verify result - should use original cron when parse fails
            Assert.assertNotNull(result);
            Assert.assertEquals("invalid-cron", result.getCron());
        }
    }

    @Test
    public void testConvertWithNullTaskDefinition() {
        // Create converter with null taskDefinition
        TriggerConverter converter = new TriggerConverter(new SpecTrigger(), null);
        Assert.assertNull(converter.convert()); // Should throw RuntimeException
    }
    
    @Test
    public void testConvertWithNullTrigger() {
        // Create converter with null trigger
        TriggerConverter converter = new TriggerConverter(null, mockTaskDefinition);
        Assert.assertNull(converter.convert()); // Should throw RuntimeException
    }
} 