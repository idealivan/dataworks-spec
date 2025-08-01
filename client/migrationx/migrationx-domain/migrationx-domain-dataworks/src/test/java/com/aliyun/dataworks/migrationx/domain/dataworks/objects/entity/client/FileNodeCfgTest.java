package com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * FileNodeCfg 类的单元测试
 */
public class FileNodeCfgTest {

    @Test
    public void testSetIgnoreBranchConditionSkip() {
        FileNodeCfg fileNodeCfg = new FileNodeCfg();
        fileNodeCfg.setIgnoreBranchConditionSkip(true);
        assertEquals("{\"ignoreBranchConditionSkip\":true}", fileNodeCfg.getExtConfig());
        assertTrue(fileNodeCfg.getIgnoreBranchConditionSkip());
        fileNodeCfg.setIgnoreBranchConditionSkip(false);
        assertEquals("{\"ignoreBranchConditionSkip\":false}", fileNodeCfg.getExtConfig());
        assertFalse(fileNodeCfg.getIgnoreBranchConditionSkip());
    }

    @Test
    public void testSetAlisaTaskKillTimeout() {
        FileNodeCfg fileNodeCfg = new FileNodeCfg();
        fileNodeCfg.setAlisaTaskKillTimeout(1);
        assertEquals("{\"alisaTaskKillTimeout\":1}", fileNodeCfg.getExtConfig());
        assertEquals(1, fileNodeCfg.getAlisaTaskKillTimeout().intValue());
    }

    @Test
    public void testGetIgnoreBranchConditionSkip() {
        FileNodeCfg fileNodeCfg = new FileNodeCfg();
        fileNodeCfg.setExtConfig("{\"ignoreBranchConditionSkip\":true");
        assertNull(fileNodeCfg.getIgnoreBranchConditionSkip());
    }

    @Test
    public void testGetAlisaTaskKillTimeout() {
        FileNodeCfg fileNodeCfg = new FileNodeCfg();
        fileNodeCfg.setExtConfig("{\"alisaTaskKillTimeout\":1");
        assertNull(fileNodeCfg.getAlisaTaskKillTimeout());
    }

}
