package com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity;

import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.File;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.FileNodeCfg;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.NodeType;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.NodeUseType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class FileDetailEntityAdapterTest {

    @Test
    public void testGetNodeType_withFileNodeCfg() {
        // Given
        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();
        FileNodeCfg fileNodeCfg = new FileNodeCfg();
        fileNodeCfg.setNodeType(1);
        adapter.setFileNodeCfg(fileNodeCfg);

        // When
        Integer nodeType = adapter.getNodeType();

        // Then
        assertEquals(Integer.valueOf(1), nodeType);
    }

    @Test
    public void testGetNodeType_withoutFileNodeCfg() {
        // Given
        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();

        // When
        Integer nodeType = adapter.getNodeType();

        // Then
        assertNull(nodeType);
    }

    @Test
    public void testGetNodeUseType_withManualNodeType() {
        // Given
        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();
        FileNodeCfg fileNodeCfg = new FileNodeCfg();
        fileNodeCfg.setNodeType(NodeType.MANUAL.getCode()); // Manual node type is 1
        adapter.setFileNodeCfg(fileNodeCfg);

        // When
        NodeUseType nodeUseType = adapter.getNodeUseType();

        // Then
        assertEquals(NodeUseType.MANUAL_WORKFLOW, nodeUseType);
    }

    @Test
    public void testGetNodeUseType_withScheduledUseType() {
        // Given
        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();
        File file = new File();
        file.setUseType(NodeUseType.SCHEDULED.getValue()); // Scheduled use type is 0
        adapter.setFile(file);

        // When
        NodeUseType nodeUseType = adapter.getNodeUseType();

        // Then
        assertEquals(NodeUseType.SCHEDULED, nodeUseType);
    }

    @Test
    public void testGetNodeUseType_withManualUseType() {
        // Given
        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();
        File file = new File();
        file.setUseType(NodeUseType.MANUAL.getValue()); // Manual use type is 1
        adapter.setFile(file);

        // When
        NodeUseType nodeUseType = adapter.getNodeUseType();

        // Then
        assertEquals(NodeUseType.MANUAL, nodeUseType);
    }

    @Test
    public void testGetNodeUseType_withUnknownUseType() {
        // Given
        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();
        File file = new File();
        file.setUseType(999); // Unknown use type
        adapter.setFile(file);

        // When
        NodeUseType nodeUseType = adapter.getNodeUseType();

        // Then
        assertEquals(NodeUseType.SCHEDULED, nodeUseType); // Should default to SCHEDULED
    }

    @Test
    public void testGetNodeUseType_withoutFileAndFileNodeCfg() {
        // Given
        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();

        // When
        NodeUseType nodeUseType = adapter.getNodeUseType();

        // Then
        assertEquals(NodeUseType.SCHEDULED, nodeUseType);
    }

    @Test
    public void testGetNodeUseType_withNullUseType() {
        // Given
        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();
        File file = new File();
        file.setUseType(null);
        adapter.setFile(file);

        // When
        NodeUseType nodeUseType = adapter.getNodeUseType();

        // Then
        assertEquals(NodeUseType.SCHEDULED, nodeUseType);
    }
}