package com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity;

import com.aliyun.dataworks.common.spec.domain.enums.NodeRecurrenceType;
import com.aliyun.dataworks.common.spec.domain.enums.TriggerType;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecTrigger;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.NodeType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class SpecNodeEntityAdapterTest {

    @Test
    public void testGetNodeType_withManualTrigger() {
        // Given
        SpecNodeEntityAdapter adapter = new SpecNodeEntityAdapter();
        SpecNode node = new SpecNode();
        SpecTrigger trigger = new SpecTrigger();
        trigger.setType(TriggerType.MANUAL);
        node.setTrigger(trigger);
        adapter.setNode(node);

        // When
        Integer nodeType = adapter.getNodeType();

        // Then
        assertEquals(Integer.valueOf(NodeType.MANUAL.getCode()), nodeType);
    }

    @Test
    public void testGetNodeType_withNormalRecurrence() {
        // Given
        SpecNodeEntityAdapter adapter = new SpecNodeEntityAdapter();
        SpecNode node = new SpecNode();
        node.setRecurrence(NodeRecurrenceType.NORMAL);
        adapter.setNode(node);

        // When
        Integer nodeType = adapter.getNodeType();

        // Then
        assertEquals(Integer.valueOf(NodeType.NORMAL.getCode()), nodeType);
    }

    @Test
    public void testGetNodeType_withPauseRecurrence() {
        // Given
        SpecNodeEntityAdapter adapter = new SpecNodeEntityAdapter();
        SpecNode node = new SpecNode();
        node.setRecurrence(NodeRecurrenceType.PAUSE);
        adapter.setNode(node);

        // When
        Integer nodeType = adapter.getNodeType();

        // Then
        assertEquals(Integer.valueOf(NodeType.PAUSE.getCode()), nodeType);
    }

    @Test
    public void testGetNodeType_withSkipRecurrence() {
        // Given
        SpecNodeEntityAdapter adapter = new SpecNodeEntityAdapter();
        SpecNode node = new SpecNode();
        node.setRecurrence(NodeRecurrenceType.SKIP);
        adapter.setNode(node);

        // When
        Integer nodeType = adapter.getNodeType();

        // Then
        assertEquals(Integer.valueOf(NodeType.SKIP.getCode()), nodeType);
    }

    @Test
    public void testGetNodeType_withoutNode() {
        // Given
        SpecNodeEntityAdapter adapter = new SpecNodeEntityAdapter();

        // When
        Integer nodeType = adapter.getNodeType();

        // Then
        assertNull(nodeType);
    }

    @Test
    public void testGetNodeType_withNullTriggerAndRecurrence() {
        // Given
        SpecNodeEntityAdapter adapter = new SpecNodeEntityAdapter();
        SpecNode node = new SpecNode();
        adapter.setNode(node);

        // When
        Integer nodeType = adapter.getNodeType();

        // Then
        assertNull(nodeType);
    }
}