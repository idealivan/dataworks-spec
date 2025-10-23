package com.aliyun.dataworks.migrationx.domain.dataworks.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDoWhile;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDataIntegrationJob;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFileResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecFunction;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecRuntimeResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.component.SpecComponent;
import com.aliyun.dataworks.common.spec.domain.ref.component.SpecComponentParameter;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.SpecExtractInfoHandler;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.SpecInfoHandlerFactory;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl.ComponentSpecInfoHandler;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl.DiJobSpecInfoHandler;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl.FlowSpecInfoHandler;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl.FunctionInfoSpecHandler;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl.ManualFlowSpecInfoHandler;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl.NodeSpecInfoHandler;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl.ResourceSpecInfoHandler;
import com.google.common.collect.Lists;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-08-20
 */
public class SpecInfoUtilTest {

    @Test
    public void testNodeCase() {
        Specification<DataWorksWorkflowSpec> doWhileSpec = getDoWhileSpec();
        assertEquals(2, SpecInfoUtil.getInnerSpecifications(doWhileSpec).size());

        Specification<DataWorksWorkflowSpec> nodeSpecification = getNodeSpecification();
        assertEquals("123456789", SpecInfoUtil.getSpecRefEntity(nodeSpecification).getId());
        assertEquals("123456789", SpecInfoUtil.getSpecId(nodeSpecification));
        assertEquals("ODPS_SQL", SpecInfoUtil.getSpecCommand(nodeSpecification));
        assertEquals(Integer.valueOf(10), SpecInfoUtil.getSpecCommandTypeId(nodeSpecification));
        assertEquals("test", SpecInfoUtil.getSpecName(nodeSpecification));
        assertEquals("path/test", SpecInfoUtil.getSpecPath(nodeSpecification));
        assertEquals("SQL", SpecInfoUtil.getSpecLanguage(nodeSpecification));
        assertEquals("content", SpecInfoUtil.getScriptContent(nodeSpecification));
        assertEquals("446209", SpecInfoUtil.getOwner(nodeSpecification));
        assertEquals("description", SpecInfoUtil.getDescription(nodeSpecification));
        assertEquals("test", SpecInfoUtil.getDataSourceName(nodeSpecification));
        assertEquals("resourceGroup", SpecInfoUtil.getResourceGroupIdentifier(nodeSpecification));
        assertEquals("resourceGroupId", SpecInfoUtil.getResourceGroupId(nodeSpecification));

        Map<String, String> uuidMap = new HashMap<>();
        SpecInfoUtil.resetUuid(nodeSpecification, uuidMap);
        assertNotEquals("123456789", uuidMap.get("123456789"));
        assertNotEquals("123456789", nodeSpecification.getSpec().getNodes().get(0).getId());
        assertEquals(uuidMap.get("123456789"), nodeSpecification.getSpec().getNodes().get(0).getId());

        uuidMap.put("123456780", "112233445566");
        SpecInfoUtil.replaceUuid(nodeSpecification, uuidMap);
        assertEquals("112233445566", nodeSpecification.getSpec().getFlow().get(0).getDepends().get(0).getNodeId().getId());
    }

    @Test
    public void testWorkflowCase() {
        Specification<DataWorksWorkflowSpec> workflowSpecification = getWorkflowSpecification();
        assertEquals(1, SpecInfoUtil.getInnerSpecifications(workflowSpecification).size());
        assertEquals("123456789", SpecInfoUtil.getSpecRefEntity(workflowSpecification).getId());
        assertEquals("123456789", SpecInfoUtil.getSpecId(workflowSpecification));
        assertEquals("WORKFLOW", SpecInfoUtil.getSpecCommand(workflowSpecification));
        assertEquals(Integer.valueOf(1001), SpecInfoUtil.getSpecCommandTypeId(workflowSpecification));
        assertEquals("test", SpecInfoUtil.getSpecName(workflowSpecification));
        assertEquals("path/test", SpecInfoUtil.getSpecPath(workflowSpecification));
        assertNull(SpecInfoUtil.getSpecLanguage(workflowSpecification));
        assertEquals("content", SpecInfoUtil.getScriptContent(workflowSpecification));
        assertEquals("446209", SpecInfoUtil.getOwner(workflowSpecification));
        assertEquals("description", SpecInfoUtil.getDescription(workflowSpecification));
        assertNull(SpecInfoUtil.getDataSourceName(workflowSpecification));
        assertNull(SpecInfoUtil.getResourceGroupIdentifier(workflowSpecification));
        assertNull(null, SpecInfoUtil.getResourceGroupId(workflowSpecification));

        Map<String, String> uuidMap = new HashMap<>();
        SpecInfoUtil.resetUuid(workflowSpecification, uuidMap);
        assertNotEquals("123456789", SpecInfoUtil.getSpecId(workflowSpecification));
        assertEquals(uuidMap.get("123456789"), SpecInfoUtil.getSpecId(workflowSpecification));
        uuidMap.put("123456780", "112233445566");
        SpecInfoUtil.replaceUuid(workflowSpecification, uuidMap);
        assertEquals("112233445566",
            workflowSpecification.getSpec().getWorkflows().get(0).getDependencies().get(0).getDepends().get(0).getNodeId().getId());
    }

    @Test
    public void testFileResourceCase() {
        Specification<DataWorksWorkflowSpec> specification = getFileResourceSpecification();
        assertEquals(0, SpecInfoUtil.getInnerSpecifications(specification).size());
        assertEquals("123456789", SpecInfoUtil.getSpecRefEntity(specification).getId());
        assertEquals("123456789", SpecInfoUtil.getSpecId(specification));
        assertEquals("ODPS_JAR", SpecInfoUtil.getSpecCommand(specification));
        assertEquals(Integer.valueOf(13), SpecInfoUtil.getSpecCommandTypeId(specification));
        assertEquals("test.jar", SpecInfoUtil.getSpecName(specification));
        assertEquals("path/test.jar", SpecInfoUtil.getSpecPath(specification));
        assertNull(SpecInfoUtil.getSpecLanguage(specification));
        assertEquals("{}", SpecInfoUtil.getScriptContent(specification));
        assertEquals("446209", SpecInfoUtil.getOwner(specification));
        assertNull(SpecInfoUtil.getDescription(specification));
        assertEquals("test", SpecInfoUtil.getDataSourceName(specification));
        assertEquals("resourceGroup", SpecInfoUtil.getResourceGroupIdentifier(specification));
        assertEquals("resourceGroupId", SpecInfoUtil.getResourceGroupId(specification));

        Map<String, String> uuidMap = new HashMap<>();
        SpecInfoUtil.resetUuid(specification, uuidMap);
        assertNotEquals("123456789", specification.getSpec().getFileResources().get(0).getId());
        assertEquals(uuidMap.get("123456789"), specification.getSpec().getFileResources().get(0).getId());
        SpecInfoUtil.replaceUuid(specification, uuidMap);
        assertEquals(uuidMap.get("123456789"), specification.getMetadata().get("uuid"));
    }

    @Test
    public void testFunctionCase() {
        Specification<DataWorksWorkflowSpec> functionSpecification = getFunctionSpecification();
        assertEquals(0, SpecInfoUtil.getInnerSpecifications(functionSpecification).size());
        assertEquals("123456789", SpecInfoUtil.getSpecRefEntity(functionSpecification).getId());
        assertEquals("123456789", SpecInfoUtil.getSpecId(functionSpecification));
        assertEquals("ODPS_FUNCTION", SpecInfoUtil.getSpecCommand(functionSpecification));
        assertEquals(Integer.valueOf(17), SpecInfoUtil.getSpecCommandTypeId(functionSpecification));
        assertEquals("test", SpecInfoUtil.getSpecName(functionSpecification));
        assertEquals("path/test", SpecInfoUtil.getSpecPath(functionSpecification));
        assertNull(SpecInfoUtil.getSpecLanguage(functionSpecification));
        assertEquals("content", SpecInfoUtil.getScriptContent(functionSpecification));
        assertEquals("446209", SpecInfoUtil.getOwner(functionSpecification));
        assertNull(SpecInfoUtil.getDescription(functionSpecification));
        assertEquals("test", SpecInfoUtil.getDataSourceName(functionSpecification));
        assertEquals("resourceGroup", SpecInfoUtil.getResourceGroupIdentifier(functionSpecification));
        assertEquals("resourceGroupId", SpecInfoUtil.getResourceGroupId(functionSpecification));

        Map<String, String> uuidMap = new HashMap<>();
        SpecInfoUtil.resetUuid(functionSpecification, uuidMap);
        assertNotEquals("123456789", functionSpecification.getSpec().getFunctions().get(0).getId());
        assertEquals(uuidMap.get("123456789"), functionSpecification.getSpec().getFunctions().get(0).getId());
        SpecInfoUtil.replaceUuid(functionSpecification, uuidMap);
        assertEquals(uuidMap.get("123456789"), functionSpecification.getMetadata().get("uuid"));
    }

    @Test
    public void testDiJobCase() {
        Specification<DataWorksWorkflowSpec> diJobSpecification = getDiJobSpecification();
        assertEquals(0, SpecInfoUtil.getInnerSpecifications(diJobSpecification).size());
        assertEquals("123456789", SpecInfoUtil.getSpecRefEntity(diJobSpecification).getId());
        assertEquals("123456789", SpecInfoUtil.getSpecId(diJobSpecification));
        assertEquals("DATA_INTEGRATION_JOB", SpecInfoUtil.getSpecCommand(diJobSpecification));
        assertNull(SpecInfoUtil.getSpecCommandTypeId(diJobSpecification));
        assertEquals("test", SpecInfoUtil.getSpecName(diJobSpecification));
        assertEquals("path/test", SpecInfoUtil.getSpecPath(diJobSpecification));
        assertNull(SpecInfoUtil.getSpecLanguage(diJobSpecification));
        assertEquals("content", SpecInfoUtil.getScriptContent(diJobSpecification));
        assertEquals("446209", SpecInfoUtil.getOwner(diJobSpecification));
        assertEquals("description", SpecInfoUtil.getDescription(diJobSpecification));
        assertNull(SpecInfoUtil.getDataSourceName(diJobSpecification));
        assertNull(SpecInfoUtil.getResourceGroupIdentifier(diJobSpecification));
        assertNull(SpecInfoUtil.getResourceGroupId(diJobSpecification));

        Map<String, String> uuidMap = new HashMap<>();
        SpecInfoUtil.resetUuid(diJobSpecification, uuidMap);
        assertNotEquals("123456789", diJobSpecification.getSpec().getDataIntegrationJobs().get(0).getId());
        assertEquals(uuidMap.get("123456789"), diJobSpecification.getSpec().getDataIntegrationJobs().get(0).getId());
        SpecInfoUtil.replaceUuid(diJobSpecification, uuidMap);
        assertEquals(uuidMap.get("123456789"), diJobSpecification.getMetadata().get("uuid"));
    }

    @Test
    public void testComponentCase() {
        Specification<DataWorksWorkflowSpec> componentSpecification = getComponentSpecification();
        assertEquals(0, SpecInfoUtil.getInnerSpecifications(componentSpecification).size());
        assertEquals("123456789", SpecInfoUtil.getSpecRefEntity(componentSpecification).getId());
        assertEquals("123456789", SpecInfoUtil.getSpecId(componentSpecification));
        assertEquals("SQL_COMPONENT", SpecInfoUtil.getSpecCommand(componentSpecification));
        assertEquals(Integer.valueOf(3010), SpecInfoUtil.getSpecCommandTypeId(componentSpecification));
        assertEquals("test", SpecInfoUtil.getSpecName(componentSpecification));
        assertEquals("path/test", SpecInfoUtil.getSpecPath(componentSpecification));
        assertEquals("SQL", SpecInfoUtil.getSpecLanguage(componentSpecification));
        assertEquals("content", SpecInfoUtil.getScriptContent(componentSpecification));
        assertEquals("446209", SpecInfoUtil.getOwner(componentSpecification));
        assertEquals("description", SpecInfoUtil.getDescription(componentSpecification));
        assertNull(SpecInfoUtil.getDataSourceName(componentSpecification));
        assertNull(SpecInfoUtil.getResourceGroupIdentifier(componentSpecification));
        assertNull(SpecInfoUtil.getResourceGroupId(componentSpecification));

        Map<String, String> uuidMap = new HashMap<>();
        SpecInfoUtil.resetUuid(componentSpecification, uuidMap);
        assertNotEquals("123456789", componentSpecification.getSpec().getComponents().get(0).getId());
        assertEquals(uuidMap.get("123456789"), componentSpecification.getSpec().getComponents().get(0).getId());
        SpecInfoUtil.replaceUuid(componentSpecification, uuidMap);
        assertEquals(uuidMap.get("123456789"), componentSpecification.getMetadata().get("uuid"));
    }

    @Test
    public void testManualFlowCase() {
        Specification<DataWorksWorkflowSpec> manualFlowSpecification = getManualFlowSpecification();
        assertEquals(1, SpecInfoUtil.getInnerSpecifications(manualFlowSpecification).size());
        assertEquals("123456789", SpecInfoUtil.getSpecRefEntity(manualFlowSpecification).getId());
        assertEquals("123456789", SpecInfoUtil.getSpecId(manualFlowSpecification));
        assertEquals("MANUAL_WORKFLOW", SpecInfoUtil.getSpecCommand(manualFlowSpecification));
        assertNull(SpecInfoUtil.getSpecCommandTypeId(manualFlowSpecification));
        assertEquals("test", SpecInfoUtil.getSpecName(manualFlowSpecification));
        assertNull(SpecInfoUtil.getSpecPath(manualFlowSpecification));
        assertNull(SpecInfoUtil.getSpecLanguage(manualFlowSpecification));
        assertNull(SpecInfoUtil.getScriptContent(manualFlowSpecification));
        assertEquals("446209", SpecInfoUtil.getOwner(manualFlowSpecification));
        assertEquals("description", SpecInfoUtil.getDescription(manualFlowSpecification));
        assertNull(SpecInfoUtil.getDataSourceName(manualFlowSpecification));
        assertNull(SpecInfoUtil.getResourceGroupIdentifier(manualFlowSpecification));
        assertNull(SpecInfoUtil.getResourceGroupId(manualFlowSpecification));

        Map<String, String> uuidMap = new HashMap<>();
        SpecInfoUtil.resetUuid(manualFlowSpecification, uuidMap);
        assertNotEquals("123456789", manualFlowSpecification.getSpec().getId());
        assertEquals(uuidMap.get("123456789"), manualFlowSpecification.getSpec().getId());
        SpecInfoUtil.replaceUuid(manualFlowSpecification, uuidMap);
        assertEquals(uuidMap.get("123456789"), manualFlowSpecification.getMetadata().get("uuid"));
    }

    @Test
    public void testGetHandlers() throws Exception {
        Field specInfoHandlerFactoryField = SpecInfoUtil.class.getDeclaredField("SPEC_INFO_HANDLER_FACTORY");
        specInfoHandlerFactoryField.setAccessible(true);
        SpecInfoHandlerFactory specInfoHandlerFactory = (SpecInfoHandlerFactory)specInfoHandlerFactoryField.get(null);
        Field handlersField = SpecInfoHandlerFactory.class.getDeclaredField("handlers");
        handlersField.setAccessible(true);
        handlersField.set(specInfoHandlerFactory, null);

        List<SpecExtractInfoHandler> handlers = specInfoHandlerFactory.getHandlers();

        assertTrue(handlers.contains(SpecInfoUtil.getHandler(NodeSpecInfoHandler.class)));
        assertTrue(handlers.contains(SpecInfoUtil.getHandler(FlowSpecInfoHandler.class)));
        assertTrue(handlers.contains(SpecInfoUtil.getHandler(ResourceSpecInfoHandler.class)));
        assertTrue(handlers.contains(SpecInfoUtil.getHandler(FunctionInfoSpecHandler.class)));
        assertTrue(handlers.contains(SpecInfoUtil.getHandler(ManualFlowSpecInfoHandler.class)));
        assertTrue(handlers.contains(SpecInfoUtil.getHandler(ComponentSpecInfoHandler.class)));
        assertTrue(handlers.contains(SpecInfoUtil.getHandler(DiJobSpecInfoHandler.class)));
    }

    @Test
    public void testGetSpecScript() {
        // Test node specification
        Specification<DataWorksWorkflowSpec> nodeSpecification = getNodeSpecification();
        SpecScript nodeScript = SpecInfoUtil.getSpecScript(nodeSpecification);
        assertEquals("path/test", nodeScript.getPath());
        assertEquals("content", nodeScript.getContent());
        assertEquals("SQL", nodeScript.getLanguage());

        // Test workflow specification
        Specification<DataWorksWorkflowSpec> workflowSpecification = getWorkflowSpecification();
        SpecScript workflowScript = SpecInfoUtil.getSpecScript(workflowSpecification);
        assertEquals("path/test", workflowScript.getPath());
        assertEquals("content", workflowScript.getContent());
        assertNull(workflowScript.getLanguage());

        // Test file resource specification
        Specification<DataWorksWorkflowSpec> fileResourceSpecification = getFileResourceSpecification();
        SpecScript fileResourceScript = SpecInfoUtil.getSpecScript(fileResourceSpecification);
        assertEquals("path/test.jar", fileResourceScript.getPath());
        assertEquals("{}", fileResourceScript.getContent());
        assertNull(fileResourceScript.getLanguage());

        // Test function specification
        Specification<DataWorksWorkflowSpec> functionSpecification = getFunctionSpecification();
        SpecScript functionScript = SpecInfoUtil.getSpecScript(functionSpecification);
        assertEquals("path/test", functionScript.getPath());
        assertEquals("content", functionScript.getContent());
        assertNull(functionScript.getLanguage());

        // Test DI job specification
        Specification<DataWorksWorkflowSpec> diJobSpecification = getDiJobSpecification();
        SpecScript diJobScript = SpecInfoUtil.getSpecScript(diJobSpecification);
        assertEquals("path/test", diJobScript.getPath());
        assertEquals("content", diJobScript.getContent());
        assertNull(diJobScript.getLanguage());

        // Test component specification
        Specification<DataWorksWorkflowSpec> componentSpecification = getComponentSpecification();
        SpecScript componentScript = SpecInfoUtil.getSpecScript(componentSpecification);
        assertEquals("path/test", componentScript.getPath());
        assertEquals("content", componentScript.getContent());
        assertEquals("SQL", componentScript.getLanguage());

        // Test manual flow specification
        Specification<DataWorksWorkflowSpec> manualFlowSpecification = getManualFlowSpecification();
        SpecScript manualFlowScript = SpecInfoUtil.getSpecScript(manualFlowSpecification);
        assertNull(manualFlowScript);
    }

    private Specification<DataWorksWorkflowSpec> getDiJobSpecification() {
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("uuid", "123456789");
        specification.setMetadata(metadata);
        specification.setKind(SpecKind.DATA_INTEGRATION_JOB.getLabel());
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        specification.setSpec(spec);

        SpecDataIntegrationJob specDataIntegrationJob = new SpecDataIntegrationJob();
        specDataIntegrationJob.setId("123456789");
        specDataIntegrationJob.setName("test");
        specDataIntegrationJob.setDescription("description");
        specDataIntegrationJob.setOwner("446209");

        SpecScript script = new SpecScript();
        script.setPath("path/test");
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand("DATA_INTEGRATION_JOB");
        script.setRuntime(runtime);
        script.setContent("content");
        specDataIntegrationJob.setScript(script);

        spec.setDataIntegrationJobs(Collections.singletonList(specDataIntegrationJob));
        return specification;
    }

    private Specification<DataWorksWorkflowSpec> getComponentSpecification() {
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("uuid", "123456789");
        specification.setMetadata(metadata);
        specification.setKind(SpecKind.CYCLE_WORKFLOW.getLabel());
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        specification.setSpec(spec);

        SpecComponent specComponent = new SpecComponent();
        specComponent.setId("123456789");
        specComponent.setName("test");
        specComponent.setDescription("description");
        specComponent.setOwner("446209");

        SpecScript script = new SpecScript();
        script.setPath("path/test");
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand(CodeProgramType.SQL_COMPONENT.name());
        script.setRuntime(runtime);
        script.setContent("content");
        script.setLanguage("SQL");
        specComponent.setScript(script);

        SpecComponentParameter specComponentParameter = new SpecComponentParameter();
        specComponentParameter.setName("parameterTest");
        specComponentParameter.setType("string");
        specComponent.setInputs(Collections.singletonList(specComponentParameter));
        specComponent.setOutputs(Collections.singletonList(specComponentParameter));

        spec.setComponents(Collections.singletonList(specComponent));
        return specification;
    }

    private Specification<DataWorksWorkflowSpec> getFunctionSpecification() {
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("uuid", "123456789");
        specification.setMetadata(metadata);
        specification.setKind(SpecKind.CYCLE_WORKFLOW.getLabel());
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        specification.setSpec(spec);

        SpecFunction specFunction = new SpecFunction();
        specFunction.setId("123456789");
        specFunction.setName("test");

        SpecScript script = new SpecScript();
        script.setPath("path/test");
        script.setContent("content");
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand(CodeProgramType.ODPS_FUNCTION.getName());
        script.setRuntime(runtime);
        specFunction.setScript(script);

        SpecDatasource specDatasource = new SpecDatasource();
        specDatasource.setName("test");
        specFunction.setDatasource(specDatasource);

        SpecRuntimeResource runtimeResource = new SpecRuntimeResource();
        runtimeResource.setResourceGroup("resourceGroup");
        runtimeResource.setResourceGroupId("resourceGroupId");
        specFunction.setRuntimeResource(runtimeResource);

        specFunction.setMetadata(new HashMap<>());
        specFunction.getMetadata().put("owner", "446209");

        spec.setFunctions(Collections.singletonList(specFunction));

        return specification;
    }

    private Specification<DataWorksWorkflowSpec> getFileResourceSpecification() {
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("uuid", "123456789");
        specification.setMetadata(metadata);
        specification.setKind(SpecKind.CYCLE_WORKFLOW.getLabel());
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        specification.setSpec(spec);

        SpecFileResource specFileResource = new SpecFileResource();
        specFileResource.setId("123456789");
        specFileResource.setName("test.jar");

        SpecScript script = new SpecScript();
        script.setPath("path/test.jar");
        script.setContent("{}");
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand(CodeProgramType.ODPS_JAR.getName());
        script.setRuntime(runtime);
        specFileResource.setScript(script);

        SpecDatasource specDatasource = new SpecDatasource();
        specDatasource.setName("test");
        specFileResource.setDatasource(specDatasource);

        SpecRuntimeResource runtimeResource = new SpecRuntimeResource();
        runtimeResource.setResourceGroup("resourceGroup");
        runtimeResource.setResourceGroupId("resourceGroupId");
        specFileResource.setRuntimeResource(runtimeResource);

        specFileResource.setMetadata(new HashMap<>());
        specFileResource.getMetadata().put("owner", "446209");

        spec.setFileResources(Collections.singletonList(specFileResource));
        return specification;
    }

    private Specification<DataWorksWorkflowSpec> getWorkflowSpecification() {
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        specification.setKind(SpecKind.TRIGGER_WORKFLOW.getLabel());
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        specification.setSpec(spec);

        SpecWorkflow specWorkflow = new SpecWorkflow();
        specWorkflow.setId("123456789");
        specWorkflow.setName("test");
        specWorkflow.setDescription("description");
        specWorkflow.setOwner("446209");

        SpecNode specNode = new SpecNode();
        specNode.setId("123456789");
        specNode.setName("test");
        specNode.setDescription("description");
        specNode.setOwner("446209");

        SpecScript nodeScript = new SpecScript();
        nodeScript.setPath("path/test");
        nodeScript.setContent("content");
        SpecScriptRuntime nodeRuntime = new SpecScriptRuntime();
        nodeRuntime.setCommand(CodeProgramType.ODPS_SQL.getName());
        nodeScript.setRuntime(nodeRuntime);
        SpecVariable specVariable = new SpecVariable();
        specVariable.setName("test");
        specVariable.setValue("test");
        nodeScript.setParameters(Collections.singletonList(specVariable));
        specNode.setScript(nodeScript);

        SpecRuntimeResource specRuntimeResource = new SpecRuntimeResource();
        specRuntimeResource.setResourceGroup("resourceGroup");
        SpecDatasource specDatasource = new SpecDatasource();
        specDatasource.setId("111");
        specNode.setRuntimeResource(specRuntimeResource);
        specNode.setDatasource(specDatasource);

        SpecNodeOutput specNodeOutput = new SpecNodeOutput();
        specNodeOutput.setData("123456789");
        specNodeOutput.setIsDefault(true);
        specNodeOutput.setRefTableName("test");
        specNode.setOutputs(Collections.singletonList(specNodeOutput));
        specWorkflow.setNodes(Collections.singletonList(specNode));

        SpecScript script = new SpecScript();
        script.setPath("path/test");
        script.setContent("content");
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand(CodeProgramType.WORKFLOW.name());
        script.setRuntime(runtime);
        specWorkflow.setScript(script);

        SpecNodeOutput output = new SpecNodeOutput();
        output.setData("123456789");
        output.setIsDefault(true);
        output.setRefTableName("test");
        specWorkflow.setOutputs(Collections.singletonList(output));

        SpecFlowDepend specFlowDepend = new SpecFlowDepend();
        SpecDepend specDepend = new SpecDepend();
        SpecNode depend = new SpecNode();
        depend.setId("123456780");
        specDepend.setNodeId(depend);
        SpecNode nodeId = new SpecNode();
        nodeId.setId("123456789");
        specFlowDepend.setNodeId(nodeId);
        specFlowDepend.setDepends(Collections.singletonList(specDepend));
        spec.setFlow(Collections.singletonList(specFlowDepend));

        specWorkflow.setDependencies(Collections.singletonList(specFlowDepend));

        spec.setWorkflows(Collections.singletonList(specWorkflow));

        return specification;
    }

    private Specification<DataWorksWorkflowSpec> getNodeSpecification() {
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("uuid", "123456789");
        metadata.put("containerId", "987654321");
        specification.setMetadata(metadata);
        specification.setKind(SpecKind.CYCLE_WORKFLOW.getLabel());
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        specification.setSpec(spec);

        SpecNode specNode = new SpecNode();
        specNode.setId("123456789");
        specNode.setName("test");
        specNode.setDescription("description");
        specNode.setOwner("446209");

        SpecScript script = new SpecScript();
        script.setPath("path/test");
        script.setContent("content");
        script.setLanguage("SQL");
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand(CodeProgramType.ODPS_SQL.getName());
        script.setRuntime(runtime);
        SpecVariable specVariable = new SpecVariable();
        specVariable.setName("test");
        specVariable.setValue("test");
        script.setParameters(Collections.singletonList(specVariable));
        specNode.setScript(script);

        SpecRuntimeResource specRuntimeResource = new SpecRuntimeResource();
        specRuntimeResource.setResourceGroup("resourceGroup");
        specRuntimeResource.setResourceGroupId("resourceGroupId");
        SpecDatasource specDatasource = new SpecDatasource();
        specDatasource.setId("111");
        specDatasource.setName("test");
        specNode.setRuntimeResource(specRuntimeResource);
        specNode.setDatasource(specDatasource);

        SpecNodeOutput specNodeOutput = new SpecNodeOutput();
        specNodeOutput.setData("123456789");
        specNodeOutput.setIsDefault(true);
        specNodeOutput.setRefTableName("test");
        specNode.setOutputs(Collections.singletonList(specNodeOutput));

        SpecFlowDepend specFlowDepend = new SpecFlowDepend();
        SpecDepend specDepend = new SpecDepend();
        SpecNode nodeId = new SpecNode();
        nodeId.setId("123456780");
        specDepend.setNodeId(nodeId);
        specFlowDepend.setNodeId(specNode);
        specFlowDepend.setDepends(Collections.singletonList(specDepend));
        spec.setFlow(Lists.newArrayList(specFlowDepend, specFlowDepend));
        spec.setNodes(Collections.singletonList(specNode));
        return specification;
    }

    private Specification<DataWorksWorkflowSpec> getDoWhileSpec() {
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        specification.setKind(SpecKind.CYCLE_WORKFLOW.getLabel());
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        specification.setSpec(spec);

        SpecNode specNode = new SpecNode();
        specNode.setId("123456789");
        specNode.setName("test");
        specNode.setDescription("description");
        specNode.setOwner("446209");

        SpecScript script = new SpecScript();
        script.setPath("path/test");
        SpecScriptRuntime runtime = new SpecScriptRuntime();
        runtime.setCommand(CodeProgramType.ODPS_SQL.getName());
        script.setRuntime(runtime);
        specNode.setScript(script);

        SpecFlowDepend specFlowDepend = new SpecFlowDepend();
        SpecDepend specDepend = new SpecDepend();
        SpecNode nodeId = new SpecNode();
        nodeId.setId("123456780");
        specDepend.setNodeId(nodeId);
        specFlowDepend.setNodeId(specNode);
        specFlowDepend.setDepends(Lists.newArrayList(specDepend, specDepend));

        spec.setFlow(Collections.singletonList(specFlowDepend));
        spec.setNodes(Collections.singletonList(specNode));

        SpecNode doWhileStart = new SpecNode();
        doWhileStart.setId("112233");
        doWhileStart.setName("doWhileStart");

        SpecScript doWhileStartScript = new SpecScript();
        SpecScriptRuntime doWhileStartRuntime = new SpecScriptRuntime();
        doWhileStartRuntime.setCommand(CodeProgramType.ODPS_SQL.getName());
        doWhileStartScript.setRuntime(doWhileStartRuntime);
        doWhileStart.setScript(doWhileStartScript);

        SpecNode doWhileEnd = new SpecNode();
        doWhileEnd.setId("445566");
        doWhileEnd.setName("doWhileEnd");

        SpecScript doWhileEndScript = new SpecScript();
        SpecScriptRuntime doWhileEndRuntime = new SpecScriptRuntime();
        doWhileEndRuntime.setCommand(CodeProgramType.CONTROLLER_CYCLE_END.getName());
        doWhileEndScript.setRuntime(doWhileEndRuntime);
        doWhileEnd.setScript(doWhileEndScript);

        SpecDoWhile doWhile = new SpecDoWhile();
        specNode.setDoWhile(doWhile);
        doWhile.setSpecWhile(doWhileEnd);
        doWhile.setNodes(Collections.singletonList(doWhileStart));

        SpecFlowDepend specFlowDependInner = new SpecFlowDepend();
        specFlowDependInner.setNodeId(doWhileEnd);
        SpecDepend specDependInner = new SpecDepend();
        specDependInner.setNodeId(doWhileStart);
        specFlowDependInner.setDepends(Collections.singletonList(specDependInner));
        doWhile.setFlow(Lists.newArrayList(specFlowDependInner, specFlowDependInner));

        return specification;
    }

    private Specification<DataWorksWorkflowSpec> getManualFlowSpecification() {
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("uuid", "123456789");
        specification.setMetadata(metadata);
        specification.setKind(SpecKind.MANUAL_WORKFLOW.getLabel());
        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        specification.setSpec(spec);

        spec.setId("123456789");
        spec.setName("test");
        spec.setOwner("446209");
        spec.setDescription("description");

        SpecNode specNode = new SpecNode();
        spec.setNodes(Collections.singletonList(specNode));
        specNode.setId("123456789");

        SpecFlowDepend specFlowDepend = new SpecFlowDepend();
        specFlowDepend.setNodeId(specNode);
        ArrayList<SpecDepend> depends = new ArrayList<>();
        SpecDepend specDepend = new SpecDepend();
        SpecNodeOutput output = new SpecNodeOutput();
        output.setData("data");
        specDepend.setOutput(output);
        depends.add(specDepend);
        specFlowDepend.setDepends(depends);
        spec.setFlow(Lists.newArrayList(specFlowDepend, specFlowDepend));

        return specification;
    }
}
