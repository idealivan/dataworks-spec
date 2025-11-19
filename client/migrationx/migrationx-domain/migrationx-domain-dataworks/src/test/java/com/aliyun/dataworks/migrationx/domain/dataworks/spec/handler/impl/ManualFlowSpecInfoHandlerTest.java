package com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.impl;

import java.util.Collections;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.ref.SpecDatasource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNodeOutput;
import com.aliyun.dataworks.common.spec.domain.ref.SpecRuntimeResource;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.SpecWorkflow;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-08-20
 */
public class ManualFlowSpecInfoHandlerTest {

    @Test
    public void testWorkflowCase() {
        Specification<DataWorksWorkflowSpec> workflowSpecification = getWorkflowSpecification();
        ManualFlowSpecInfoHandler manualFlowSpecInfoHandler = new ManualFlowSpecInfoHandler();
        assertEquals("123456789", manualFlowSpecInfoHandler.getSpecId(workflowSpecification));
        assertEquals("446209", manualFlowSpecInfoHandler.getOwner(workflowSpecification));
        assertEquals("description", manualFlowSpecInfoHandler.getDescription(workflowSpecification));
    }

    private Specification<DataWorksWorkflowSpec> getWorkflowSpecification() {
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        specification.setKind(SpecKind.CYCLE_WORKFLOW.getLabel());
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
}
