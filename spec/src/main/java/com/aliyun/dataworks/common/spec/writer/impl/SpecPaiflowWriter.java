package com.aliyun.dataworks.common.spec.writer.impl;

import com.alibaba.fastjson2.JSONObject;

import com.aliyun.dataworks.common.spec.annotation.SpecWriter;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.noref.SpecPaiflow;
import com.aliyun.dataworks.common.spec.writer.SpecWriterContext;
import com.aliyun.dataworks.common.spec.writer.WriterFactory;

/**
 * PaiflowWriter
 *
 * @author 戒迷
 * @date 2025/2/11
 */
@SpecWriter
public class SpecPaiflowWriter extends DefaultJsonObjectWriter<SpecPaiflow> {
    public SpecPaiflowWriter(SpecWriterContext context) {
        super(context);
    }

    @Override
    public JSONObject write(SpecPaiflow specObj, SpecWriterContext context) {
        DataWorksWorkflowSpec constructedSpec = new DataWorksWorkflowSpec();
        constructedSpec.setNodes(specObj.getNodes());
        constructedSpec.setFlow(specObj.getFlow());

        @SuppressWarnings("unchecked")
        DefaultJsonObjectWriter<DataWorksWorkflowSpec> writer =
            (DefaultJsonObjectWriter<DataWorksWorkflowSpec>)WriterFactory.getWriter(DataWorksWorkflowSpec.class, context);
        return writer.write(constructedSpec, context);
    }
}
