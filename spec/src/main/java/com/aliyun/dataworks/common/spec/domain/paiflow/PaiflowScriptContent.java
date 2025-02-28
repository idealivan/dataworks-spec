package com.aliyun.dataworks.common.spec.domain.paiflow;

import java.util.Map;

import com.alibaba.fastjson2.annotation.JSONField;

import lombok.Data;

/**
 * @author 戒迷
 * @date 2025/2/6
 */
@Data
public class PaiflowScriptContent {
    private String paiflowPipelineId;
    @JSONField(serializeUsing = PaiflowObjectToStringSerializer.class)
    private PaiflowSpec paiflowPipeline;
    @JSONField(serializeUsing = PaiflowObjectToStringSerializer.class)
    private PaiflowArgumentsWrapper paiflowArguments;
    @JSONField(serialize = false)
    private String projectId;
    private String paraValue;
    private String connectionType;
    private Map<String, Object> computeResource;

    /**
     * 序列化给调度用
     * @return app_id
     */
    public String getAppId() {
        return this.projectId;
    }

    /**
     * 序列化给调度用
     * @return workspace_id
     */
    public String getWorkspaceId() {
        return this.projectId;
    }
}
