package com.aliyun.dataworks.common.spec.domain.paiflow;

import java.util.Map;

import lombok.Data;

/**
 * @author 戒迷
 * @date 2025/1/10
 * @see <a href="https://aliyuque.antfin.com/pai-user/manual/za5wbknh158dqm5s" />
 */
@Data
public class PaiflowArtifact extends AbstractPaiflowBase {
    private String name;
    private String from;
    private Map<String, Object> metadata;
    private Object value;
    private String desc;
    private Boolean repeated;
    private Boolean required;
}
