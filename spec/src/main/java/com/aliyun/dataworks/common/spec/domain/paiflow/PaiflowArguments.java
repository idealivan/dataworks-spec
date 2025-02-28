package com.aliyun.dataworks.common.spec.domain.paiflow;

import java.util.List;

import lombok.Data;

/**
 * @author 戒迷
 * @date 2025/1/10
 * @see <a href="https://aliyuque.antfin.com/pai-user/manual/za5wbknh158dqm5s" />
 */
@Data
public class PaiflowArguments extends AbstractPaiflowBase {
    private List<PaiflowArtifact> artifacts;
    private List<PaiflowParameter> parameters;
}
