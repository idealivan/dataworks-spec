package com.aliyun.dataworks.common.spec.domain.paiflow;

import java.beans.Transient;

import lombok.Data;

/**
 * @author 戒迷
 * @date 2025/1/10
 * @see <a href="https://aliyuque.antfin.com/pai-user/manual/za5wbknh158dqm5s" />
 */
@Data
public class PaiflowParameter extends AbstractPaiflowBase {
    private String name;
    private String from;
    private String type;
    private Object value;
    private String desc;
    private String feasible;

    @Transient
    public String getFromParameterName() {
        if (null == from) {
            return null;
        }

        return from.replaceAll("\\{\\{inputs.parameters.", "").replace("}}", "");
    }
}
