package com.aliyun.dataworks.common.spec.domain.noref;

import java.util.List;

import com.aliyun.dataworks.common.spec.domain.SpecNoRefEntity;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Paiflow Spec
 *
 * @author 戒迷
 * @date 2025/1/10
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SpecPaiflow extends SpecNoRefEntity {
    private List<SpecNode> nodes;
    private List<SpecFlowDepend> flow;
}
