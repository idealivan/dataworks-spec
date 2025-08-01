package com.aliyun.dataworks.common.spec.domain.ref;

import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Data Integration Job
 *
 * @author 世嘉
 * @date 2025/7/11
 */
@Data
@ToString
@EqualsAndHashCode(callSuper = true)
public class SpecDataIntegrationJob extends SpecRefEntity {
    private String name;
    private SpecScript script;
    private String owner;
    private String description;
}
