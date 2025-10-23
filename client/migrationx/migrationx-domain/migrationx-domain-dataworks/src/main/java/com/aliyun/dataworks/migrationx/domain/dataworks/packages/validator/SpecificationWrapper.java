package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator;

import javax.validation.constraints.NotNull;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-09-24
 */
@Getter
@AllArgsConstructor
public class SpecificationWrapper {

    @NotNull
    private final Specification<DataWorksWorkflowSpec> specification;

    @NotNull
    private final String spec;
}
