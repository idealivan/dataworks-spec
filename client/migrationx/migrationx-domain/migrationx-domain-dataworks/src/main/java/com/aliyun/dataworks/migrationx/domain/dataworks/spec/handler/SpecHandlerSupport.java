package com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-08-13
 */
public interface SpecHandlerSupport {

    /**
     * Check whether the handler supports the specification
     *
     * @param specification specification
     * @return is or not support
     */
    boolean support(Specification<DataWorksWorkflowSpec> specification);
}
