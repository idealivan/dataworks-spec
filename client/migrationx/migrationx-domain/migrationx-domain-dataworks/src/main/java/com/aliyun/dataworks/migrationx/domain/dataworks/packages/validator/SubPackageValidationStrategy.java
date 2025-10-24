package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator;

import java.io.File;

/**
 * Subpackage Validation Strategy Interface
 * Used to define special validation rules for different parent package types on their subpackages
 *
 * @author 莫泣
 * @date 2025-08-27
 */
public interface SubPackageValidationStrategy {

    /**
     * Check if applicable to the given parent package type
     *
     * @param parentPackageType parent package type
     * @return whether applicable
     */
    boolean support(String parentPackageType);

    /**
     * Validate if subpackages meet requirements
     *
     * @param packageDir parent package directory
     * @param context    validation context
     */
    void validateSubPackages(File packageDir, ValidateContext context);
}