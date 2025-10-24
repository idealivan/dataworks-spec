package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.strategy;

import java.io.File;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.SubPackageValidationStrategy;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl.SpecPackageValidator;
import org.apache.commons.lang3.StringUtils;

/**
 * Subpackage Validation Strategy Abstract Base Class
 *
 * @author 莫泣
 * @date 2025-08-28
 */
public abstract class AbstractSubPackageValidationStrategy implements SubPackageValidationStrategy {

    public static final String DATAWORKS_FILE_NAME = ".dataworks";

    /**
     * Check if it's a package of specified type
     *
     * @param packageDir   package directory
     * @param expectedType expected type
     * @param context      validation context
     * @return whether it's a package of specified type
     */
    protected boolean isSpecificTypePackage(File packageDir, String expectedType, ValidateContext context) {
        String packageType = SpecPackageValidator.getInstance().getPackageType(packageDir, context);
        return StringUtils.equals(expectedType, packageType);
    }
}