package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.util.Collections;
import java.util.Set;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.AbstractFolderValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;

/**
 * Empty Validator Class, used for directories that don't require special validation
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class EmptyFolderValidator extends AbstractFolderValidator {

    private static final EmptyFolderValidator INSTANCE = new EmptyFolderValidator();

    protected EmptyFolderValidator() {

    }

    public static EmptyFolderValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
        // Allow all directories
        return PackageValidatorResult.success();
    }

    @Override
    protected PackageValidatorResult isAllowedFiles(File file, ValidateContext context) {
        // Allow all files
        return PackageValidatorResult.success();
    }

    @Override
    protected Set<String> getRequiredFolders(ValidateContext context) {
        return Collections.emptySet();
    }

    @Override
    protected Set<String> getRequiredFiles(ValidateContext context) {
        return Collections.emptySet();
    }
}