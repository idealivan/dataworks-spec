package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.AbstractFileValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;

/**
 * Empty File Validator Class
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class EmptyFileValidator extends AbstractFileValidator {

    private static final EmptyFileValidator INSTANCE = new EmptyFileValidator();

    public static EmptyFileValidator getInstance() {
        return INSTANCE;
    }

    protected EmptyFileValidator() {

    }

    @Override
    protected void doValidate(File file, ValidateContext context) {
        // Empty file validator does not execute any validation logic
    }
}
