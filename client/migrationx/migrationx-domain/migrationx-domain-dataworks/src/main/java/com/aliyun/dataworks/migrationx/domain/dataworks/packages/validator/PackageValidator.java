package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator;

import java.io.File;

/**
 * Package Validator Interface
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public interface PackageValidator {

    void validate(File file, ValidateContext context);

    default void validate(String path, ValidateContext context) {
        validate(new File(path), context);
    }
}