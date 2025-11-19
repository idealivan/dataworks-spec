package com.aliyun.dataworks.migrationx.domain.dataworks.utils;

import java.io.File;
import java.util.List;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.ValidateMode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl.DataStudioFolderValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl.RootFolderValidator;

/**
 * SPEC Package Validation Utility Class
 * Provides simple entry methods to validate whether the SPEC package structure meets requirements
 *
 * @author 莫泣
 * @date 2025-08-29
 */
public class SpecPackageValidateUtil {

    /**
     * Validate SPEC package structure
     *
     * @param packagePath SPEC package path
     * @return Validation result list
     */
    public static List<PackageValidatorResult> validate(String packagePath) {
        ValidateContext context = new ValidateContext();
        context.setValidateMode(ValidateMode.NORMAL);
        doValidate(packagePath, RootFolderValidator.getInstance(), context);
        return context.getResults();
    }

    /**
     * Pre-check SPEC package structure
     *
     * @param packagePath SPEC package path
     * @return Validation result list
     */
    public static List<PackageValidatorResult> validatePreCheck(String packagePath) {
        ValidateContext context = new ValidateContext();
        context.setValidateMode(ValidateMode.PRE_VALIDATE);
        doValidate(packagePath, DataStudioFolderValidator.getInstance(), context);
        return context.getResults();
    }

    private static void doValidate(String packagePath, PackageValidator validator, ValidateContext context) {
        // Parameter validation
        if (packagePath == null || packagePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Package path cannot be empty");
        }

        File packageFile = new File(packagePath);
        if (!packageFile.exists()) {
            throw new IllegalArgumentException("Specified package path does not exist: " + packagePath);
        }

        if (!packageFile.isDirectory()) {
            throw new IllegalArgumentException("Specified path is not a directory: " + packagePath);
        }
        validator.validate(packageFile, context);
    }

    /**
     * Private constructor to prevent instantiation
     */
    private SpecPackageValidateUtil() {
        // Prevent instantiation
    }
}