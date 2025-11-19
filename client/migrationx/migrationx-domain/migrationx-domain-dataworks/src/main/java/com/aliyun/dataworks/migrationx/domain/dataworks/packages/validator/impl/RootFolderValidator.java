package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.fastjson2.JSON;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.AbstractFolderValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;

/**
 * Root Directory Validator Class, used to validate whether the root directory structure meets requirements
 * <p>
 * Root directory structure requirements:
 * 1. Allowed directories:
 * - DataStudio
 * - DataQuality
 * - DataService
 * - DataSource
 * 2. Allowed files:
 * - SPEC.FORMAT
 * - mapping.json
 * 3. Required files:
 * - SPEC.FORMAT
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class RootFolderValidator extends AbstractFolderValidator {

    public static final String DATA_STUDIO_FILE_NAME = "DataStudio";
    public static final String DATA_QUALITY_FILE_NAME = "DataQuality";
    public static final String DATA_SERVICE_FILE_NAME = "DataService";
    public static final String DATA_SOURCE_FILE_NAME = "DataSource";
    public static final String SPEC_FORMAT_FILE_NAME = "SPEC.FORMAT";
    public static final String MAPPING_JSON_FILE_NAME = "mapping.json";

    private static final RootFolderValidator INSTANCE = new RootFolderValidator();

    private final Set<String> allowedDirectories;

    private final Set<String> allowedFiles;

    private final Set<String> requiredFiles;

    protected RootFolderValidator() {
        allowedDirectories = new HashSet<>();
        allowedFiles = new HashSet<>();
        requiredFiles = new HashSet<>();

        allowedDirectories.add(DATA_STUDIO_FILE_NAME);
        allowedDirectories.add(DATA_QUALITY_FILE_NAME);
        allowedDirectories.add(DATA_SERVICE_FILE_NAME);
        allowedDirectories.add(DATA_SOURCE_FILE_NAME);

        allowedFiles.add(SPEC_FORMAT_FILE_NAME);
        allowedFiles.add(MAPPING_JSON_FILE_NAME);

        requiredFiles.add(SPEC_FORMAT_FILE_NAME);
    }

    public static RootFolderValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
        if (allowedDirectories.contains(file.getName())) {
            return PackageValidatorResult.success();
        }
        // Return specific error code for folder not allowed
        return PackageValidatorResult.failed(PackageValidateErrorCode.ONLY_SPEC_FOLDER_ALLOWED_IN_FOLDER, context,
            "folderType", "root",
            "folderName", file.getName(),
            "allowedFolders", JSON.toJSONString(allowedDirectories));
    }

    @Override
    protected PackageValidatorResult isAllowedFiles(File file, ValidateContext context) {
        if (allowedFiles.contains(file.getName())) {
            return PackageValidatorResult.success();
        }
        // Return specific error code for file not allowed
        return PackageValidatorResult.failed(PackageValidateErrorCode.ONLY_SPECIFIC_FILE_ALLOWED_IN_FOLDER, context,
            "folderType", "root",
            "fileName", file.getName(),
            "allowedFiles", JSON.toJSONString(allowedFiles));
    }

    @Override
    protected Set<String> getRequiredFolders(ValidateContext context) {
        return Collections.emptySet();
    }

    @Override
    protected Set<String> getRequiredFiles(ValidateContext context) {
        return requiredFiles;
    }

    @Override
    public List<PackageValidator> getChildValidators(File file, ValidateContext context) {
        // If it's a DataStudio folder, use DataStudioFolderValidator for validation
        if (file.isDirectory() && DATA_STUDIO_FILE_NAME.equalsIgnoreCase(file.getName())) {
            return Collections.singletonList(DataStudioFolderValidator.getInstance());
        }

        // Otherwise use the parent class's default implementation
        return super.getChildValidators(file, context);
    }
}