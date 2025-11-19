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
 * Snapshot Folder Validator Class, used to validate whether the content in the snapshot folder meets requirements
 * <p>
 * Snapshot folder structure requirements:
 * 1. Allowed subfolders:
 * - saved
 * - committed
 * 2. No files are allowed
 * 3. No required files or folders
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class SnapshotFolderValidator extends AbstractFolderValidator {

    public static final String SAVED_FILE_NAME = "saved";
    public static final String COMMITTED_FILE_NAME = "committed";

    private static final SnapshotFolderValidator INSTANCE = new SnapshotFolderValidator();

    private final Set<String> allowedFolders;

    protected SnapshotFolderValidator() {
        this.allowedFolders = new HashSet<>();
        allowedFolders.add(SAVED_FILE_NAME);
        allowedFolders.add(COMMITTED_FILE_NAME);
    }

    public static SnapshotFolderValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
        if (allowedFolders.contains(file.getName())) {
            return PackageValidatorResult.success();
        }
        // Return specific error code for folder not allowed
        return PackageValidatorResult.failed(PackageValidateErrorCode.ONLY_SPEC_FOLDER_ALLOWED_IN_FOLDER, context,
            "folderType", "snapshots",
            "folderName", file.getName(),
            "allowedFolders", JSON.toJSONString(allowedFolders));
    }

    @Override
    protected PackageValidatorResult isAllowedFiles(File file, ValidateContext context) {
        // Files are never allowed, so we always return an error code
        return PackageValidatorResult.failed(PackageValidateErrorCode.ANY_FILE_NOT_ALLOWED_IN_FOLDER, context,
            "folderType", "snapshots",
            "fileName", file.getName());
    }

    @Override
    protected Set<String> getRequiredFolders(ValidateContext context) {
        return Collections.emptySet();
    }

    @Override
    protected Set<String> getRequiredFiles(ValidateContext context) {
        return Collections.emptySet();
    }

    @Override
    public List<PackageValidator> getChildValidators(File file, ValidateContext context) {
        return super.getChildValidators(file, context);
    }
}
