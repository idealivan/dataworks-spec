package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;

/**
 * DataWorks Folder Validator Class, used to validate whether the content in the DataWorks folder meets requirements
 * <p>
 * DataWorks folder structure requirements:
 * 1. Allowed subfolders:
 * - .snapshots
 * - .deleted
 * 2. Allow all non-hidden directories
 * 3. No files are allowed
 * 4. No required files or folders
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class DataWorksFolderValidator extends CommonFolderValidator {

    public static final String SNAPSHOT_FILE_NAME = ".snapshots";
    public static final String DELETED_FILE_NAME = ".deleted";

    private static final DataWorksFolderValidator INSTANCE = new DataWorksFolderValidator();

    private final Set<String> allowedHiddenFolders;

    protected DataWorksFolderValidator() {
        allowedHiddenFolders = new HashSet<>();
        allowedHiddenFolders.add(SNAPSHOT_FILE_NAME);
        allowedHiddenFolders.add(DELETED_FILE_NAME);
    }

    public static DataWorksFolderValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
        // First check with parent class (CommonFolderValidator)
        PackageValidatorResult parentResult = super.isAllowedFolders(file, context);
        // If parent allows it (returns success()), or it's one of our allowed hidden folders, then it's allowed
        if (parentResult == null || parentResult.getValid() || allowedHiddenFolders.contains(file.getName())) {
            return PackageValidatorResult.success();
        }

        // Return specific error code for folder not allowed
        return PackageValidatorResult.failed(PackageValidateErrorCode.HIDDEN_FOLDER_NOT_ALLOWED_IN_DATAWORKS_FOLDER, context,
            "folderName", file.getName());
    }

    @Override
    public List<PackageValidator> getChildValidators(File file, ValidateContext context) {
        // Select appropriate validator based on folder name
        if (file.isDirectory()) {
            String name = file.getName();
            if (DELETED_FILE_NAME.equalsIgnoreCase(name)) {
                return Collections.singletonList(DeletedFolderValidator.getInstance());
            } else if (SNAPSHOT_FILE_NAME.equalsIgnoreCase(name)) {
                return Collections.singletonList(SnapshotFolderValidator.getInstance());
            }
        }

        // Otherwise use the parent class's default implementation
        return super.getChildValidators(file, context);
    }
}
