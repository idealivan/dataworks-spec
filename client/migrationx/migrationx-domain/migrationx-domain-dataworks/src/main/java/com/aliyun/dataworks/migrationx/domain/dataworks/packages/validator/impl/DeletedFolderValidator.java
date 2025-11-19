package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.AbstractFolderValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;
import org.apache.commons.lang3.StringUtils;

/**
 * Deleted Folder Validator Class, used to validate whether the content in the deleted folder meets requirements
 * <p>
 * Deleted folder structure requirements:
 * 1. No subfolders are allowed
 * 2. Only deleted.json file is allowed
 * 3. Required files:
 * - deleted.json
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class DeletedFolderValidator extends AbstractFolderValidator {

    public static final String DELETED_FILE_NAME = "deleted.json";

    private static final DeletedFolderValidator INSTANCE = new DeletedFolderValidator();

    protected DeletedFolderValidator() {

    }

    public static DeletedFolderValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
        // Subfolders are not allowed in deleted folder
        return PackageValidatorResult.failed(PackageValidateErrorCode.HIDDEN_FOLDER_NOT_ALLOW_IN_FOLDER, context,
            "folderType", "deleted",
            "folderName", file.getName());
    }

    @Override
    protected PackageValidatorResult isAllowedFiles(File file, ValidateContext context) {
        boolean isAllowed = Optional.ofNullable(file)
            .map(File::getName)
            .filter(name -> StringUtils.equals(DELETED_FILE_NAME, name))
            .isPresent();
        if (isAllowed) {
            return PackageValidatorResult.success();
        }
        // Return specific error code for file not allowed
        return PackageValidatorResult.failed(PackageValidateErrorCode.ANY_FILE_NOT_ALLOWED_IN_FOLDER, context,
            "folderType", "deleted",
            "fileName", file.getName());
    }

    @Override
    protected Set<String> getRequiredFolders(ValidateContext context) {
        return Collections.emptySet();
    }

    @Override
    protected Set<String> getRequiredFiles(ValidateContext context) {
        return Collections.singleton(DELETED_FILE_NAME);
    }

    @Override
    public List<PackageValidator> getChildValidators(File file, ValidateContext context) {
        return super.getChildValidators(file, context);
    }
}
