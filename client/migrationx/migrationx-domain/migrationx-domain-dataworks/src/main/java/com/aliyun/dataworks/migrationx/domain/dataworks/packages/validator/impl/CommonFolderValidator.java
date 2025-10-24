package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.AbstractFolderValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;

/**
 * 通用文件夹验证器类，用于验证通用文件夹中的内容是否符合要求
 * <p>
 * 通用文件夹结构要求：
 * 1. 允许所有非隐藏目录
 * 2. 不允许任何文件
 * 3. 不需要必需的文件和文件夹
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class CommonFolderValidator extends AbstractFolderValidator {

    private static final CommonFolderValidator INSTANCE = new CommonFolderValidator();

    protected CommonFolderValidator() {

    }

    public static CommonFolderValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
        if (!isHidden(file)) {
            return PackageValidatorResult.success();
        }
        // Return specific error code for hidden folder not allowed
        return PackageValidatorResult.failed(PackageValidateErrorCode.HIDDEN_FOLDER_NOT_ALLOW_IN_FOLDER, context,
            "folderType", "common",
            "folderName", file.getName());
    }

    @Override
    protected PackageValidatorResult isAllowedFiles(File file, ValidateContext context) {
        // Files are never allowed, so we always return an error code
        return PackageValidatorResult.failed(PackageValidateErrorCode.ANY_FILE_NOT_ALLOWED_IN_FOLDER, context,
            "folderType", "common",
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
        // 如果是包目录，使用SpecPackageValidator进行验证
        if (file.isDirectory()) {
            if (isPackage(file)) {
                return Collections.singletonList(SpecPackageValidator.getInstance());
            } else {
                return Collections.singletonList(CommonFolderValidator.getInstance());
            }
        }
        return super.getChildValidators(file, context);
    }
}
