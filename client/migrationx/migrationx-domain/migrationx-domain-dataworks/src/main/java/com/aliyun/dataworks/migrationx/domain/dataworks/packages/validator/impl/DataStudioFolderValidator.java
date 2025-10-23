package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.fastjson2.JSON;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.ValidateMode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.AbstractFolderValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;
import org.apache.commons.collections4.SetUtils;

/**
 * DataStudio Folder Validator Class, used to validate whether the content in the DataStudio folder meets requirements
 * <p>
 * DataStudio folder structure requirements:
 * 1. Allowed subfolders:
 * - DATAWORKS_PROJECT
 * - DATAWORKS_RESOURCE
 * - DATAWORKS_MANUAL_WORKFLOW
 * - DATAWORKS_MANUAL_TASK
 * 2. No other files or folders are allowed
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class DataStudioFolderValidator extends AbstractFolderValidator {

    public static final String DATAWORKS_PROJECT_FOLDER_NAME = "DATAWORKS_PROJECT";
    public static final String DATAWORKS_RESOURCE_FOLDER_NAME = "DATAWORKS_RESOURCE";
    public static final String DATAWORKS_MANUAL_WORKFLOW_FOLDER_NAME = "DATAWORKS_MANUAL_WORKFLOW";
    public static final String DATAWORKS_MANUAL_TASK_FOLDER_NAME = "DATAWORKS_MANUAL_TASK";
    public static final String DATAWORKS_MANUAL_BUSINESS_FOLDER_NAME = "DATAWORKS_MANUAL_BUSINESS";
    public static final String DATAWORKS_MANUAL_NODE_FOLDER_NAME = "DATAWORKS_MANUAL_NODE";
    public static final String DATAWORKS_CYCLE_WORKFLOW_FOLDER_NAME = "DATAWORKS_CYCLE_WORKFLOW";
    public static final String DATAWORKS_COMPONENT_FOLDER_NAME = "DATAWORKS_COMPONENT";

    private static final DataStudioFolderValidator INSTANCE = new DataStudioFolderValidator();

    private final Map<ValidateMode, Set<String>> allowedFolders;

    protected DataStudioFolderValidator() {
        allowedFolders = new HashMap<>();

        Set<String> normalAllowedFolders = new HashSet<>();
        normalAllowedFolders.add(DATAWORKS_PROJECT_FOLDER_NAME);
        normalAllowedFolders.add(DATAWORKS_MANUAL_WORKFLOW_FOLDER_NAME);
        normalAllowedFolders.add(DATAWORKS_MANUAL_TASK_FOLDER_NAME);
        normalAllowedFolders.add(DATAWORKS_RESOURCE_FOLDER_NAME);
        normalAllowedFolders.add(DATAWORKS_COMPONENT_FOLDER_NAME);
        allowedFolders.put(ValidateMode.NORMAL, normalAllowedFolders);

        Set<String> preValidateAllowedFolders = new HashSet<>();
        preValidateAllowedFolders.add(DATAWORKS_CYCLE_WORKFLOW_FOLDER_NAME);
        preValidateAllowedFolders.add(DATAWORKS_MANUAL_BUSINESS_FOLDER_NAME);
        preValidateAllowedFolders.add(DATAWORKS_MANUAL_NODE_FOLDER_NAME);
        preValidateAllowedFolders.add(DATAWORKS_RESOURCE_FOLDER_NAME);
        preValidateAllowedFolders.add(DATAWORKS_COMPONENT_FOLDER_NAME);
        allowedFolders.put(ValidateMode.PRE_VALIDATE, preValidateAllowedFolders);
    }

    public static DataStudioFolderValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
        Set<String> allowed = getAllowedFolders(context);
        if (allowed.contains(file.getName())) {
            return PackageValidatorResult.success();
        }
        // Return specific error code for folder not allowed
        return PackageValidatorResult.failed(PackageValidateErrorCode.ONLY_SPEC_FOLDER_ALLOWED_IN_FOLDER, context,
            "folderType", "DataStudio",
            "folderName", file.getName(),
            "allowedFolders", JSON.toJSONString(allowed));
    }

    @Override
    protected PackageValidatorResult isAllowedFiles(File file, ValidateContext context) {
        // Files are never allowed, so we always return an error code
        return PackageValidatorResult.failed(PackageValidateErrorCode.ANY_FILE_NOT_ALLOWED_IN_FOLDER, context,
            "folderType", "DataStudio",
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
        // If it's an allowed folder, use DataWorksFolderValidator for validation

        if (file.isDirectory() && getAllowedFolders(context).contains(file.getName())) {
            return Collections.singletonList(DataWorksFolderValidator.getInstance());
        }

        // Otherwise use the parent class's default implementation
        return super.getChildValidators(file, context);
    }

    private Set<String> getAllowedFolders(ValidateContext context) {
        return SetUtils.emptyIfNull(allowedFolders.get(context.getValidateMode()));
    }
}
