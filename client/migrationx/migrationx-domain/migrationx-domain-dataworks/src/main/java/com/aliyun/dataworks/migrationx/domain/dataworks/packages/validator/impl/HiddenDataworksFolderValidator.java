package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.AbstractFolderValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;

/**
 * .dataworks Hidden Folder Validator Class, used to validate whether the content in the .dataworks folder meets requirements
 *
 * @author 莫泣
 * @date 2025-08-31
 */
public class HiddenDataworksFolderValidator extends AbstractFolderValidator {

    public static final String METADATA_FILE_NAME = "metadata.json";
    public static final String TYPE_FILE_SUFFIX = ".type";

    private static final HiddenDataworksFolderValidator INSTANCE = new HiddenDataworksFolderValidator();

    protected HiddenDataworksFolderValidator() {
    }

    public static HiddenDataworksFolderValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected void doValidate(File file, ValidateContext context) {
        // First call the parent class's validation method
        super.doValidate(file, context);

        // Validate .dataworks folder content
        validateDataworksFolder(file, context);
    }

    /**
     * Validate .dataworks folder content
     *
     * @param dataworksFolder .dataworks folder
     * @param context         validation context
     */
    private void validateDataworksFolder(File dataworksFolder, ValidateContext context) {
        File[] files = Optional.ofNullable(dataworksFolder.listFiles()).orElse(new File[0]);

        // Count file types
        Map<String, Integer> fileCounts = new HashMap<>();
        Map<String, File> fileMap = new HashMap<>();

        // Process files
        for (File file : files) {
            if (file.isFile()) {
                processFile(file, fileCounts, fileMap, context);
            } else {
                // Check directories (.dataworks folder does not allow subfolders)
                context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.ANY_FOLDER_NOT_ALLOW_IN_FOLDER, context,
                    "folderType", ".dataworks",
                    "folderName", context.getRelativePath(file)));
            }
        }

        // Check if required files exist and are unique
        validateRequiredFiles(fileCounts, context);
    }

    /**
     * Process a single file
     *
     * @param file       file
     * @param fileCounts file count map
     * @param fileMap    file map
     * @param context    validation context
     */
    private void processFile(File file, Map<String, Integer> fileCounts, Map<String, File> fileMap,
                             ValidateContext context) {
        String name = file.getName();
        if (METADATA_FILE_NAME.equals(name)) {
            fileCounts.merge("metadataFile", 1, Integer::sum);
            fileMap.put("metadataFile", file);
        } else if (name.endsWith(TYPE_FILE_SUFFIX)) {
            fileCounts.merge("typeFile", 1, Integer::sum);
        }
    }

    /**
     * Validate if required files exist and are unique
     *
     * @param fileCounts file count map
     * @param context    validation context
     */
    private void validateRequiredFiles(Map<String, Integer> fileCounts, ValidateContext context) {
        int metadataFileCount = fileCounts.getOrDefault("metadataFile", 0);
        int typeFileCount = fileCounts.getOrDefault("typeFile", 0);

        if (metadataFileCount == 0) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.DATAWORKS_HIDDEN_FOLDER_MISSING_METADATA_FILE, context));
        } else if (metadataFileCount > 1) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.DATAWORKS_HIDDEN_FOLDER_MULTIPLE_METADATA_FILES, context));
        }
        // Validation of metadata.json file will be handled by MetadataFileValidator returned in getChildValidators

        if (typeFileCount == 0) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.DATAWORKS_HIDDEN_FOLDER_MISSING_TYPE_FILE, context));
        } else if (typeFileCount > 1) {
            context.addResult(
                PackageValidatorResult.failed(PackageValidateErrorCode.DATAWORKS_HIDDEN_FOLDER_MULTIPLE_TYPE_FILES, context));
        }
    }

    @Override
    protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
        // Subfolders are not allowed in .dataworks folder
        return PackageValidatorResult.failed(PackageValidateErrorCode.ANY_FOLDER_NOT_ALLOW_IN_FOLDER, context,
            "folderType", ".dataworks",
            "folderName", file.getName());
    }

    @Override
    protected PackageValidatorResult isAllowedFiles(File file, ValidateContext context) {
        // Allow metadata.json and .type files
        String name = file.getName();
        if (METADATA_FILE_NAME.equals(name) || name.endsWith(TYPE_FILE_SUFFIX)) {
            return PackageValidatorResult.success();
        }
        // Return specific error code for file not allowed
        return PackageValidatorResult.failed(PackageValidateErrorCode.ONLY_SPECIFIC_FILE_ALLOWED_IN_FOLDER, context,
            "folderType", ".dataworks",
            "fileName", name,
            "allowedFiles", "metadata.json and *.type files");
    }

    @Override
    protected java.util.Set<String> getRequiredFolders(ValidateContext context) {
        // No required folders in .dataworks folder
        return Collections.emptySet();
    }

    @Override
    protected java.util.Set<String> getRequiredFiles(ValidateContext context) {
        // No need to check required files here, more detailed checks are performed in validateDataworksFolder method
        return Collections.emptySet();
    }

    @Override
    public List<PackageValidator> getChildValidators(File file, ValidateContext context) {
        // If it's a metadata.json file, use MetadataFileValidator for validation
        if (file.isFile() && METADATA_FILE_NAME.equals(file.getName())) {
            return Collections.singletonList(MetadataFileValidator.getInstance());
        }

        // For other files and folders, use the parent class's default implementation
        return super.getChildValidators(file, context);
    }
}