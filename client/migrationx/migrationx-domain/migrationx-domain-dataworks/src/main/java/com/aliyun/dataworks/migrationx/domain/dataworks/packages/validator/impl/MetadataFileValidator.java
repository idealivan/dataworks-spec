package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.util.Optional;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.AbstractFileValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Metadata File Validator Class, used to validate whether the content of metadata.json file meets requirements
 *
 * @author 莫泣
 * @date 2025-08-27
 */
public class MetadataFileValidator extends AbstractFileValidator {

    private static final MetadataFileValidator INSTANCE = new MetadataFileValidator();

    protected MetadataFileValidator() {

    }

    public static MetadataFileValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected void doValidate(File file, ValidateContext context) {
        // Read file content
        String content = readFileContent(file, context);
        if (content == null) {
            return;
        }

        // Check if file is empty
        if (content.trim().isEmpty()) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.METADATA_FILE_EMPTY, context));
            return;
        }

        // Validate JSON format
        if (!isValidJson(content, file, context)) {
            return;
        }

        // Parse JSON content
        JsonNode jsonNode = parseJsonObject(content, file, context);
        if (jsonNode == null) {
            return;
        }

        // Validate scriptPath field
        validateScriptPath(jsonNode, file, context);

        // Validate schedulePath field
        validateSchedulePath(jsonNode, file, context);
    }

    /**
     * Validate scriptPath field
     *
     * @param jsonNode JSON node
     * @param file     file
     * @param context  validation context
     */
    private void validateScriptPath(JsonNode jsonNode, File file, ValidateContext context) {
        // Check if scriptPath field is present
        if (!jsonNode.has("scriptPath")) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.METADATA_FILE_MISSING_SCRIPT_PATH, context,
                "fieldName", "scriptPath"));
            return;
        }

        JsonNode scriptPathNode = jsonNode.get("scriptPath");
        Optional<String> scriptPathOpt = Optional.ofNullable(scriptPathNode)
            .filter(node -> !node.isNull())
            .map(JsonNode::asText)
            .filter(text -> !text.trim().isEmpty());

        if (scriptPathOpt.isPresent()) {
            String scriptPath = scriptPathOpt.get();
            File packageDir = file.getParentFile().getParentFile();

            // Find file in package directory that matches scriptPath
            File scriptFile = new File(packageDir, scriptPath);

            // Get package name (directory name)
            String packageName = packageDir.getName();
            if (!isScriptFile(scriptFile, packageName)) {
                context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.METADATA_FILE_SCRIPT_PATH_MISMATCH, context,
                    "scriptPath", scriptPath));
            }
        } else {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.METADATA_FILE_EMPTY_SCRIPT_PATH, context,
                "fieldName", "scriptPath"));
        }
    }

    /**
     * Validate schedulePath field
     *
     * @param jsonNode JSON node
     * @param file     file
     * @param context  validation context
     */
    private void validateSchedulePath(JsonNode jsonNode, File file, ValidateContext context) {
        // Check if schedulePath field is present
        if (!jsonNode.has("schedulePath")) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.METADATA_FILE_MISSING_SCHEDULE_PATH, context,
                "fieldName", "schedulePath"));
            return;
        }

        JsonNode schedulePathNode = jsonNode.get("schedulePath");
        Optional<String> schedulePathOpt = Optional.ofNullable(schedulePathNode)
            .filter(node -> !node.isNull())
            .map(JsonNode::asText)
            .filter(text -> !text.trim().isEmpty());

        if (schedulePathOpt.isPresent()) {
            String schedulePath = schedulePathOpt.get();
            File packageDir = file.getParentFile().getParentFile();

            // Find file in package directory that matches schedulePath
            File scheduleFile = new File(packageDir, schedulePath);

            // Get package name (directory name)
            String packageName = packageDir.getName();
            if (!isScheduleFile(scheduleFile, packageName)) {
                context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.METADATA_FILE_SCHEDULE_PATH_MISMATCH, context,
                    "schedulePath", schedulePath));
            }
        } else {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.METADATA_FILE_EMPTY_SCHEDULE_PATH, context,
                "fieldName", "schedulePath"));
        }
    }
}