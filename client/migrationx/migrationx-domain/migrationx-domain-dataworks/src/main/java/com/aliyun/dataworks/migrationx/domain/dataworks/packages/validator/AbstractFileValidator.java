package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator;

import java.io.File;
import java.io.IOException;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.migrationx.common.utils.JSONUtils;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.FileUtils;

/**
 * Abstract File Validator Class
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public abstract class AbstractFileValidator extends AbstractBaseValidator {

    private static final long MAX_FILE_SIZE = 15 * FileUtils.ONE_MB;

    @Override
    public final void validate(File file, ValidateContext context) {
        context.pushFile(file);
        try {
            doValidate(file, context);
        } finally {
            context.popFile();
        }
    }

    /**
     * Execute specific validation logic
     *
     * @param file    file
     * @param context validation context
     */
    protected abstract void doValidate(File file, ValidateContext context);

    /**
     * Read file content
     *
     * @param file    file
     * @param context validation context
     * @return file content, or null if reading fails
     */
    protected String readFileContent(File file, ValidateContext context) {
        if (!file.exists() || !file.isFile()) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FILE_NOT_EXIST, context, "path", context.getRelativePath(file)));
            return null;
        }
        if (file.length() > MAX_FILE_SIZE) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FILE_SIZE_EXCEED_LIMIT, context,
                "fileName", context.getRelativePath(file), "limit", FileUtils.byteCountToDisplaySize(MAX_FILE_SIZE)));
            return null;
        }
        try {
            return FileUtils.readFileToString(file, "UTF-8");
        } catch (IOException e) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FILE_CONTENT_INVALID, context,
                "error", e.getLocalizedMessage()));
            return null;
        }
    }

    /**
     * Validate if it's valid JSON format
     *
     * @param content file content
     * @param file    file
     * @param context validation context
     * @return whether it's valid JSON format
     */
    protected boolean isValidJson(String content, File file, ValidateContext context) {
        if (!JSONUtils.checkJsonValid(content)) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FILE_CONTENT_INVALID, context,
                "error", "Not valid JSON format"));
            return false;
        }
        return true;
    }

    /**
     * Parse JSON object
     *
     * @param content file content
     * @param file    file
     * @param context validation context
     * @return JSON object, or null if parsing fails
     */
    protected JsonNode parseJsonObject(String content, File file, ValidateContext context) {
        try {
            return JSONUtils.parseObject(content, JsonNode.class);
        } catch (Exception e) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FILE_CONTENT_INVALID, context,
                "error", "Error parsing JSON: " + e.getLocalizedMessage()));
            return null;
        }
    }
}
