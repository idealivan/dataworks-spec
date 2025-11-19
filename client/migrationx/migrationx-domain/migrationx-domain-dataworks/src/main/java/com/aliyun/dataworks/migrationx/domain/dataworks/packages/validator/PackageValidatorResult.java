package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import lombok.Getter;
import org.apache.commons.collections4.MapUtils;

/**
 * Package Validation Result Class
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class PackageValidatorResult {

    private static final PackageValidatorResult SUCCESS_RESULT =
        new PackageValidatorResult(true, null, PackageValidateErrorCode.VALIDATION_PASSED);

    private static final String ERROR_MSG_FORMAT = "[{0}]: {1}";

    @Getter
    private final Boolean valid;

    private final String path;

    private final PackageValidateErrorCode errorCode;

    private final Map<String, Object> errorParams;

    private PackageValidatorResult(Boolean valid, String path, PackageValidateErrorCode errorCode, Map<String, Object> errorParams) {
        this.valid = valid;
        this.path = path;
        this.errorCode = errorCode;
        this.errorParams = errorParams;
    }

    private PackageValidatorResult(Boolean valid, String path, PackageValidateErrorCode errorCode) {
        this(valid, path, errorCode, null);
    }

    public String getErrorCode() {
        if (errorCode == null) {
            return null;
        }
        return errorCode.getErrorCode();
    }

    public String getErrorMessage() {
        if (errorCode == null || errorCode == PackageValidateErrorCode.VALIDATION_PASSED) {
            return null;
        }

        String message = errorCode.getMessage();
        for (Map.Entry<String, Object> entry : MapUtils.emptyIfNull(errorParams).entrySet()) {
            message = message.replace("{" + entry.getKey() + "}", String.valueOf(entry.getValue()));
        }
        return MessageFormat.format(ERROR_MSG_FORMAT, path, message);
    }

    public static PackageValidatorResult success() {
        return SUCCESS_RESULT;
    }

    public static PackageValidatorResult failed(PackageValidateErrorCode errorCode, ValidateContext context, Object... params) {
        Map<String, Object> errorParams = new HashMap<>();

        // Parse parameters, supporting named and positional parameters
        if (params != null && params.length > 0) {
            if (params.length % 2 != 0) {
                throw new IllegalArgumentException("error param invalid");
            }
            // Assume key-value pair parameters
            for (int i = 0; i < params.length; i += 2) {
                if (params[i] instanceof String) {
                    errorParams.put((String)params[i], params[i + 1]);
                }
            }
        }
        return new PackageValidatorResult(false, context.getRelativePath(), errorCode, errorParams);
    }
}