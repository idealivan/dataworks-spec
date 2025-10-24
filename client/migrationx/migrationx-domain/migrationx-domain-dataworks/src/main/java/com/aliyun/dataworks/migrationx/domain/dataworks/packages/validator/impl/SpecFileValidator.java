package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.util.List;
import java.util.Optional;

import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.utils.SpecValidateUtil;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.AbstractFileValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.SpecificationWrapper;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.BaseSpecInfoHandler;
import com.aliyun.dataworks.migrationx.domain.dataworks.utils.SpecInfoUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spec File Validator
 * Used to validate whether the content of .spec.json and .schedule.json files conforms to the specification
 *
 * @author 莫泣
 * @date 2025-08-31
 */
public class SpecFileValidator extends AbstractFileValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpecFileValidator.class);

    private static final SpecFileValidator INSTANCE = new SpecFileValidator();

    private SpecFileValidator() {
    }

    public static SpecFileValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected void doValidate(File file, ValidateContext context) {
        // Read file content
        SpecificationWrapper wrapper = readSpecification(file, context);

        // If file content is empty, return error
        if (wrapper == null) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FILE_CONTENT_INVALID, context,
                "error", "spec is empty"));
            return;
        }

        // Call SpecValidateUtil for validation
        try {
            List<String> errors = SpecValidateUtil.validate(wrapper.getSpec());
            if (errors != null && !errors.isEmpty()) {
                // If there are validation errors, add them to results
                errors.forEach(error -> context.addResult(
                    PackageValidatorResult.failed(PackageValidateErrorCode.FILE_SCHEMA_INVALID, context, "errorMessage", error)));
            }
        } catch (Exception e) {
            LOGGER.error("Failed to validate spec file: {}", file.getAbsolutePath(), e);
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FILE_CONTENT_INVALID, context,
                "error", e.getLocalizedMessage()));
            return;
        }
        // Validate specification
        validateSpecification(wrapper.getSpecification(), context);
    }

    public SpecificationWrapper readSpecification(File file, ValidateContext context) {
        String relativePath = context.getRelativePath(file);
        return Optional.ofNullable(context.getSpecification(relativePath))
            .orElseGet(() -> {
                try {
                    String content = readFileContent(file, context);
                    Specification<DataWorksWorkflowSpec> specification = SpecUtil.parseToDomain(content);
                    SpecificationWrapper wrapper = new SpecificationWrapper(specification, content);
                    context.addSpecification(relativePath, wrapper);
                    return wrapper;
                } catch (Exception e) {
                    return null;
                }
            });
    }

    private void validateSpecification(Specification<DataWorksWorkflowSpec> specification, ValidateContext context) {
        try {
            // don't use SpecInfoUtil#getSpecId, because it will return null when spec id is invalid
            SpecRefEntity specRefEntity = SpecInfoUtil.getSpecRefEntity(specification);
            String specId = Optional.ofNullable(specRefEntity).map(SpecRefEntity::getId).orElse(null);
            if (specId != null && !BaseSpecInfoHandler.checkUuidValid(specId)) {
                context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FILE_CONTENT_INVALID, context,
                    "error", "specId is expected to be a positive number within the range of the Long type, but was: " + specId
                ));
            }

            if (StringUtils.isNotBlank(specId) && !context.addUuidIfAbsent(specId)) {
                context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FILE_CONTENT_INVALID, context,
                    "error", "uuid duplicate: " + specId));
            }
            File currentFile = context.getCurrentFile();
            File packageFile = currentFile.getParentFile();
            String specName = SpecInfoUtil.getSpecName(specification);
            if (!StringUtils.equals(specName, packageFile.getName())) {
                context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FILE_CONTENT_INVALID, context,
                    "error", "specName not equal packageName"));
            }
        } catch (Exception e) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FILE_CONTENT_INVALID, context,
                "error", e.getLocalizedMessage()));
        }
    }
}