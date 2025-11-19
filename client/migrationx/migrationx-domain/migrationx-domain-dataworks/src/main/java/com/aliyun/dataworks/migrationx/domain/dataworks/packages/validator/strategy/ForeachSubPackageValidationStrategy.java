package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.strategy;

import java.io.File;
import java.util.Optional;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;

/**
 * Controller Traverse Subpackage Validation Strategy
 * Validates that packages of type CONTROLLER_TRAVERSE.type must contain exactly one CONTROLLER_TRAVERSE_START and one CONTROLLER_TRAVERSE_END
 * subpackage
 *
 * @author 莫泣
 * @date 2025-08-27
 */
public class ForeachSubPackageValidationStrategy extends AbstractSubPackageValidationStrategy {

    public static final String CONTROLLER_TRAVERSE_TYPE = "CONTROLLER_TRAVERSE.type";
    public static final String CONTROLLER_TRAVERSE_START_TYPE = "CONTROLLER_TRAVERSE_START.type";
    public static final String CONTROLLER_TRAVERSE_END_TYPE = "CONTROLLER_TRAVERSE_END.type";

    @Override
    public boolean support(String parentPackageType) {
        return CONTROLLER_TRAVERSE_TYPE.equals(parentPackageType);
    }

    @Override
    public void validateSubPackages(File packageDir, ValidateContext context) {
        // Get all subdirectories
        File[] subDirs = Optional.ofNullable(packageDir.listFiles(File::isDirectory))
            .orElse(new File[0]);

        int startPackageCount = 0;
        int endPackageCount = 0;

        // Check each subdirectory
        for (File subDir : subDirs) {
            if (isSpecificTypePackage(subDir, CONTROLLER_TRAVERSE_START_TYPE, context)) {
                startPackageCount++;
            } else if (isSpecificTypePackage(subDir, CONTROLLER_TRAVERSE_END_TYPE, context)) {
                endPackageCount++;
            }
        }

        // Validate if required subpackages exist and are unique
        if (startPackageCount == 0) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.SUBPACKAGE_MISSING_START_FOLDER, context,
                "subpackageType", "CONTROLLER_TRAVERSE_START"));
        } else if (startPackageCount > 1) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.SUBPACKAGE_MULTIPLE_START_FOLDERS, context,
                "subpackageType", "CONTROLLER_TRAVERSE_START"));
        }

        if (endPackageCount == 0) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.SUBPACKAGE_MISSING_END_FOLDER, context,
                "subpackageType", "CONTROLLER_TRAVERSE_END"));
        } else if (endPackageCount > 1) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.SUBPACKAGE_MULTIPLE_END_FOLDERS, context,
                "subpackageType", "CONTROLLER_TRAVERSE_END"));
        }
    }
}