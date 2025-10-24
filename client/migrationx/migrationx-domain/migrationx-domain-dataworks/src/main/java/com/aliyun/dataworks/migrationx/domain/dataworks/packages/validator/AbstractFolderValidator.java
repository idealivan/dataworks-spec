package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl.EmptyFileValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl.EmptyFolderValidator;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.SetUtils;

/**
 * Abstract Package Validator Base Class
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public abstract class AbstractFolderValidator extends AbstractBaseValidator {

    public static final String DATAWORKS_FILE_NAME = ".dataworks";

    @Override
    public final void validate(File file, ValidateContext context) {
        context.pushFile(file);

        try {
            doValidate(file, context);

            // Validate subdirectory content
            if (file.isDirectory()) {
                Arrays.stream(Optional.ofNullable(file.listFiles()).orElse(new File[0]))
                    .forEach(subFile ->
                        ListUtils.emptyIfNull(getChildValidators(subFile, context))
                            .forEach(validator -> validator.validate(subFile, context))
                    );
            }
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
    protected void doValidate(File file, ValidateContext context) {
        // Check if file/directory exists
        if (!file.exists()) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.FOLDER_NOT_EXIST, context,
                "path", context.getRelativePath(file)));
            return;
        }

        if (!file.isDirectory()) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.NOT_A_FOLDER, context,
                "path", context.getRelativePath(file)));
            return;
        }

        // Check if required files exist
        SetUtils.emptyIfNull(getRequiredFiles(context)).forEach(requiredFile -> {
            File required = new File(file, requiredFile);
            if (!required.exists() || !required.isFile()) {
                context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.MISSING_REQUIRED_FILE, context,
                    "fileName", requiredFile));
            }
        });

        // Check if required directories exist
        SetUtils.emptyIfNull(getRequiredFolders(context)).forEach(requiredDirectory -> {
            File required = new File(file, requiredDirectory);
            if (!required.exists() || !required.isDirectory()) {
                context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.MISSING_REQUIRED_FOLDER, context,
                    "folderName", requiredDirectory));
            }
        });

        // Check directory content
        Arrays.stream(Optional.ofNullable(file.listFiles()).orElse(new File[0]))
            .forEach(subFile -> {
                if (subFile.isDirectory()) {
                    // Check if it's an allowed directory
                    PackageValidatorResult folderResult = isAllowedFolders(subFile, context);
                    if (folderResult != null && !folderResult.getValid()) {
                        // Directly add the result to the context
                        context.addResult(folderResult);
                    }
                } else {
                    // Check if it's an allowed file
                    PackageValidatorResult fileResult = isAllowedFiles(subFile, context);
                    if (fileResult != null && !fileResult.getValid()) {
                        // Directly add the result to the context
                        context.addResult(fileResult);
                    }
                }
            });
    }

    /**
     * Get child validator list
     *
     * @param file    sub file or directory object
     * @param context validation context
     * @return child validator list
     */
    protected List<PackageValidator> getChildValidators(File file, ValidateContext context) {
        if (file.isDirectory()) {
            return Collections.singletonList(EmptyFolderValidator.getInstance());
        } else {
            return Collections.singletonList(EmptyFileValidator.getInstance());
        }
    }

    /**
     * Check if directory is an allowed directory
     *
     * @param file    directory file
     * @param context validation context
     * @return validation result containing error code and parameters, or success() if permitted
     */
    protected abstract PackageValidatorResult isAllowedFolders(File file, ValidateContext context);

    /**
     * Check if file is an allowed file
     *
     * @param file    file
     * @param context validation context
     * @return validation result containing error code and parameters, or success() if permitted
     */
    protected abstract PackageValidatorResult isAllowedFiles(File file, ValidateContext context);

    /**
     * Get required directory set
     *
     * @return required directory set
     */
    protected abstract Set<String> getRequiredFolders(ValidateContext context);

    /**
     * Get required file set
     *
     * @return required file set
     */
    protected abstract Set<String> getRequiredFiles(ValidateContext context);

    /**
     * Check if directory is a package directory (contains .dataworks subdirectory)
     *
     * @param folder directory
     * @return whether it's a package directory
     */
    protected boolean isPackage(File folder) {
        return Optional.ofNullable(folder)
            .filter(File::isDirectory)
            .map(File::listFiles)
            .map(Arrays::stream)
            .orElse(Stream.empty())
            .anyMatch(file -> isHiddenDataWorksFolder(file)
                || isScheduleFile(file, Optional.ofNullable(folder).map(File::getName).orElse(null)));
    }

    private boolean isHiddenDataWorksFolder(File folder) {
        return Optional.ofNullable(folder)
            .filter(f -> f.isDirectory() && DATAWORKS_FILE_NAME.equals(f.getName()))
            .isPresent();
    }
}