package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator;

import java.io.File;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

/**
 * Abstract Base Validator Class, providing common validation methods
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public abstract class AbstractBaseValidator implements PackageValidator {

    public static final String SCHEDULE_FILE_SUFFIX = ".schedule.json";
    public static final String SPEC_FILE_SUFFIX = ".spec.json";

    /**
     * Determine if file is a hidden file
     *
     * @param file file
     * @return true if is hidden file
     */
    protected boolean isHidden(File file) {
        return Optional.ofNullable(file).map(File::getName).map(name -> name.startsWith(".")).orElse(false);
    }

    /**
     * Determine if file is a schedule file
     *
     * @param file        file
     * @param packageName package name
     * @return whether it is a schedule file
     */
    protected boolean isScheduleFile(File file, String packageName) {
        if (!Optional.ofNullable(file).map(File::exists).orElse(false)) {
            return false;
        }
        String name = file.getName();
        String scheduleFileName = packageName + SCHEDULE_FILE_SUFFIX;
        String specFileName = packageName + SPEC_FILE_SUFFIX;
        return scheduleFileName.equals(name) || specFileName.equals(name);
    }

    /**
     * Determine if file is a script file
     *
     * @param file        file
     * @param packageName package name
     * @return whether it is a script file
     */
    protected boolean isScriptFile(File file, String packageName) {
        if (!Optional.ofNullable(file).map(File::exists).orElse(false)) {
            return false;
        }
        String name = file.getName();
        // After removing the extension, it should match the package name
        boolean nameSame = StringUtils.startsWith(name, packageName + ".") || StringUtils.equals(name, packageName);
        return nameSame && !isScheduleFile(file, packageName);
    }
}