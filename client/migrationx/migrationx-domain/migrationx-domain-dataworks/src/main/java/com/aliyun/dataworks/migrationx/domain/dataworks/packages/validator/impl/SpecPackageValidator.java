package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.ValidateMode;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.AbstractFolderValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.SpecificationWrapper;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.SubPackageValidationStrategy;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.strategy.DoWhileSubPackageValidationStrategy;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.strategy.ForeachSubPackageValidationStrategy;
import com.aliyun.dataworks.migrationx.domain.dataworks.utils.SpecInfoUtil;

import static com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl.HiddenDataworksFolderValidator.TYPE_FILE_SUFFIX;

/**
 * SPEC Package Validator Class, used to validate whether the SPEC package structure meets requirements
 * <p>
 * SPEC package structure requirements:
 * 1. In the root directory:
 * - Must contain a .dataworks folder
 * - Must contain a .schedule.json or .spec.json file (schedule configuration)
 * - Must contain a script content file (filename same as package name)
 * 2. In the .dataworks folder:
 * - Must contain metadata.json file (metadata content)
 * - Must contain a .type file (metadata content)
 * 3. Subpackages are only allowed when the .type file is of a specific type
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class SpecPackageValidator extends AbstractFolderValidator {

    public static final String DATAWORKS_FILE_NAME = ".dataworks";

    // Supported subpackage types set
    private static final Set<String> SUPPORTED_SUBPACKAGE_TYPES = new HashSet<>();

    // Subpackage validation strategy list
    private static final List<SubPackageValidationStrategy> SUB_PACKAGE_VALIDATION_STRATEGIES = new ArrayList<>();

    static {
        SUPPORTED_SUBPACKAGE_TYPES.add("WORKFLOW.type");
        SUPPORTED_SUBPACKAGE_TYPES.add("MANUAL_WORKFLOW.type");
        SUPPORTED_SUBPACKAGE_TYPES.add("CONTROLLER_CYCLE.type");
        SUPPORTED_SUBPACKAGE_TYPES.add("CONTROLLER_TRAVERSE.type");
        SUPPORTED_SUBPACKAGE_TYPES.add("PAI_FLOW.type");

        // Initialize subpackage validation strategies
        initializeSubPackageValidationStrategies();
    }

    private static final SpecPackageValidator INSTANCE = new SpecPackageValidator();

    private final Set<String> requiredFolders;

    protected SpecPackageValidator() {
        this.requiredFolders = new HashSet<>(Collections.singletonList(DATAWORKS_FILE_NAME));
    }

    public static SpecPackageValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected void doValidate(File file, ValidateContext context) {
        // First call the parent class's validation method, then add our own validation logic
        super.doValidate(file, context);

        // Get package name (folder name)
        String packageName = file.getName();

        // Check root directory file structure
        validateRootDirectory(file, packageName, context);
    }

    /**
     * Validate root directory structure
     *
     * @param packageDir  package directory
     * @param packageName package name
     * @param context     validation context
     */
    private void validateRootDirectory(File packageDir, String packageName, ValidateContext context) {
        File[] files = Optional.ofNullable(packageDir.listFiles()).orElse(new File[0]);

        boolean allowedSubPackage = isAllowedSubPackage(packageDir, context);

        // Separate files and directories
        List<File> directories = Stream.of(files)
            .filter(File::isDirectory)
            .collect(Collectors.toList());
        List<File> regularFiles = Stream.of(files)
            .filter(file -> !file.isDirectory())
            .collect(Collectors.toList());

        // Validate directories
        directories.forEach(subFile -> {
            if (DATAWORKS_FILE_NAME.equals(subFile.getName())) {
                return;
            }
            if (!allowedSubPackage || !isPackage(subFile)) {
                // Disallowed folder
                context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.ONLY_SPEC_FOLDER_ALLOWED_IN_FOLDER, context,
                    "folderType", "SPEC package",
                    "folderName", context.getRelativePath(subFile),
                    "allowedFolders", "subpackage or .dataworks"));
            }
        });

        // Validate files
        Map<String, Integer> fileCounts = new HashMap<>();
        regularFiles.forEach(subFile -> {
            if (isScheduleFile(subFile, packageName)) {
                fileCounts.merge("scheduleFile", 1, Integer::sum);
                // Validation of schedule configuration file will be handled by SpecFileValidator returned in getChildValidators
                // Here we just need to ensure it's allowed
            } else if (isScriptFile(subFile, packageName)) {
                // Script content filename same as package name
                fileCounts.merge("scriptFile", 1, Integer::sum);
            } else {
                // Disallowed file
                context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.ONLY_SPECIFIC_FILE_ALLOWED_IN_FOLDER, context,
                    "folderType", "SPEC package",
                    "fileName", context.getRelativePath(subFile),
                    "allowedFiles", "schedule configuration file and script content file"));
            }
        });

        // Check if required files and folders exist and are unique
        int scheduleFileCount = fileCounts.getOrDefault("scheduleFile", 0);
        int scriptFileCount = fileCounts.getOrDefault("scriptFile", 0);

        if (scheduleFileCount == 0) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.SPEC_PACKAGE_MISSING_FILE, context,
                "fileType", "schedule configuration",
                "expectedFileName", packageName + "(" + SCHEDULE_FILE_SUFFIX + "|" + SPEC_FILE_SUFFIX + ")"));
        } else if (scheduleFileCount > 1) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.SPEC_PACKAGE_MULTIPLE_FILES, context,
                "fileType", "schedule configuration"));
        }

        if (scriptFileCount == 0) {
            context.addResult(
                PackageValidatorResult.failed(PackageValidateErrorCode.SPEC_PACKAGE_MISSING_FILE, context,
                    "fileType", "script content",
                    "expectedFileName", "script content file: " + packageName + "[.ext]"));
        } else if (scriptFileCount > 1) {
            context.addResult(PackageValidatorResult.failed(PackageValidateErrorCode.SPEC_PACKAGE_MULTIPLE_FILES, context,
                "fileType", "script content"));
        }

        // If it's a supported subpackage type, execute subpackage validation
        if (allowedSubPackage) {
            validateSubPackages(packageDir, context);
        }
    }

    /**
     * Determine if subpackage is allowed
     *
     * @param packageDir parent package directory
     * @param context    context
     * @return whether subpackage is allowed
     */
    private boolean isAllowedSubPackage(File packageDir, ValidateContext context) {
        // Subpackages are only allowed when the parent package's .type file is of a supported type
        return Optional.ofNullable(packageDir)
            .filter(this::isPackage)
            .filter(folder -> isSupportedSubPackageType(folder, context))
            .isPresent();
    }

    @Override
    public List<PackageValidator> getChildValidators(File file, ValidateContext context) {
        // If it's a .dataworks folder, use HiddenDataworksFolderValidator
        if (file.isDirectory() && DATAWORKS_FILE_NAME.equals(file.getName())) {
            return Collections.singletonList(HiddenDataworksFolderValidator.getInstance());
        }

        // If it's a schedule configuration file, use SpecFileValidator
        if (file.isFile() && (file.getName().endsWith(SCHEDULE_FILE_SUFFIX) || file.getName().endsWith(SPEC_FILE_SUFFIX))) {
            return Collections.singletonList(SpecFileValidator.getInstance());
        }

        // If it's a supported subpackage type, allow subpackages to use SpecPackageValidator
        if (file.isDirectory() && isAllowedSubPackage(file.getParentFile(), context)) {
            return Collections.singletonList(SpecPackageValidator.getInstance());
        }

        // Otherwise use the parent class's default implementation
        return super.getChildValidators(file, context);
    }

    /**
     * Determine if it's a supported subpackage type
     *
     * @param folder  folder
     * @param context context
     * @return whether it's a supported subpackage type
     */
    private boolean isSupportedSubPackageType(File folder, ValidateContext context) {
        return SUPPORTED_SUBPACKAGE_TYPES.contains(getPackageType(folder, context));
    }

    /**
     * Get package type
     *
     * @param folder folder
     * @return package type, or null if unable to determine
     */
    public String getPackageType(File folder, ValidateContext context) {
        if (context.isPreCheck()) {
            File scheduleFile = Optional.ofNullable(folder)
                .filter(file -> file.exists() && file.isDirectory())
                .map(File::listFiles)
                .flatMap(files -> Stream.of(files)
                    .filter(f -> isScheduleFile(f, folder.getName()))
                    .findFirst())
                .orElse(null);
            SpecificationWrapper wrapper = SpecFileValidator.getInstance().readSpecification(scheduleFile, context);
            return Optional.ofNullable(wrapper).map(s -> {
                try {
                    return SpecInfoUtil.getSpecCommand(s.getSpecification()) + TYPE_FILE_SUFFIX;
                } catch (Exception e) {
                    return null;
                }
            }).orElse(null);
        }
        return Optional.ofNullable(folder)
            .map(f -> new File(f, DATAWORKS_FILE_NAME))
            .filter(file -> file.exists() && file.isDirectory())
            .map(File::listFiles)
            .flatMap(files -> Stream.of(files)
                .filter(File::isFile)
                .map(File::getName)
                .filter(name -> name.endsWith(TYPE_FILE_SUFFIX))
                .findFirst())
            .orElse(null);
    }

    @Override
    protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
        // .dataworks folder and other subpackages are allowed in SPEC package
        boolean allowed = Optional.ofNullable(file)
            .map(File::getName)
            .filter(name -> DATAWORKS_FILE_NAME.equals(name) || isPackage(file))
            .isPresent();
        if (allowed) {
            return PackageValidatorResult.success();
        }
        // Return specific error code for folder not allowed
        return PackageValidatorResult.failed(PackageValidateErrorCode.ONLY_SPEC_FOLDER_ALLOWED_IN_FOLDER, context,
            "folderType", "SPEC package",
            "folderName", file.getName(),
            "allowedFolders", "subpackage or .dataworks");
    }

    @Override
    protected PackageValidatorResult isAllowedFiles(File file, ValidateContext context) {
        // Allow all files, specific validation is performed in validateRootDirectory
        return PackageValidatorResult.success();
    }

    @Override
    protected Set<String> getRequiredFolders(ValidateContext context) {
        return ValidateMode.PRE_VALIDATE.equals(context.getValidateMode()) ? Collections.emptySet() : requiredFolders;
    }

    @Override
    protected Set<String> getRequiredFiles(ValidateContext context) {
        // Here we don't directly use the parent class's required file check, but perform more detailed checks in validateRootDirectory method
        return Collections.emptySet();
    }

    /**
     * Initialize subpackage validation strategies
     */
    private static void initializeSubPackageValidationStrategies() {
        // Manually add default strategies
        SUB_PACKAGE_VALIDATION_STRATEGIES.add(new DoWhileSubPackageValidationStrategy());
        SUB_PACKAGE_VALIDATION_STRATEGIES.add(new ForeachSubPackageValidationStrategy());
    }

    /**
     * Validate subpackages
     *
     * @param packageDir parent package directory
     * @param context    validation context
     */
    private void validateSubPackages(File packageDir, ValidateContext context) {
        // Find applicable validation strategies and execute validation
        SUB_PACKAGE_VALIDATION_STRATEGIES.stream()
            .filter(strategy -> strategy.support(getPackageType(packageDir, context)))
            .forEach(strategy -> strategy.validateSubPackages(packageDir, context));
    }
}