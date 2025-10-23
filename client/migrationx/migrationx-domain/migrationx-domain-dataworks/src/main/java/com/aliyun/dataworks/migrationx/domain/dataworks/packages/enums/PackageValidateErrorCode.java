package com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * Package Validation Error Codes
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public enum PackageValidateErrorCode {

    // Validation passed code
    VALIDATION_PASSED("VALIDATION_PASSED",
        "Validation passed.",
        "No action needed as validation has passed."),

    // General validation error codes
    FOLDER_NOT_EXIST("FOLDER_NOT_EXIST",
        "Folder does not exist: {path}.",
        "Create the folder with the correct name or move related files to an existing folder."),
    FILE_NOT_EXIST("FILE_NOT_EXIST",
        "File does not exist: {path}.",
        "Create the file with the correct name or move related files to an existing file."),
    NOT_A_FOLDER("NOT_A_FOLDER",
        "Not a folder: {path}.",
        "Remove the file and create a folder with the same name or rename the existing file."),
    FILE_CONTENT_INVALID("FILE_CONTENT_INVALID",
        "Invalid file content: {error}.",
        "Check the file content and fix the syntax or format errors."),
    FILE_SCHEMA_INVALID("FILE_SCHEMA_INVALID",
        "File content does not conform to specification: {errorMessage}.",
        "Check the file content against the specification and make necessary corrections."),
    FILE_SIZE_EXCEED_LIMIT("FILE_SIZE_EXCEED_LIMIT",
        "File size exceeds limit: {limit}, {fileName}.",
        "Reduce the file size or split the file into smaller parts."),

    // Missing required file or folder error codes
    MISSING_REQUIRED_FILE("MISSING_REQUIRED_FILE",
        "Missing required file: {fileName}.",
        "Add the required file with the correct name and format."),
    MISSING_REQUIRED_FOLDER("MISSING_REQUIRED_FOLDER",
        "Missing required folder: {folderName}.",
        "Add the required folder with the correct name."),

    // File not allowed error codes
    ANY_FILE_NOT_ALLOWED_IN_FOLDER("ANY_FILE_NOT_ALLOWED_IN_FOLDER",
        "No files are allowed in {folderType} folder. Found: '{fileName}'.",
        "Remove the file or move it to an appropriate folder."),
    ONLY_SPECIFIC_FILE_ALLOWED_IN_FOLDER("ONLY_SPECIFIC_FILE_ALLOWED_IN_FOLDER",
        "File not allowed in {folderType} folder: {fileName}. Only specific files are allowed: {allowedFiles}.",
        "Remove the file or move it to an appropriate folder."),

    // Folder not allowed error codes
    ANY_FOLDER_NOT_ALLOW_IN_FOLDER("ANY_FOLDER_NOT_ALLOW_IN_FOLDER",
        "No folders are allowed in {folderType} folder. Found: '{folderName}'.",
        "Remove the folder or move it to an appropriate folder."),
    HIDDEN_FOLDER_NOT_ALLOW_IN_FOLDER("HIDDEN_FOLDER_NOT_ALLOW_IN_FOLDER",
        "Hidden folders are not allowed in {folderType} folder. Found: '{folderName}'.",
        "Remove the hidden folder or rename it to a non-hidden folder."),
    ONLY_SPEC_FOLDER_ALLOWED_IN_FOLDER("ONLY_SPEC_FOLDER_ALLOWED_IN_FOLDER",
        "Only specific folders are allowed in {folderType} folder. Found: '{folderName}'. Allowed folders: {allowedFolders}.",
        "Remove the folder or rename it to one of the allowed folder names."),
    HIDDEN_FOLDER_NOT_ALLOWED_IN_DATAWORKS_FOLDER("HIDDEN_FOLDER_NOT_ALLOWED_IN_DATAWORKS_FOLDER",
        "Only specific hidden folders [.snapshots, .deleted] and non-hidden folders are allowed in DataWorks directory. Found: '{folderName}'.",
        "Remove the hidden folder or rename it to a non-hidden folder or one of the allowed hidden folder names."),

    // .dataworks folder validation error codes
    DATAWORKS_HIDDEN_FOLDER_MISSING_METADATA_FILE("DATAWORKS_HIDDEN_FOLDER_MISSING_METADATA_FILE",
        "Missing required metadata.json file in .dataworks folder.",
        "Add a metadata.json file with the correct format."),
    DATAWORKS_HIDDEN_FOLDER_MULTIPLE_METADATA_FILES("DATAWORKS_HIDDEN_FOLDER_MULTIPLE_METADATA_FILES",
        "Multiple metadata.json files exist in .dataworks folder.",
        "Keep only one metadata.json file."),
    DATAWORKS_HIDDEN_FOLDER_MISSING_TYPE_FILE("DATAWORKS_HIDDEN_FOLDER_MISSING_TYPE_FILE",
        "Missing required .type file in .dataworks folder.",
        "Add a .type file with the correct format."),
    DATAWORKS_HIDDEN_FOLDER_MULTIPLE_TYPE_FILES("DATAWORKS_HIDDEN_FOLDER_MULTIPLE_TYPE_FILES",
        "Multiple .type files exist in .dataworks folder.",
        "Keep only one .type file."),

    // Metadata file validation error codes
    METADATA_FILE_EMPTY("METADATA_FILE_EMPTY",
        "File content is empty.",
        "Add valid content to the metadata file according to the required format."),
    METADATA_FILE_INVALID_JSON("METADATA_FILE_INVALID_JSON",
        "Metadata file contains invalid JSON. Error: {error}.",
        "Fix the JSON syntax errors in the metadata file."),
    METADATA_FILE_MISSING_SCRIPT_PATH("METADATA_FILE_MISSING_SCRIPT_PATH",
        "Missing required field: scriptPath.",
        "Add the scriptPath field with the correct value to the metadata file."),
    METADATA_FILE_EMPTY_SCRIPT_PATH("METADATA_FILE_EMPTY_SCRIPT_PATH",
        "scriptPath field cannot be empty.",
        "Provide a valid value for the scriptPath field in the metadata file."),
    METADATA_FILE_SCRIPT_PATH_MISMATCH("METADATA_FILE_SCRIPT_PATH_MISMATCH",
        "scriptPath field value does not match actual script file name.",
        "Update the scriptPath field value to match the actual script file name."),
    METADATA_FILE_MISSING_SCHEDULE_PATH("METADATA_FILE_MISSING_SCHEDULE_PATH",
        "Missing required field: schedulePath.",
        "Add the schedulePath field with the correct value to the metadata file."),
    METADATA_FILE_EMPTY_SCHEDULE_PATH("METADATA_FILE_EMPTY_SCHEDULE_PATH",
        "schedulePath field cannot be empty.",
        "Provide a valid value for the schedulePath field in the metadata file."),
    METADATA_FILE_SCHEDULE_PATH_MISMATCH("METADATA_FILE_SCHEDULE_PATH_MISMATCH",
        "schedulePath field value does not match actual schedule configuration file name.",
        "Update the schedulePath field value to match the actual schedule configuration file name."),

    // SPEC package validation error codes
    SPEC_PACKAGE_MISSING_FILE("SPEC_PACKAGE_MISSING_FILE",
        "Missing required {fileType} file. Expected file with name pattern: '{expectedFileName}'.",
        "Add a {fileType} file with the correct name."),
    SPEC_PACKAGE_MULTIPLE_FILES("SPEC_PACKAGE_MULTIPLE_FILES",
        "Multiple {fileType} files exist.",
        "Keep only one {fileType} file."),

    // Subpackage validation error codes
    SUBPACKAGE_MISSING_START_FOLDER("SUBPACKAGE_MISSING_START_FOLDER",
        "Missing required {subpackageType} subpackage.",
        "Add a {subpackageType} subpackage."),
    SUBPACKAGE_MULTIPLE_START_FOLDERS("SUBPACKAGE_MULTIPLE_START_FOLDERS",
        "Multiple {subpackageType} subpackages exist.",
        "Keep only one {subpackageType} subpackage."),
    SUBPACKAGE_MISSING_END_FOLDER("SUBPACKAGE_MISSING_END_FOLDER",
        "Missing required {subpackageType} subpackage.",
        "Add a {subpackageType} subpackage."),
    SUBPACKAGE_MULTIPLE_END_FOLDERS("SUBPACKAGE_MULTIPLE_END_FOLDERS",
        "Multiple {subpackageType} subpackages exist.",
        "Keep only one {subpackageType} subpackage."),
    ;

    @Getter
    private final String errorCode;

    private final String errorMessage;

    private final String solution;

    PackageValidateErrorCode(String errorCode, String errorMessage, String solution) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.solution = solution;
    }

    public String getMessage() {
        if (StringUtils.isNotBlank(solution)) {
            return errorMessage + " Solution: " + solution;
        }
        return errorMessage;
    }
}