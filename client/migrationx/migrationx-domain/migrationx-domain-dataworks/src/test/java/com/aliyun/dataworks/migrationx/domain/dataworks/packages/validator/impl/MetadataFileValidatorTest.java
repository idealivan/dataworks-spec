package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * MetadataFileValidator Test Class
 *
 * @author 莫泣
 * @date 2025-08-28
 */
public class MetadataFileValidatorTest {

    private MetadataFileValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = MetadataFileValidator.getInstance();
        tempDir = Files.createTempDirectory("metadata-file-test");
    }

    @After
    public void tearDown() throws IOException {
        deleteRecursively(tempDir.toFile());
    }

    private void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File child : files) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }

    @Test
    public void testValidateValidMetadataFile() throws IOException {
        // Create valid metadata.json file
        String packageName = "test-package";
        Path packageDir = tempDir.resolve(packageName);
        Files.createDirectory(packageDir);

        Path dataworksDir = packageDir.resolve(SpecPackageValidator.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        String metadataContent = "{\n  \"scriptPath\": \"" + packageName + "\",\n  \"schedulePath\": \"" + packageName + ".schedule.json\"\n}";
        Path metadataFile = dataworksDir.resolve("metadata.json");
        Files.write(metadataFile, metadataContent.getBytes());

        // Create script file and schedule config file to make paths in metadata.json valid
        Files.createFile(packageDir.resolve(packageName));
        Files.createFile(packageDir.resolve(packageName + ".schedule.json"));

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(metadataFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue("Should have no validation failure results", results.stream().allMatch(result -> result.getValid() == null || result.getValid()));
    }

    @Test
    public void testValidateEmptyMetadataFile() throws IOException {
        // Create empty metadata.json file
        String packageName = "test-package";
        Path packageDir = tempDir.resolve(packageName);
        Files.createDirectory(packageDir);

        Path dataworksDir = packageDir.resolve(SpecPackageValidator.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        Path metadataFile = dataworksDir.resolve("metadata.json");
        Files.createFile(metadataFile);

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(metadataFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
        assertTrue(results.get(0).getErrorMessage().contains("File content is empty"));
    }

    @Test
    public void testValidateInvalidJsonMetadataFile() throws IOException {
        // Create invalid JSON format metadata.json file
        String packageName = "test-package";
        Path packageDir = tempDir.resolve(packageName);
        Files.createDirectory(packageDir);

        Path dataworksDir = packageDir.resolve(SpecPackageValidator.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        String metadataContent = "{invalid json}";
        Path metadataFile = dataworksDir.resolve("metadata.json");
        Files.write(metadataFile, metadataContent.getBytes());

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(metadataFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
        assertTrue(results.get(0).getErrorMessage().contains("Not valid JSON format"));
    }

    @Test
    public void testValidateMetadataFileWithoutScriptPath() throws IOException {
        // Create metadata.json file missing scriptPath field
        String packageName = "test-package";
        Path packageDir = tempDir.resolve(packageName);
        Files.createDirectory(packageDir);

        Path dataworksDir = packageDir.resolve(SpecPackageValidator.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        String metadataContent = "{\n  \"schedulePath\": \"" + packageName + ".schedule.json\"\n}";
        Path metadataFile = dataworksDir.resolve("metadata.json");
        Files.write(metadataFile, metadataContent.getBytes());

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(metadataFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // Should now get at least 1 validation result
        assertTrue("Should have at least one validation result", results.size() >= 1);
        // Check if it contains the expected error message
        assertTrue("Should contain error for missing required field: scriptPath",
            results.stream().anyMatch(result -> result.getErrorMessage().contains("Missing required field: scriptPath")));
    }

    @Test
    public void testValidateMetadataFileWithoutSchedulePath() throws IOException {
        // Create metadata.json file missing schedulePath field
        String packageName = "test-package";
        Path packageDir = tempDir.resolve(packageName);
        Files.createDirectory(packageDir);

        Path dataworksDir = packageDir.resolve(SpecPackageValidator.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        String metadataContent = "{\n  \"scriptPath\": \"" + packageName + "\"\n}";
        Path metadataFile = dataworksDir.resolve("metadata.json");
        Files.write(metadataFile, metadataContent.getBytes());

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(metadataFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // Should now get at least 1 validation result
        assertTrue("Should have at least one validation result", results.size() >= 1);
        // Check if it contains the expected error message
        assertTrue("Should contain error for missing required field: schedulePath",
            results.stream().anyMatch(result -> result.getErrorMessage().contains("Missing required field: schedulePath")));
    }

    @Test
    public void testValidateMetadataFileWithEmptyScriptPath() throws IOException {
        // Create metadata.json file with empty scriptPath field
        String packageName = "test-package";
        Path packageDir = tempDir.resolve(packageName);
        Files.createDirectory(packageDir);

        Path dataworksDir = packageDir.resolve(SpecPackageValidator.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        String metadataContent = "{\n  \"scriptPath\": \"\",\n  \"schedulePath\": \"" + packageName + ".schedule.json\"\n}";
        Path metadataFile = dataworksDir.resolve("metadata.json");
        Files.write(metadataFile, metadataContent.getBytes());

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(metadataFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // Should now get at least 1 validation result
        assertTrue("Should have at least one validation result", results.size() >= 1);
        // Check if it contains the expected error message
        assertTrue("Should contain error for scriptPath field cannot be empty",
            results.stream().anyMatch(result -> result.getErrorMessage().contains("scriptPath field cannot be empty")));
    }

    @Test
    public void testValidateMetadataFileWithEmptySchedulePath() throws IOException {
        // Create metadata.json file with empty schedulePath field
        String packageName = "test-package";
        Path packageDir = tempDir.resolve(packageName);
        Files.createDirectory(packageDir);

        Path dataworksDir = packageDir.resolve(SpecPackageValidator.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        String metadataContent = "{\n  \"scriptPath\": \"" + packageName + "\",\n  \"schedulePath\": \"\"\n}";
        Path metadataFile = dataworksDir.resolve("metadata.json");
        Files.write(metadataFile, metadataContent.getBytes());

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(metadataFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // Should now get at least 1 validation result
        assertTrue("Should have at least one validation result", results.size() >= 1);
        // Check if it contains the expected error message
        assertTrue("Should contain error for schedulePath field cannot be empty",
            results.stream().anyMatch(result -> result.getErrorMessage().contains("schedulePath field cannot be empty")));
    }

    @Test
    public void testValidateMetadataFileWithIncorrectScriptPath() throws IOException {
        // Create metadata.json file with scriptPath field that doesn't match package name
        String packageName = "test-package";
        Path packageDir = tempDir.resolve(packageName);
        Files.createDirectory(packageDir);

        Path dataworksDir = packageDir.resolve(SpecPackageValidator.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        String metadataContent = "{\n  \"scriptPath\": \"incorrect-name\",\n  \"schedulePath\": \"" + packageName + ".schedule.json\"\n}";
        Path metadataFile = dataworksDir.resolve("metadata.json");
        Files.write(metadataFile, metadataContent.getBytes());

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(metadataFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // Should now get at least 1 validation result
        assertTrue("Should have at least one validation result", results.size() >= 1);
        // Check if it contains the expected error message
        assertTrue("Should contain error for scriptPath field value does not match actual script file name",
            results.stream().anyMatch(result -> result.getErrorMessage().contains("scriptPath field value does not match actual script file name")));
    }

    @Test
    public void testValidateMetadataFileWithIncorrectSchedulePath() throws IOException {
        // Create metadata.json file with schedulePath field that doesn't match expected value
        String packageName = "test-package";
        Path packageDir = tempDir.resolve(packageName);
        Files.createDirectory(packageDir);

        Path dataworksDir = packageDir.resolve(SpecPackageValidator.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        String metadataContent = "{\n  \"scriptPath\": \"" + packageName + "\",\n  \"schedulePath\": \"incorrect-name.schedule.json\"\n}";
        Path metadataFile = dataworksDir.resolve("metadata.json");
        Files.write(metadataFile, metadataContent.getBytes());

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(metadataFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // Should now get at least 1 validation result
        assertTrue("Should have at least one validation result", results.size() >= 1);
        // Check if it contains the expected error message
        assertTrue("Should contain error for schedulePath field value does not match actual schedule configuration file name",
            results.stream().anyMatch(
                result -> result.getErrorMessage().contains("schedulePath field value does not match actual schedule configuration file name")));
    }

    @Test
    public void testValidateScriptPathWithNullNode() throws IOException {
        // Create metadata.json file with scriptPath field as null (covers line 144)
        String packageName = "test-package";
        Path packageDir = tempDir.resolve(packageName);
        Files.createDirectory(packageDir);

        Path dataworksDir = packageDir.resolve(SpecPackageValidator.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        String metadataContent = "{\n  \"scriptPath\": null,\n  \"schedulePath\": \"" + packageName + ".schedule.json\"\n}";
        Path metadataFile = dataworksDir.resolve("metadata.json");
        Files.write(metadataFile, metadataContent.getBytes());

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(metadataFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        // Should now get at least 1 validation result
        assertTrue("Should have at least one validation result", results.size() >= 1);
        // Check if it contains the expected error message
        assertTrue("Should contain error for scriptPath field cannot be empty",
            results.stream().anyMatch(result -> result.getErrorMessage().contains("scriptPath field cannot be empty")));
    }

    @Test
    public void testValidateSchedulePathWithNullNode() throws IOException {
        // Create metadata.json file with schedulePath field as null (covers line 181)
        String packageName = "test-package";
        Path packageDir = tempDir.resolve(packageName);
        Files.createDirectory(packageDir);

        Path dataworksDir = packageDir.resolve(SpecPackageValidator.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        String metadataContent = "{\n  \"scriptPath\": \"" + packageName + "\",\n  \"schedulePath\": null\n}";
        Path metadataFile = dataworksDir.resolve("metadata.json");
        Files.write(metadataFile, metadataContent.getBytes());

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(metadataFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        // Should now get at least 1 validation result
        assertTrue("Should have at least one validation result", results.size() >= 1);
        // Check if it contains the expected error message
        assertTrue("Should contain error for schedulePath field cannot be empty",
            results.stream().anyMatch(result -> result.getErrorMessage().contains("schedulePath field cannot be empty")));
    }
}