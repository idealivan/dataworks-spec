package com.aliyun.dataworks.migrationx.domain.dataworks.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * SpecPackageValidateUtil Test Class
 *
 * @author 莫泣
 * @date 2025-08-29
 */
public class SpecPackageValidateUtilTest {

    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("spec-package-validate-util-test");
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
    public void testValidateWithNullPath() {
        try {
            SpecPackageValidateUtil.validate(null);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Package path cannot be empty"));
        }
    }

    @Test
    public void testValidateWithEmptyPath() {
        try {
            SpecPackageValidateUtil.validate("");
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Package path cannot be empty"));
        }
    }

    @Test
    public void testValidateWithBlankPath() {
        try {
            SpecPackageValidateUtil.validate("   ");
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Package path cannot be empty"));
        }
    }

    @Test
    public void testValidateWithNonExistentPath() {
        String nonExistentPath = tempDir.resolve("non-existent").toString();
        try {
            SpecPackageValidateUtil.validate(nonExistentPath);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Specified package path does not exist"));
        }
    }

    @Test
    public void testValidateWithRegularFile() throws IOException {
        Path regularFile = tempDir.resolve("regular-file.txt");
        Files.createFile(regularFile);
        try {
            SpecPackageValidateUtil.validate(regularFile.toString());
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Specified path is not a directory"));
        }
    }

    @Test
    public void testValidateWithEmptyDirectory() throws IOException {
        Path emptyDir = tempDir.resolve("empty-dir");
        Files.createDirectory(emptyDir);

        List<PackageValidatorResult> results = SpecPackageValidateUtil.validate(emptyDir.toString());

        assertNotNull(results);
        // Empty directory should have validation failure results
        assertTrue(results.stream().anyMatch(result -> result.getValid() != null && !result.getValid()));
    }

    @Test
    public void testValidateWithValidSpecPackage() throws IOException {
        // Create a valid SPEC package structure, but it needs to meet RootFolderValidator requirements
        Path rootDir = tempDir.resolve("root-dir");
        Files.createDirectory(rootDir);

        // Create SPEC.FORMAT file
        Files.createFile(rootDir.resolve("SPEC.FORMAT"));

        // Create DataStudio directory
        Path dataStudioDir = rootDir.resolve("DataStudio");
        Files.createDirectory(dataStudioDir);

        // Create DATAWORKS_PROJECT directory in DataStudio directory
        Path projectDir = dataStudioDir.resolve("DATAWORKS_PROJECT");
        Files.createDirectory(projectDir);

        // Create SPEC package in DATAWORKS_PROJECT directory
        String packageName = "test-package";
        Path packageDir = projectDir.resolve(packageName);
        Files.createDirectory(packageDir);

        // Create .dataworks folder and its contents
        Path dataworksDir = packageDir.resolve(".dataworks");
        Files.createDirectory(dataworksDir);

        // Create metadata.json file with valid content
        String metadataContent = "{\n  \"scriptPath\": \"" + packageName + "\",\n  \"schedulePath\": \"" + packageName + ".schedule.json\"\n}";
        Files.write(dataworksDir.resolve("metadata.json"), metadataContent.getBytes());

        Files.createFile(dataworksDir.resolve("test.type"));

        // Create script content file (filename same as package name)
        Files.createFile(packageDir.resolve(packageName));

        // Create .schedule.json file (schedule configuration)
        String scheduleContent =
            "{\n  \"version\": \"1.0.0\",\n  \"kind\": \"CycleWorkflow\",\n  \"spec\": {\n    \"nodes\": [\n      {\n        \"id\": \"123456789\","
                + "\n        \"name\": \"test-package\",\n        \"script\": {\n          \"path\": \""
                + packageName
                + "\",\n          \"runtime\": {\n            \"command\": \"ODPS_SQL\"\n          }\n        }\n      }\n    ]\n  }\n}";
        Files.write(packageDir.resolve(packageName + ".schedule.json"), scheduleContent.getBytes());

        List<PackageValidatorResult> results = SpecPackageValidateUtil.validate(rootDir.toString());

        assertNotNull(results);
        // Validation should pass, no failure results
        boolean hasFailure = results.stream().anyMatch(result -> result.getValid() != null && !result.getValid());
        if (hasFailure) {
            String errorMessages = results.stream()
                .filter(result -> result.getValid() != null && !result.getValid())
                .map(PackageValidatorResult::getErrorMessage)
                .reduce("", (a, b) -> a + "; " + b);
            fail("Validation should pass, but has failures: " + errorMessages);
        }
    }

    // Tests for validatePreCheck method

    @Test
    public void testValidatePreCheckWithNullPath() {
        try {
            SpecPackageValidateUtil.validatePreCheck(null);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Package path cannot be empty"));
        }
    }

    @Test
    public void testValidatePreCheckWithEmptyPath() {
        try {
            SpecPackageValidateUtil.validatePreCheck("");
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Package path cannot be empty"));
        }
    }

    @Test
    public void testValidatePreCheckWithBlankPath() {
        try {
            SpecPackageValidateUtil.validatePreCheck("   ");
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Package path cannot be empty"));
        }
    }

    @Test
    public void testValidatePreCheckWithNonExistentPath() {
        String nonExistentPath = tempDir.resolve("non-existent").toString();
        try {
            SpecPackageValidateUtil.validatePreCheck(nonExistentPath);
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Specified package path does not exist"));
        }
    }

    @Test
    public void testValidatePreCheckWithRegularFile() throws IOException {
        Path regularFile = tempDir.resolve("regular-file.txt");
        Files.createFile(regularFile);
        try {
            SpecPackageValidateUtil.validatePreCheck(regularFile.toString());
            fail("Should throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Specified path is not a directory"));
        }
    }

    @Test
    public void testValidatePreCheckWithEmptyDirectory() throws IOException {
        Path emptyDir = tempDir.resolve("empty-dir");
        Files.createDirectory(emptyDir);

        List<PackageValidatorResult> results = SpecPackageValidateUtil.validatePreCheck(emptyDir.toString());

        assertNotNull(results);
        // For PRE_VALIDATE mode, an empty directory may not have validation failures
        // We just need to ensure the method doesn't throw exceptions
    }

    @Test
    public void testValidatePreCheckWithValidDataStudioFolder() throws IOException {
        // Create a valid DataStudio folder structure for pre-check
        Path dataStudioDir = tempDir.resolve("DataStudio");
        Files.createDirectory(dataStudioDir);

        // Create allowed folders for PRE_VALIDATE mode
        Path projectDir = dataStudioDir.resolve("DATAWORKS_RESOURCE");
        Files.createDirectory(projectDir);

        Path businessDir = dataStudioDir.resolve("DATAWORKS_MANUAL_BUSINESS");
        Files.createDirectory(businessDir);

        Path nodeDir = dataStudioDir.resolve("DATAWORKS_MANUAL_NODE");
        Files.createDirectory(nodeDir);

        Path workflowDir = dataStudioDir.resolve("DATAWORKS_CYCLE_WORKFLOW");
        Files.createDirectory(workflowDir);

        Path componentDir = dataStudioDir.resolve("DATAWORKS_COMPONENT");
        Files.createDirectory(componentDir);

        List<PackageValidatorResult> results = SpecPackageValidateUtil.validatePreCheck(dataStudioDir.toString());

        assertNotNull(results);
        // Validation should pass, no failure results
        boolean hasFailure = results.stream().anyMatch(result -> result.getValid() != null && !result.getValid());
        if (hasFailure) {
            String errorMessages = results.stream()
                .filter(result -> result.getValid() != null && !result.getValid())
                .map(PackageValidatorResult::getErrorMessage)
                .reduce("", (a, b) -> a + "; " + b);
            fail("Validation should pass, but has failures: " + errorMessages);
        }
    }

    @Test
    public void testValidatePreCheckWithInvalidDataStudioFolder() throws IOException {
        // Create a DataStudio folder structure with invalid folders for PRE_VALIDATE mode
        Path dataStudioDir = tempDir.resolve("DataStudio");
        Files.createDirectory(dataStudioDir);

        // Create folders that are NOT allowed in PRE_VALIDATE mode
        Path qualityDir = dataStudioDir.resolve("DataQuality");
        Files.createDirectory(qualityDir);

        Path serviceDir = dataStudioDir.resolve("DataService");
        Files.createDirectory(serviceDir);

        List<PackageValidatorResult> results = SpecPackageValidateUtil.validatePreCheck(dataStudioDir.toString());

        assertNotNull(results);
        // Validation should fail because of invalid folders
        boolean hasFailure = results.stream().anyMatch(result -> result.getValid() != null && !result.getValid());
        assertTrue("Validation should fail with invalid folders", hasFailure);
    }
}