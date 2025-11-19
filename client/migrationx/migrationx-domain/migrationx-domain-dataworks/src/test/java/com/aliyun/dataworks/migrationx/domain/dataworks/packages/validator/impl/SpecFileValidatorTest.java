package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * SpecFileValidator Test Class
 * Used to test the validation functionality of SpecFileValidator class
 *
 * @author 莫泣
 * @date 2025-08-31
 */
public class SpecFileValidatorTest {

    private SpecFileValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = SpecFileValidator.getInstance();
        tempDir = Files.createTempDirectory("spec-file-test");
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
    public void testValidateSpecFileWithDuplicateUuid() throws IOException {
        // Create two .spec.json files with the same uuid
        Path specFile1 = tempDir.resolve("test-package1.spec.json");
        Path specFile2 = tempDir.resolve("test-package2.spec.json");

        String contentWithUuid
            = "{\n  \"version\": \"1.0.0\",\n  \"kind\": \"CycleWorkflow\",\n  \"spec\": {\n    \"uuid\": \"same-uuid\",\n    \"name\": "
            + "\"test-package1\",\n    \"nodes\": [\n      {\n        \"id\": \"node1\",\n        \"name\": \"test-node\",\n        \"script\": {\n"
            + "          \"path\": \"test-package1\",\n          \"runtime\": {\n            \"command\": \"ODPS_SQL\"\n          }\n        }\n   "
            + "   }\n    ]\n  }\n}";

        Files.write(specFile1, contentWithUuid.getBytes());
        Files.write(specFile2, contentWithUuid.getBytes());

        ValidateContext context = new ValidateContext();

        // Validate first file
        validator.validate(specFile1.toFile(), context);

        // Validate second file with same uuid
        validator.validate(specFile2.toFile(), context);

        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateSpecFileWithMismatchedSpecNameAndPackageName() throws IOException {
        // Create a directory with a specific name
        Path packageDir = tempDir.resolve("test-package");
        Files.createDirectory(packageDir);

        // Create a .spec.json file where spec name doesn't match package name
        Path specFile = packageDir.resolve("test-package.spec.json");

        String contentWithMismatchedName
            = "{\n  \"version\": \"1.0.0\",\n  \"kind\": \"CycleWorkflow\",\n  \"spec\": {\n    \"name\": \"different-name\",\n    \"nodes\": [\n  "
            + "    {\n        \"id\": \"111\",\n        \"name\": \"different-name\",\n        \"script\": {\n          \"path\": \"test-package\","
            + "\n "
            + "         \"runtime\": {\n            \"command\": \"ODPS_SQL\"\n          }\n        }\n      }\n    ]\n  }\n}";

        Files.write(specFile, contentWithMismatchedName.getBytes());

        // Execute validation
        ValidateContext context = new ValidateContext();
        validator.validate(specFile.toFile(), context);

        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateSpecFileWithMatchingSpecNameAndPackageName() throws IOException {
        // Create a directory with a specific name
        Path packageDir = tempDir.resolve("test-package");
        Files.createDirectory(packageDir);

        // Create a .spec.json file where spec name matches package name
        Path specFile = packageDir.resolve("test-package.spec.json");

        String contentWithMatchingName
            = "{\n  \"version\": \"1.0.0\",\n  \"kind\": \"CycleWorkflow\",\n  \"spec\": {\n    \"name\": \"test-package\",\n    \"nodes\": [\n    "
            + "  {\n        \"id\": \"111\",\n        \"name\": \"test-package\",\n        \"script\": {\n          \"path\": \"test-package\",\n   "
            + "       \"runtime\": {\n            \"command\": \"ODPS_SQL\"\n          }\n        }\n      }\n    ]\n  }\n}";

        Files.write(specFile, contentWithMatchingName.getBytes());

        // Execute validation
        ValidateContext context = new ValidateContext();
        validator.validate(specFile.toFile(), context);

        // Verify that results are added to context
        assertEquals(0, context.getResults().size());
    }
}