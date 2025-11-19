package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * RootFolderValidator Test Class
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class RootFolderValidatorTest {

    private RootFolderValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = RootFolderValidator.getInstance();
        tempDir = Files.createTempDirectory("root-folder-test");
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
    public void testValidateEmptyDirectory() throws IOException {
        Path emptyDir = tempDir.resolve("empty-dir");
        Files.createDirectory(emptyDir);
        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(emptyDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
        assertTrue(results.get(0).getErrorMessage().contains("Missing required file"));
    }

    @Test
    public void testValidateDirectoryWithAllowedDirectory() throws IOException {
        Path dirWithAllowedDir = tempDir.resolve("dir-with-allowed-dir");
        Files.createDirectory(dirWithAllowedDir);
        Files.createDirectory(dirWithAllowedDir.resolve(RootFolderValidator.DATA_STUDIO_FILE_NAME));

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(dirWithAllowedDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
        assertTrue(results.get(0).getErrorMessage().contains("Missing required file"));
    }

    @Test
    public void testValidateDirectoryWithAllowedFile() throws IOException {
        Path dirWithAllowedFile = tempDir.resolve("dir-with-allowed-file");
        Files.createDirectory(dirWithAllowedFile);
        Files.createFile(dirWithAllowedFile.resolve(RootFolderValidator.SPEC_FORMAT_FILE_NAME));

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(dirWithAllowedFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithNotAllowedDirectory() throws IOException {
        Path dirWithNotAllowedDir = tempDir.resolve("dir-with-not-allowed-dir");
        Files.createDirectory(dirWithNotAllowedDir);
        Files.createDirectory(dirWithNotAllowedDir.resolve("not-allowed-dir"));

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(dirWithNotAllowedDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(2, results.size());
        assertFalse(results.get(0).getValid());
        assertFalse(results.get(1).getValid());
    }

    @Test
    public void testValidateDirectoryWithNotAllowedFile() throws IOException {
        Path dirWithNotAllowedFile = tempDir.resolve("dir-with-not-allowed-file");
        Files.createDirectory(dirWithNotAllowedFile);
        Files.createFile(dirWithNotAllowedFile.resolve("not-allowed-file.txt"));

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(dirWithNotAllowedFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(2, results.size());
        assertFalse(results.get(0).getValid());
        assertFalse(results.get(1).getValid());
    }

    @Test
    public void testGetChildValidatorsForDataStudio() throws IOException {
        Path dirWithDataStudio = tempDir.resolve("dir-with-datastudio");
        Files.createDirectory(dirWithDataStudio);
        Files.createDirectory(dirWithDataStudio.resolve(RootFolderValidator.DATA_STUDIO_FILE_NAME));

        List<PackageValidator> childValidators = validator.getChildValidators(
            dirWithDataStudio.resolve(RootFolderValidator.DATA_STUDIO_FILE_NAME).toFile(),
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext());

        assertNotNull(childValidators);
        assertEquals(1, childValidators.size());
        assertTrue(childValidators.get(0) instanceof DataStudioFolderValidator);
    }

    @Test
    public void testGetChildValidatorsForOtherDirectory() throws IOException {
        Path dirWithOther = tempDir.resolve("dir-with-other");
        Files.createDirectory(dirWithOther);
        Files.createDirectory(dirWithOther.resolve("other-dir"));

        List<PackageValidator> childValidators = validator.getChildValidators(
            dirWithOther.resolve("other-dir").toFile(), new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext());

        assertNotNull(childValidators);
        assertEquals(1, childValidators.size());
        assertTrue(childValidators.get(0) instanceof EmptyFolderValidator);
    }
}