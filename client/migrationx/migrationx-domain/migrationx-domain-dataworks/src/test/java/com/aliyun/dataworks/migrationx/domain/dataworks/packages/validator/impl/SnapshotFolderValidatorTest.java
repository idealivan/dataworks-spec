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
 * SnapshotFolderValidator测试类
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class SnapshotFolderValidatorTest {

    private SnapshotFolderValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = SnapshotFolderValidator.getInstance();
        tempDir = Files.createTempDirectory("snapshot-folder-test");
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
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithAllowedFolder() throws IOException {
        Path dirWithAllowedFolder = tempDir.resolve("dir-with-allowed-folder");
        Files.createDirectory(dirWithAllowedFolder);
        Files.createDirectory(dirWithAllowedFolder.resolve(SnapshotFolderValidator.SAVED_FILE_NAME));

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(dirWithAllowedFolder.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithNotAllowedFolder() throws IOException {
        Path dirWithNotAllowedFolder = tempDir.resolve("dir-with-not-allowed-folder");
        Files.createDirectory(dirWithNotAllowedFolder);
        Files.createDirectory(dirWithNotAllowedFolder.resolve("not-allowed-folder"));

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(dirWithNotAllowedFolder.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
    }

    @Test
    public void testValidateDirectoryWithFiles() throws IOException {
        Path dirWithFiles = tempDir.resolve("dir-with-files");
        Files.createDirectory(dirWithFiles);
        Files.createFile(dirWithFiles.resolve("file.txt"));

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(dirWithFiles.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
    }
}