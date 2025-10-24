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
 * DeletedFolderValidator测试类
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class DeletedFolderValidatorTest {

    private DeletedFolderValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = DeletedFolderValidator.getInstance();
        tempDir = Files.createTempDirectory("deleted-folder-test");
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
    }

    @Test
    public void testValidateDirectoryWithDeletedJson() throws IOException {
        Path dirWithDeletedJson = tempDir.resolve("dir-with-deleted-json");
        Files.createDirectory(dirWithDeletedJson);
        Files.createFile(dirWithDeletedJson.resolve(DeletedFolderValidator.DELETED_FILE_NAME));

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(dirWithDeletedJson.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithOtherFile() throws IOException {
        Path dirWithOtherFile = tempDir.resolve("dir-with-other-file");
        Files.createDirectory(dirWithOtherFile);
        Files.createFile(dirWithOtherFile.resolve("other-file.txt"));

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(dirWithOtherFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // 应该有两个错误：
        // 1. 不允许的文件
        // 2. 缺少必需的文件 deleted.json
        assertEquals(2, results.size());
        assertFalse(results.get(0).getValid());
        assertFalse(results.get(1).getValid());
    }

    @Test
    public void testValidateDirectoryWithFolder() throws IOException {
        Path dirWithFolder = tempDir.resolve("dir-with-folder");
        Files.createDirectory(dirWithFolder);
        Files.createDirectory(dirWithFolder.resolve("folder"));

        com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext context =
            new com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext();
        validator.validate(dirWithFolder.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // 应该有两个错误：
        // 1. 不允许的目录
        // 2. 缺少必需的文件 deleted.json
        assertEquals(2, results.size());
        assertFalse(results.get(0).getValid());
        assertFalse(results.get(1).getValid());
    }
}