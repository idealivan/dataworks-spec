package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidator;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * DataWorksFolderValidator测试类
 * 用于测试DataWorksFolderValidator类的验证功能
 *
 * @author 莫泣
 * @date 2025-08-31
 */
public class DataWorksFolderValidatorTest {

    private DataWorksFolderValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = DataWorksFolderValidator.getInstance();
        tempDir = Files.createTempDirectory("dataworks-folder-test");
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
        // 创建空目录
        Path emptyDir = tempDir.resolve("empty-dir");
        Files.createDirectory(emptyDir);

        // 执行验证
        ValidateContext context = new ValidateContext();
        validator.validate(emptyDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // 验证结果
        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithAllowedFolder() throws IOException {
        // 创建包含允许的文件夹的目录
        Path dirWithAllowedFolder = tempDir.resolve("dir-with-allowed-folder");
        Files.createDirectory(dirWithAllowedFolder);
        Files.createDirectory(dirWithAllowedFolder.resolve(DataWorksFolderValidator.SNAPSHOT_FILE_NAME));

        // 执行验证
        ValidateContext context = new ValidateContext();
        validator.validate(dirWithAllowedFolder.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // 验证结果
        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithDeletedFolder() throws IOException {
        // 创建包含.deleted文件夹的目录
        Path dirWithDeletedFolder = tempDir.resolve("dir-with-deleted-folder");
        Files.createDirectory(dirWithDeletedFolder);

        // 创建.deleted文件夹并添加必需的deleted.json文件
        Path deletedFolder = dirWithDeletedFolder.resolve(DataWorksFolderValidator.DELETED_FILE_NAME);
        Files.createDirectory(deletedFolder);
        Files.createFile(deletedFolder.resolve(DeletedFolderValidator.DELETED_FILE_NAME));

        // 执行验证
        ValidateContext context = new ValidateContext();
        validator.validate(dirWithDeletedFolder.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // 验证结果
        assertNotNull(results);
        // DataWorksFolderValidator允许.deleted文件夹，所以不应该有错误
        // 但可能有子目录的验证结果
        assertTrue("验证结果应该为空或所有结果都有效",
            results.isEmpty() || results.stream().allMatch(result -> result.getValid() == null || result.getValid()));
    }

    @Test
    public void testValidateDirectoryWithNotAllowedFolder() throws IOException {
        // 创建包含不允许的文件夹的目录
        Path dirWithNotAllowedFolder = tempDir.resolve("dir-with-not-allowed-folder");
        Files.createDirectory(dirWithNotAllowedFolder);
        Files.createDirectory(dirWithNotAllowedFolder.resolve("not-allowed-folder"));

        // 执行验证
        ValidateContext context = new ValidateContext();
        validator.validate(dirWithNotAllowedFolder.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // 验证结果
        assertNotNull(results);
        assertTrue(results.isEmpty()); // DataWorksFolderValidator允许所有非隐藏目录
    }

    @Test
    public void testValidateDirectoryWithHiddenFolder() throws IOException {
        // 创建包含隐藏文件夹的目录
        Path dirWithHiddenFolder = tempDir.resolve("dir-with-hidden-folder");
        Files.createDirectory(dirWithHiddenFolder);
        Files.createDirectory(dirWithHiddenFolder.resolve(".hidden-folder"));

        // 执行验证
        ValidateContext context = new ValidateContext();
        validator.validate(dirWithHiddenFolder.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // 验证结果
        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
    }

    @Test
    public void testValidateDirectoryWithFiles() throws IOException {
        // 创建包含文件的目录
        Path dirWithFiles = tempDir.resolve("dir-with-files");
        Files.createDirectory(dirWithFiles);
        Files.createFile(dirWithFiles.resolve("file.txt"));

        // 执行验证
        ValidateContext context = new ValidateContext();
        validator.validate(dirWithFiles.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // 验证结果
        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
    }

    @Test
    public void testGetChildValidatorsForDeletedFolder() throws IOException {
        // 创建包含.deleted文件夹的目录
        Path dirWithDeletedFolder = tempDir.resolve("dir-with-deleted-folder");
        Files.createDirectory(dirWithDeletedFolder);
        Path deletedFolder = dirWithDeletedFolder.resolve(DataWorksFolderValidator.DELETED_FILE_NAME);
        Files.createDirectory(deletedFolder);

        // 获取子验证器
        List<PackageValidator> childValidators = validator.getChildValidators(deletedFolder.toFile(),
            new ValidateContext());

        // 验证结果
        assertNotNull(childValidators);
        assertEquals(1, childValidators.size());
        assertTrue(childValidators.get(0) instanceof DeletedFolderValidator);
    }

    @Test
    public void testGetChildValidatorsForSnapshotFolder() throws IOException {
        // 创建包含.snapshots文件夹的目录
        Path dirWithSnapshotFolder = tempDir.resolve("dir-with-snapshot-folder");
        Files.createDirectory(dirWithSnapshotFolder);
        Path snapshotFolder = dirWithSnapshotFolder.resolve(DataWorksFolderValidator.SNAPSHOT_FILE_NAME);
        Files.createDirectory(snapshotFolder);

        // 获取子验证器
        List<PackageValidator> childValidators = validator.getChildValidators(snapshotFolder.toFile(),
            new ValidateContext());

        // 验证结果
        assertNotNull(childValidators);
        assertEquals(1, childValidators.size());
        assertTrue(childValidators.get(0) instanceof SnapshotFolderValidator);
    }

    @Test
    public void testGetChildValidatorsForOtherFolder() throws IOException {
        // 创建包含其他文件夹的目录
        Path dirWithOtherFolder = tempDir.resolve("dir-with-other-folder");
        Files.createDirectory(dirWithOtherFolder);
        Path otherFolder = dirWithOtherFolder.resolve("other-folder");
        Files.createDirectory(otherFolder);

        // 获取子验证器
        List<PackageValidator> childValidators = validator.getChildValidators(otherFolder.toFile(),
            new ValidateContext());

        // 验证结果
        assertNotNull(childValidators);
        assertEquals(1, childValidators.size());
        assertTrue(childValidators.get(0) instanceof CommonFolderValidator);
    }
}