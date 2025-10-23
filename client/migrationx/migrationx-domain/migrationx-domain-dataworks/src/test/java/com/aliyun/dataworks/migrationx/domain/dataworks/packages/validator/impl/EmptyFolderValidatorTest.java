package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.PackageValidatorResult;
import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * EmptyFolderValidator测试类
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class EmptyFolderValidatorTest {

    private EmptyFolderValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = EmptyFolderValidator.getInstance();
        tempDir = Files.createTempDirectory("empty-folder-test");
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
        ValidateContext context = new ValidateContext();
        validator.validate(emptyDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithFiles() throws IOException {
        Path dirWithFiles = tempDir.resolve("dir-with-files");
        Files.createDirectory(dirWithFiles);
        Files.createFile(dirWithFiles.resolve("file.txt"));

        ValidateContext context = new ValidateContext();
        validator.validate(dirWithFiles.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithFolders() throws IOException {
        Path dirWithFolders = tempDir.resolve("dir-with-folders");
        Files.createDirectory(dirWithFolders);
        Files.createDirectory(dirWithFolders.resolve("folder"));

        ValidateContext context = new ValidateContext();
        validator.validate(dirWithFolders.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    // 从EmptyFolderValidatorAdditionalTest合并的测试方法开始

    @Test
    public void testValidateDeepDirectoryStructure() throws IOException {
        // 创建深层目录结构
        Path deepDir = tempDir.resolve("level1").resolve("level2").resolve("level3").resolve("level4").resolve("level5");
        Files.createDirectories(deepDir);

        ValidateContext context = new ValidateContext();
        validator.validate(deepDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithSpecialCharactersInName() throws IOException {
        // 创建包含特殊字符的目录名
        Path specialDir = tempDir.resolve("special-目录-🎉");
        Files.createDirectory(specialDir);

        ValidateContext context = new ValidateContext();
        validator.validate(specialDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithLongName() throws IOException {
        // 创建长目录名
        StringBuilder longName = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            longName.append("a");
        }
        Path longNameDir = tempDir.resolve(longName.toString());
        Files.createDirectory(longNameDir);

        ValidateContext context = new ValidateContext();
        validator.validate(longNameDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateNonExistentDirectory() throws IOException {
        // 创建一个不存在的目录路径
        Path nonExistentDir = tempDir.resolve("non-existent");

        ValidateContext context = new ValidateContext();
        validator.validate(nonExistentDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // 对于不存在的目录，测试通过，不需要额外的断言
    }

    @Test
    public void testValidateDirectoryWithNestedStructure() throws IOException {
        // 创建嵌套目录结构
        Path parentDir = tempDir.resolve("parent");
        Files.createDirectory(parentDir);

        Path childDir1 = parentDir.resolve("child1");
        Files.createDirectory(childDir1);
        Files.createFile(childDir1.resolve("file1.txt"));
        Files.createFile(childDir1.resolve("file2.txt"));

        Path childDir2 = parentDir.resolve("child2");
        Files.createDirectory(childDir2);
        Path grandChildDir = childDir2.resolve("grandchild");
        Files.createDirectory(grandChildDir);
        Files.createFile(grandChildDir.resolve("file3.txt"));

        ValidateContext context = new ValidateContext();
        validator.validate(parentDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithContextWithExistingDirectories() throws IOException {
        // 创建多个目录来测试上下文
        Path dir1 = tempDir.resolve("dir1");
        Path dir2 = tempDir.resolve("dir2");
        Path dir3 = tempDir.resolve("dir3");
        Files.createDirectory(dir1);
        Files.createDirectory(dir2);
        Files.createDirectory(dir3);

        ValidateContext context1 = new ValidateContext();
        validator.validate(dir1.toFile(), context1);
        List<PackageValidatorResult> results1 = context1.getResults();

        ValidateContext context2 = new ValidateContext();
        validator.validate(dir2.toFile(), context2);
        List<PackageValidatorResult> results2 = context2.getResults();

        ValidateContext context3 = new ValidateContext();
        validator.validate(dir3.toFile(), context3);
        List<PackageValidatorResult> results3 = context3.getResults();

        assertNotNull(results1);
        assertNotNull(results2);
        assertNotNull(results3);
        assertTrue(results1.isEmpty());
        assertTrue(results2.isEmpty());
        assertTrue(results3.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithHiddenFilesAndFolders() throws IOException {
        // 创建包含隐藏文件和文件夹的目录
        Path dirWithHidden = tempDir.resolve("dir-with-hidden");
        Files.createDirectory(dirWithHidden);
        Files.createFile(dirWithHidden.resolve(".hidden-file"));
        Files.createDirectory(dirWithHidden.resolve(".hidden-folder"));
        Files.createFile(dirWithHidden.resolve(".hidden-folder").resolve("file-in-hidden-folder.txt"));

        ValidateContext context = new ValidateContext();
        validator.validate(dirWithHidden.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithReadOnlyFiles() throws IOException {
        // 创建包含只读文件的目录
        Path dirWithReadOnly = tempDir.resolve("dir-with-read-only");
        Files.createDirectory(dirWithReadOnly);
        Path readOnlyFile = dirWithReadOnly.resolve("read-only-file.txt");
        Files.createFile(readOnlyFile);
        // 设置为只读
        readOnlyFile.toFile().setReadOnly();

        ValidateContext context = new ValidateContext();
        validator.validate(dirWithReadOnly.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }
}