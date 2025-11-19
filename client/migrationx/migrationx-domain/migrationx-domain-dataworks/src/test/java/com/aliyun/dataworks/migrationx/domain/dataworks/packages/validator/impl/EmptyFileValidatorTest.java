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
 * EmptyFileValidator测试类
 *
 * @author 莫泣
 * @date 2025-08-28
 */
public class EmptyFileValidatorTest {

    private EmptyFileValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = EmptyFileValidator.getInstance();
        tempDir = Files.createTempDirectory("empty-file-test");
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
    public void testValidateFile() throws IOException {
        Path testFile = tempDir.resolve("test-file.txt");
        Files.createFile(testFile);
        ValidateContext context = new ValidateContext();
        validator.validate(testFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    // 从EmptyFileValidatorAdditionalTest合并的测试方法开始

    @Test
    public void testValidateLargeFile() throws IOException {
        // 创建一个大文件
        Path largeFile = tempDir.resolve("large-file.txt");
        byte[] content = new byte[1024 * 1024]; // 1MB
        Files.write(largeFile, content);

        ValidateContext context = new ValidateContext();
        validator.validate(largeFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateFileWithSpecialCharactersInName() throws IOException {
        // 创建包含特殊字符的文件名
        Path specialFile = tempDir.resolve("special-文件-🎉.txt");
        Files.createFile(specialFile);

        ValidateContext context = new ValidateContext();
        validator.validate(specialFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateFileWithLongName() throws IOException {
        // 创建长文件名
        StringBuilder longName = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            longName.append("a");
        }
        Path longNameFile = tempDir.resolve(longName.toString() + ".txt");
        Files.createFile(longNameFile);

        ValidateContext context = new ValidateContext();
        validator.validate(longNameFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateNonExistentFile() throws IOException {
        // 创建一个不存在的文件路径
        Path nonExistentFile = tempDir.resolve("non-existent.txt");

        ValidateContext context = new ValidateContext();
        validator.validate(nonExistentFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateFileInDeepDirectoryStructure() throws IOException {
        // 创建深层目录结构
        Path deepDir = tempDir.resolve("level1").resolve("level2").resolve("level3").resolve("level4").resolve("level5");
        Files.createDirectories(deepDir);
        Path deepFile = deepDir.resolve("deep-file.txt");
        Files.createFile(deepFile);

        ValidateContext context = new ValidateContext();
        validator.validate(deepFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateFileWithContextWithExistingFiles() throws IOException {
        // 创建多个文件来测试上下文
        Path file1 = tempDir.resolve("file1.txt");
        Path file2 = tempDir.resolve("file2.txt");
        Path file3 = tempDir.resolve("file3.txt");
        Files.createFile(file1);
        Files.createFile(file2);
        Files.createFile(file3);

        ValidateContext context1 = new ValidateContext();
        validator.validate(file1.toFile(), context1);
        List<PackageValidatorResult> results1 = context1.getResults();

        ValidateContext context2 = new ValidateContext();
        validator.validate(file2.toFile(), context2);
        List<PackageValidatorResult> results2 = context2.getResults();

        ValidateContext context3 = new ValidateContext();
        validator.validate(file3.toFile(), context3);
        List<PackageValidatorResult> results3 = context3.getResults();

        assertNotNull(results1);
        assertNotNull(results2);
        assertNotNull(results3);
        assertTrue(results1.isEmpty());
        assertTrue(results2.isEmpty());
        assertTrue(results3.isEmpty());
    }

    @Test
    public void testValidateFileWithBinaryContent() throws IOException {
        // 创建包含二进制内容的文件
        Path binaryFile = tempDir.resolve("binary-file.bin");
        byte[] binaryContent = new byte[1024];
        for (int i = 0; i < binaryContent.length; i++) {
            binaryContent[i] = (byte)(i % 256);
        }
        Files.write(binaryFile, binaryContent);

        ValidateContext context = new ValidateContext();
        validator.validate(binaryFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }
}