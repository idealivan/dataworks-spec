package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.impl;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Deque;
import java.util.List;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.AbstractFolderValidator;
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
 * AbstractFolderValidator测试类
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class AbstractFolderValidatorTest {

    private TestFolderValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = new TestFolderValidator();
        tempDir = Files.createTempDirectory("abstract-folder-test");
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

    // Helper method to access private fileStack field
    private Deque<File> getFileStack(ValidateContext context) {
        try {
            Field field = context.getClass().getDeclaredField("fileStack");
            field.setAccessible(true);
            return (Deque<File>)field.get(context);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testValidateNonExistentFile() throws IOException {
        Path nonExistentPath = tempDir.resolve("non-existent");
        ValidateContext context = new ValidateContext();
        validator.validate(nonExistentPath.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
    }

    @Test
    public void testValidateRegularFile() throws IOException {
        Path regularFile = tempDir.resolve("regular-file.txt");
        Files.createFile(regularFile);
        ValidateContext context = new ValidateContext();
        validator.validate(regularFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
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
    public void testValidateDirectoryWithRequiredFile() throws IOException {
        Path dirWithFile = tempDir.resolve("dir-with-file");
        Files.createDirectory(dirWithFile);
        Files.createFile(dirWithFile.resolve("required-file.txt"));

        TestFolderValidator validatorWithRequiredFile = new TestFolderValidator();
        validatorWithRequiredFile.addRequiredFile("required-file.txt");
        // 也需要添加允许的文件，否则会报错
        validatorWithRequiredFile.addAllowedFile("required-file.txt");

        ValidateContext context = new ValidateContext();
        validatorWithRequiredFile.validate(dirWithFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithoutRequiredFile() throws IOException {
        Path dirWithoutFile = tempDir.resolve("dir-without-file");
        Files.createDirectory(dirWithoutFile);

        TestFolderValidator validatorWithRequiredFile = new TestFolderValidator();
        validatorWithRequiredFile.addRequiredFile("required-file.txt");

        ValidateContext context = new ValidateContext();
        validatorWithRequiredFile.validate(dirWithoutFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
    }

    @Test
    public void testValidateDirectoryWithDisallowedFile() throws IOException {
        Path dirWithDisallowedFile = tempDir.resolve("dir-with-disallowed-file");
        Files.createDirectory(dirWithDisallowedFile);
        Files.createFile(dirWithDisallowedFile.resolve("disallowed-file.txt"));

        TestFolderValidator validatorWithAllowedFiles = new TestFolderValidator();
        validatorWithAllowedFiles.addAllowedFile("allowed-file.txt");

        ValidateContext context = new ValidateContext();
        validatorWithAllowedFiles.validate(dirWithDisallowedFile.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
    }

    @Test
    public void testValidateDirectoryWithDisallowedFolder() throws IOException {
        Path dirWithDisallowedFolder = tempDir.resolve("dir-with-disallowed-folder");
        Files.createDirectory(dirWithDisallowedFolder);
        Path disallowedSubDir = dirWithDisallowedFolder.resolve("disallowed-subdir");
        Files.createDirectory(disallowedSubDir);

        TestFolderValidator validatorWithAllowedFolders = new TestFolderValidator();
        validatorWithAllowedFolders.addAllowedFolder("allowed-subdir");

        ValidateContext context = new ValidateContext();
        validatorWithAllowedFolders.validate(dirWithDisallowedFolder.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
    }

    @Test
    public void testValidateDirectoryWithNestedStructure() throws IOException {
        Path nestedDir = tempDir.resolve("nested-dir");
        Files.createDirectory(nestedDir);
        Path subDir = nestedDir.resolve("sub-dir");
        Files.createDirectory(subDir);
        Files.createFile(subDir.resolve("file-in-subdir.txt"));

        TestFolderValidator validatorWithNestedStructure = new TestFolderValidator();
        validatorWithNestedStructure.addAllowedFolder("sub-dir");
        validatorWithNestedStructure.addAllowedFile("file-in-subdir.txt");

        ValidateContext context = new ValidateContext();
        validatorWithNestedStructure.validate(nestedDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateWithIOException() throws IOException {
        // 创建一个不存在的目录路径
        Path nonExistentDir = tempDir.resolve("non-existent-dir");

        ValidateContext context = new ValidateContext();
        validator.validate(nonExistentDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // 验证结果不为空
        assertFalse(results.isEmpty());
        // 验证context在验证完成后被正确清理
        assertEquals(0, getFileStack(context).size());
    }

    @Test
    public void testValidateWithContextFileStackOperations() throws IOException {
        Path testDir = tempDir.resolve("test-dir");
        Files.createDirectory(testDir);

        // 创建一个自定义验证器来验证context操作
        AbstractFolderValidator customValidator = new AbstractFolderValidator() {
            @Override
            protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
                // Allow all folders
                return PackageValidatorResult.success();
            }

            @Override
            protected PackageValidatorResult isAllowedFiles(File file, ValidateContext context) {
                // Allow all files
                return PackageValidatorResult.success();
            }

            @Override
            protected java.util.Set<String> getRequiredFolders(ValidateContext context) {
                return new java.util.HashSet<>();
            }

            @Override
            protected java.util.Set<String> getRequiredFiles(ValidateContext context) {
                return new java.util.HashSet<>();
            }

            @Override
            protected void doValidate(File file, ValidateContext context) {
                // 验证context中的文件栈是否正确
                assertEquals(file, context.getCurrentFile());
            }
        };

        ValidateContext context = new ValidateContext();
        customValidator.validate(testDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // 验证context在验证完成后被正确清理
        assertEquals(0, getFileStack(context).size());
    }

    @Test
    public void testValidateWithNestedContext() throws IOException {
        // 创建测试目录
        Path testDir = tempDir.resolve("test-dir");
        Files.createDirectory(testDir);

        // 创建带有嵌套调用的验证器
        AbstractFolderValidator nestedValidator = new AbstractFolderValidator() {
            @Override
            protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
                // Allow all folders
                return PackageValidatorResult.success();
            }

            @Override
            protected PackageValidatorResult isAllowedFiles(File file, ValidateContext context) {
                // Allow all files
                return PackageValidatorResult.success();
            }

            @Override
            protected java.util.Set<String> getRequiredFolders(ValidateContext context) {
                return new java.util.HashSet<>();
            }

            @Override
            protected java.util.Set<String> getRequiredFiles(ValidateContext context) {
                return new java.util.HashSet<>();
            }

            @Override
            protected void doValidate(File file, ValidateContext context) {
                // 模拟嵌套验证调用
                if (getFileStack(context).size() == 1) {
                    // 第一次调用，再调用一次自己
                    validate(file, context);
                }
            }
        };

        ValidateContext context = new ValidateContext();
        nestedValidator.validate(testDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // 验证context在嵌套调用完成后被正确清理
        assertEquals(0, getFileStack(context).size());
    }

    @Test
    public void testValidateWithSpecialCharactersInDirectoryName() throws IOException {
        // 创建包含特殊字符的目录名
        Path specialDir = tempDir.resolve("special-目录-🎉");
        Files.createDirectory(specialDir);

        ValidateContext context = new ValidateContext();
        validator.validate(specialDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // 验证context在验证完成后被正确清理
        assertEquals(0, getFileStack(context).size());
    }

    @Test
    public void testValidateWithReadOnlyDirectory() throws IOException {
        // 创建只读目录
        Path readOnlyDir = tempDir.resolve("readonly-dir");
        Files.createDirectory(readOnlyDir);
        readOnlyDir.toFile().setReadOnly();

        ValidateContext context = new ValidateContext();
        validator.validate(readOnlyDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // 验证context在验证完成后被正确清理
        assertEquals(0, getFileStack(context).size());
    }

    @Test
    public void testValidateWithSymlinkDirectory() throws IOException {
        // 创建普通目录和符号链接
        Path realDir = tempDir.resolve("real-dir");
        Files.createDirectory(realDir);

        Path symlinkDir = tempDir.resolve("symlink-dir");
        Files.createSymbolicLink(symlinkDir, realDir);

        ValidateContext context = new ValidateContext();
        validator.validate(symlinkDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // 验证context在验证完成后被正确清理
        assertEquals(0, getFileStack(context).size());
    }

    /**
     * 测试用的AbstractFolderValidator实现
     */
    private static class TestFolderValidator extends AbstractFolderValidator {
        private final java.util.Set<String> allowedFolders = new java.util.HashSet<>();
        private final java.util.Set<String> allowedFiles = new java.util.HashSet<>();
        private final java.util.Set<String> requiredFolders = new java.util.HashSet<>();
        private final java.util.Set<String> requiredFiles = new java.util.HashSet<>();

        public void addAllowedFolder(String folder) {
            allowedFolders.add(folder);
        }

        public void addAllowedFile(String file) {
            allowedFiles.add(file);
        }

        public void addRequiredFolder(String folder) {
            requiredFolders.add(folder);
        }

        public void addRequiredFile(String file) {
            requiredFiles.add(file);
        }

        @Override
        protected PackageValidatorResult isAllowedFolders(File file, ValidateContext context) {
            return allowedFolders.contains(file.getName()) ? PackageValidatorResult.success() :
                PackageValidatorResult.failed(
                    com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode.FOLDER_NOT_EXIST, context,
                    "path", file.getName());
        }

        @Override
        protected PackageValidatorResult isAllowedFiles(File file, ValidateContext context) {
            return allowedFiles.contains(file.getName()) ? PackageValidatorResult.success() :
                PackageValidatorResult.failed(
                    com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.PackageValidateErrorCode.FILE_NOT_EXIST, context,
                    "path", file.getName());
        }

        @Override
        protected java.util.Set<String> getRequiredFolders(ValidateContext context) {
            return requiredFolders;
        }

        @Override
        protected java.util.Set<String> getRequiredFiles(ValidateContext context) {
            return requiredFiles;
        }

        @Override
        protected void doValidate(File file, ValidateContext context) {
            // Call the parent class's doValidate method to perform the actual validation
            super.doValidate(file, context);
        }
    }
}