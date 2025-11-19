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
 * CommonFolderValidator测试类
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class CommonFolderValidatorTest {

    private CommonFolderValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = CommonFolderValidator.getInstance();
        tempDir = Files.createTempDirectory("common-folder-test");
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
    public void testValidateDirectoryWithNormalFolder() throws IOException {
        Path dirWithNormalFolder = tempDir.resolve("dir-with-normal-folder");
        Files.createDirectory(dirWithNormalFolder);
        Files.createDirectory(dirWithNormalFolder.resolve("normal-folder"));

        ValidateContext context = new ValidateContext();
        validator.validate(dirWithNormalFolder.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    @Test
    public void testValidateDirectoryWithHiddenFolder() throws IOException {
        Path dirWithHiddenFolder = tempDir.resolve("dir-with-hidden-folder");
        Files.createDirectory(dirWithHiddenFolder);
        Files.createDirectory(dirWithHiddenFolder.resolve(".hidden-folder"));

        ValidateContext context = new ValidateContext();
        validator.validate(dirWithHiddenFolder.toFile(), context);
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

        ValidateContext context = new ValidateContext();
        validator.validate(dirWithFiles.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        // CommonFolderValidator不允许任何文件，所以应该有两个错误：
        // 1. 不允许的文件
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
    }

    @Test
    public void testGetChildValidatorsForPackage() throws IOException {
        Path packageDir = tempDir.resolve("package-dir");
        Files.createDirectory(packageDir);
        Path dataworksDir = packageDir.resolve(".dataworks");
        Files.createDirectory(dataworksDir);
        Files.createFile(dataworksDir.resolve("metadata.json"));
        Files.createFile(dataworksDir.resolve("test.type"));

        List<PackageValidator> childValidators = validator.getChildValidators(packageDir.toFile(),
            new ValidateContext());

        assertNotNull(childValidators);
        assertEquals(1, childValidators.size());
        assertTrue(childValidators.get(0) instanceof SpecPackageValidator);
    }

    @Test
    public void testGetChildValidatorsForNonPackage() throws IOException {
        Path nonPackageDir = tempDir.resolve("non-package-dir");
        Files.createDirectory(nonPackageDir);
        Files.createDirectory(nonPackageDir.resolve("normal-folder"));

        List<PackageValidator> childValidators = validator.getChildValidators(nonPackageDir.toFile(),
            new ValidateContext());

        assertNotNull(childValidators);
        assertEquals(1, childValidators.size());
        assertTrue(childValidators.get(0) instanceof CommonFolderValidator);
    }
}