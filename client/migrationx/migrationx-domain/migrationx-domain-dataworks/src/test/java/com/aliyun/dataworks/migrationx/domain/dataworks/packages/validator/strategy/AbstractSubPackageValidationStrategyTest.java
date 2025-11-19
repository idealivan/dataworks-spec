package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.strategy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.ValidateContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * AbstractSubPackageValidationStrategy测试类
 *
 * @author 莫泣
 * @date 2025-08-31
 */
public class AbstractSubPackageValidationStrategyTest {

    private TestSubPackageValidationStrategy strategy;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        strategy = new TestSubPackageValidationStrategy();
        tempDir = Files.createTempDirectory("abstract-sub-package-strategy-test");
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
    public void testIsSpecificTypePackageWithValidPackage() throws IOException {
        // 创建包目录结构
        Path packageDir = tempDir.resolve("package-dir");
        Files.createDirectory(packageDir);
        Path dataworksDir = packageDir.resolve(AbstractSubPackageValidationStrategy.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);
        Files.createFile(dataworksDir.resolve("test.type"));

        assertTrue(strategy.testIsSpecificTypePackage(packageDir.toFile(), "test.type", new ValidateContext()));
    }

    @Test
    public void testIsSpecificTypePackageWithInvalidPackage() throws IOException {
        // 创建包目录结构，但没有目标类型文件
        Path packageDir = tempDir.resolve("package-dir");
        Files.createDirectory(packageDir);
        Path dataworksDir = packageDir.resolve(AbstractSubPackageValidationStrategy.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);
        Files.createFile(dataworksDir.resolve("other.type"));

        assertFalse(strategy.testIsSpecificTypePackage(packageDir.toFile(), "test.type", new ValidateContext()));
    }

    @Test
    public void testIsSpecificTypePackageWithNonExistentDataworksDir() throws IOException {
        // 创建包目录，但没有.dataworks子目录
        Path packageDir = tempDir.resolve("package-dir");
        Files.createDirectory(packageDir);

        assertFalse(strategy.testIsSpecificTypePackage(packageDir.toFile(), "test.type", new ValidateContext()));
    }

    @Test
    public void testIsSpecificTypePackageWithEmptyDataworksDir() throws IOException {
        // 创建包目录和空的.dataworks子目录
        Path packageDir = tempDir.resolve("package-dir");
        Files.createDirectory(packageDir);
        Path dataworksDir = packageDir.resolve(AbstractSubPackageValidationStrategy.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        assertFalse(strategy.testIsSpecificTypePackage(packageDir.toFile(), "test.type", new ValidateContext()));
    }

    @Test
    public void testIsSpecificTypePackageWithNullPackageDir() {
        assertFalse(strategy.testIsSpecificTypePackage(null, "test.type", new ValidateContext()));
    }

    /**
     * 测试用的SubPackageValidationStrategy实现
     */
    private static class TestSubPackageValidationStrategy extends AbstractSubPackageValidationStrategy {
        @Override
        public boolean support(String parentPackageType) {
            return false;
        }

        @Override
        public void validateSubPackages(
            File packageDir, ValidateContext context) {
            // 空实现
        }

        /**
         * 公开isSpecificTypePackage方法用于测试
         */
        public boolean testIsSpecificTypePackage(File packageDir, String expectedType, ValidateContext context) {
            return isSpecificTypePackage(packageDir, expectedType, context);
        }
    }
}