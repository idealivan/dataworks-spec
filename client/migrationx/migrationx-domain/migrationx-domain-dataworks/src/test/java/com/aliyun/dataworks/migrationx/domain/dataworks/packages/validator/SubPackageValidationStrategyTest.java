package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * SubPackageValidationStrategy测试类
 *
 * @author 莫泣
 * @date 2025-08-31
 */
public class SubPackageValidationStrategyTest {

    private TestSubPackageValidationStrategy strategy;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        strategy = new TestSubPackageValidationStrategy();
        tempDir = Files.createTempDirectory("sub-package-strategy-test");
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
    public void testSupport() {
        assertTrue(strategy.support("test.type"));
        assertFalse(strategy.support("other.type"));
        assertFalse(strategy.support(null));
    }

    @Test
    public void testValidateSubPackages() throws IOException {
        Path packageDir = tempDir.resolve("package-dir");
        Files.createDirectory(packageDir);

        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        assertNotNull(results);
        assertTrue(results.isEmpty());
    }

    /**
     * 测试用的SubPackageValidationStrategy实现
     */
    private static class TestSubPackageValidationStrategy implements SubPackageValidationStrategy {
        @Override
        public boolean support(String parentPackageType) {
            return "test.type".equals(parentPackageType);
        }

        @Override
        public void validateSubPackages(File packageDir, ValidateContext context) {
            // 空实现
        }
    }
}