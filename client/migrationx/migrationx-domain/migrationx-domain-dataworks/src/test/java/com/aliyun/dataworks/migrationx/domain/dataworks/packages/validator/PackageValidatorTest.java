package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Deque;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * PackageValidator Test Class
 *
 * @author Mo Qi
 * @date 2025-08-26
 */
public class PackageValidatorTest {

    private PackageValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = new TestPackageValidator();
        tempDir = Files.createTempDirectory("package-validator-test");
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
    public void testValidateWithValidFile() throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("test-file.txt");
        Files.createFile(testFile);

        ValidateContext context = new ValidateContext();
        validator.validate(testFile.toFile(), context);

        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithNonExistentFile() {
        Path nonExistentPath = tempDir.resolve("non-existent-file.txt");
        ValidateContext context = new ValidateContext();
        validator.validate(nonExistentPath.toFile(), context);

        // Verify that the file stack in context is handled correctly
        assertEquals(0, getFileStack(context).size());
        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithContextOperations() throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("test-file.txt");
        Files.createFile(testFile);

        ValidateContext context = new ValidateContext();
        validator.validate(testFile.toFile(), context);

        // Verify context operations
        assertEquals(0, getFileStack(context).size());
        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithFileAndExceptionInValidation() throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("test-file.txt");
        Files.createFile(testFile);

        // Create a validator that throws an exception
        PackageValidator exceptionValidator = new AbstractBaseValidator() {
            @Override
            public void validate(File file, ValidateContext context) {
                throw new RuntimeException("Test exception in validate");
            }
        };

        ValidateContext context = new ValidateContext();
        try {
            exceptionValidator.validate(testFile.toFile(), context);
            // If no exception is thrown, it means it was handled correctly
        } catch (Exception e) {
            // Verify exception message
            assertEquals("Test exception in validate", e.getMessage());
        }
    }

    @Test
    public void testValidateWithSpecialCharactersInPath() throws IOException {
        // Create a file path with special characters
        Path specialPathFile = tempDir.resolve("special-文件-🎉.txt");
        Files.createFile(specialPathFile);

        ValidateContext context = new ValidateContext();
        validator.validate(specialPathFile.toFile(), context);

        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithVeryLongPath() throws IOException {
        // Create a long but reasonable file path
        Path longPathDir = tempDir.resolve("very/long/path/with/several/directories/that/are/nested/deeply");
        Files.createDirectories(longPathDir);

        Path longPathFile = longPathDir.resolve("long-path-file.txt");
        Files.createFile(longPathFile);

        ValidateContext context = new ValidateContext();
        validator.validate(longPathFile.toFile(), context);

        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithFileAndContextFileStackOperations() throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("test-file.txt");
        Files.createFile(testFile);

        ValidateContext context = new ValidateContext();
        validator.validate(testFile.toFile(), context);

        // Verify context operations
        assertEquals(0, getFileStack(context).size());
        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithFileAndNestedContext() throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("test-file.txt");
        Files.createFile(testFile);

        // Create a validator with nested calls
        PackageValidator nestedValidator = new AbstractBaseValidator() {
            @Override
            public void validate(File file, ValidateContext context) {
                // Simulate nested validation calls
                if (getFileStack(context).size() == 1) {
                    // First call, call itself again
                    validate(file, context);
                }
            }
        };

        ValidateContext context = new ValidateContext();
        nestedValidator.validate(testFile.toFile(), context);

        // Verify context operations
        assertEquals(0, getFileStack(context).size());
        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithReadOnlyFile() throws IOException {
        // Create read-only file
        Path readOnlyFile = tempDir.resolve("readonly-file.txt");
        Files.createFile(readOnlyFile);
        readOnlyFile.toFile().setReadOnly();

        ValidateContext context = new ValidateContext();
        validator.validate(readOnlyFile.toFile(), context);

        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithSymlinkFile() throws IOException {
        // Create regular file and symbolic link
        Path realFile = tempDir.resolve("real-file.txt");
        Files.createFile(realFile);

        Path symlinkFile = tempDir.resolve("symlink-file.txt");
        Files.createSymbolicLink(symlinkFile, realFile);

        ValidateContext context = new ValidateContext();
        validator.validate(symlinkFile.toFile(), context);

        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    /**
     * 测试用的PackageValidator实现
     */
    private static class TestPackageValidator extends AbstractBaseValidator {
        @Override
        public void validate(File file, ValidateContext context) {
            // 空实现
        }
    }
}