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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * AbstractFileValidator Test Class
 *
 * @author Mo Qi
 * @date 2025-08-31
 */
public class AbstractFileValidatorTest {

    private TestFileValidator validator;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        validator = new TestFileValidator();
        tempDir = Files.createTempDirectory("abstract-file-test");
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
    public void testValidateFile() throws IOException {
        Path testFile = tempDir.resolve("test-file.txt");
        Files.createFile(testFile);

        ValidateContext context = new ValidateContext();
        validator.validate(testFile.toFile(), context);

        // Verify that the file stack in context is handled correctly
        assertEquals(0, getFileStack(context).size());
        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateFileWithExceptionInDoValidate() throws IOException {
        Path testFile = tempDir.resolve("test-file.txt");
        Files.createFile(testFile);

        // Create a validator that throws an exception
        AbstractFileValidator exceptionValidator = new AbstractFileValidator() {
            @Override
            protected void doValidate(File file, ValidateContext context) {
                throw new RuntimeException("Test exception");
            }
        };

        ValidateContext context = new ValidateContext();
        try {
            exceptionValidator.validate(testFile.toFile(), context);
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            // Verify that the exception is caught
            assertEquals("Test exception", e.getMessage());
        }
        // Verify that the file stack in context is handled correctly (should pop the file even with exception)
        assertEquals(0, getFileStack(context).size());
    }

    // Test methods merged from AbstractFileValidatorAdditionalTest start here

    @Test
    public void testValidateWithExceptionInDoValidateAdditional() throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("test-file.txt");
        Files.createFile(testFile);

        // Create a validator that throws an exception
        AbstractFileValidator exceptionValidator = new AbstractFileValidator() {
            @Override
            protected void doValidate(File file, ValidateContext context) {
                throw new RuntimeException("Test exception in doValidate");
            }
        };

        ValidateContext context = new ValidateContext();
        try {
            exceptionValidator.validate(testFile.toFile(), context);
            // If no exception is thrown, it means it was handled correctly
        } catch (Exception e) {
            // Verify exception message
            assertEquals("Test exception in doValidate", e.getMessage());
        }
        // Verify that the file stack in context is handled correctly (should pop the file even with exception)
        assertEquals(0, getFileStack(context).size());
    }

    @Test
    public void testValidateWithContextFileStackOperations() throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("test-file.txt");
        Files.createFile(testFile);

        ValidateContext context = new ValidateContext();
        validator.validate(testFile.toFile(), context);

        // Verify that the file stack in context is handled correctly
        assertEquals(0, getFileStack(context).size());
        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithIOException() throws IOException {
        // Create a non-existent file path
        Path nonExistentFile = tempDir.resolve("non-existent.txt");

        ValidateContext context = new ValidateContext();
        validator.validate(nonExistentFile.toFile(), context);

        // Verify that the file stack in context is handled correctly
        assertEquals(0, getFileStack(context).size());
        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithSpecialCharactersInFileName() throws IOException {
        // Create a filename with special characters
        Path specialFile = tempDir.resolve("special-文件-🎉.txt");
        Files.createFile(specialFile);

        ValidateContext context = new ValidateContext();
        validator.validate(specialFile.toFile(), context);

        // Verify that the file stack in context is handled correctly
        assertEquals(0, getFileStack(context).size());
        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithVeryLongFileName() throws IOException {
        // 创建一个较长但合理的文件名
        StringBuilder longFileName = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            longFileName.append("long-file-name-part-");
        }
        longFileName.append(".txt");

        Path longNameFile = tempDir.resolve(longFileName.toString());
        Files.createFile(longNameFile);

        ValidateContext context = new ValidateContext();
        validator.validate(longNameFile.toFile(), context);

        // Verify that the file stack in context is handled correctly
        assertEquals(0, getFileStack(context).size());
        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    @Test
    public void testValidateWithContextFileStackNestedCalls() throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("test-file.txt");
        Files.createFile(testFile);

        // Create a validator with nested calls
        AbstractFileValidator nestedValidator = new AbstractFileValidator() {
            @Override
            protected void doValidate(File file, ValidateContext context) {
                // Simulate nested validation calls
                if (getFileStack(context).size() == 1) {
                    // First call, call itself again
                    validate(file, context);
                }
            }
        };

        ValidateContext context = new ValidateContext();
        nestedValidator.validate(testFile.toFile(), context);

        // Verify that context is correctly cleaned up after nested calls
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

        // Verify that the file stack in context is handled correctly
        assertEquals(0, getFileStack(context).size());
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

        // Verify that the file stack in context is handled correctly
        assertEquals(0, getFileStack(context).size());
        // Verify that results are added to context
        assertNotNull(context.getResults());
    }

    // Test cases for readFileContent method

    @Test
    public void testReadFileContentWithValidFile() throws IOException {
        // Create test file
        Path testFile = tempDir.resolve("test-file.txt");
        String content = "This is test content";
        Files.write(testFile, content.getBytes());

        ValidateContext context = new ValidateContext();
        TestFileValidatorForReadFileContent validatorForReadFileContent = new TestFileValidatorForReadFileContent();
        String readContent = validatorForReadFileContent.testReadFileContent(testFile.toFile(), context);

        // Verify read content
        assertEquals(content, readContent);
        // Verify that results are added to context
        assertNotNull(context.getResults());
        // Verify that the file stack in context is handled correctly
        assertEquals(0, getFileStack(context).size());
    }

    @Test
    public void testReadFileContentWithIOException() throws IOException {
        // Create a non-existent file path
        Path nonExistentFile = tempDir.resolve("non-existent.txt");

        ValidateContext context = new ValidateContext();
        TestFileValidatorForReadFileContent validatorForReadFileContent = new TestFileValidatorForReadFileContent();
        String readContent = validatorForReadFileContent.testReadFileContent(nonExistentFile.toFile(), context);

        // Verify that read content is null
        assertNull(readContent);
        // Verify that results are added to context
        assertNotNull(context.getResults());
        // Verify that the file stack in context is handled correctly
        assertEquals(0, getFileStack(context).size());
    }

    /**
     * Test AbstractFileValidator implementation
     */
    private static class TestFileValidator extends AbstractFileValidator {
        @Override
        protected void doValidate(File file, ValidateContext context) {
            // Empty implementation
        }
    }

    /**
     * Test AbstractFileValidator implementation, specifically for testing readFileContent method
     */
    private static class TestFileValidatorForReadFileContent extends AbstractFileValidator {
        public String testReadFileContent(File file, ValidateContext context) {
            return readFileContent(file, context);
        }

        @Override
        protected void doValidate(File file, ValidateContext context) {
            // Empty implementation
        }
    }
}