package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * ValidateContext测试类
 *
 * @author 莫泣
 * @date 2025-08-30
 */
public class ValidateContextTest {

    private ValidateContext context;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        context = new ValidateContext();
        tempDir = Files.createTempDirectory("validate-context-test");
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
    public void testFileStackOperations() throws IOException {
        // 创建测试文件
        Path rootDir = tempDir.resolve("root");
        Files.createDirectory(rootDir);
        Path subDir = rootDir.resolve("sub");
        Files.createDirectory(subDir);
        Path file = subDir.resolve("test.txt");
        Files.createFile(file);

        File rootFile = rootDir.toFile();
        File subFile = subDir.toFile();
        File testFile = file.toFile();

        // 测试推入文件
        context.pushFile(rootFile);
        assertEquals(rootFile, context.getCurrentFile());
        assertEquals(rootFile, context.getRootFile());

        context.pushFile(subFile);
        assertEquals(subFile, context.getCurrentFile());
        assertEquals(rootFile, context.getRootFile());

        context.pushFile(testFile);
        assertEquals(testFile, context.getCurrentFile());
        assertEquals(rootFile, context.getRootFile());

        // 测试弹出文件
        assertEquals(testFile, context.popFile());
        assertEquals(subFile, context.getCurrentFile());
        assertEquals(rootFile, context.getRootFile());

        assertEquals(subFile, context.popFile());
        assertEquals(rootFile, context.getCurrentFile());
        assertEquals(rootFile, context.getRootFile());

        assertEquals(rootFile, context.popFile());
        assertNull(context.getCurrentFile());
        assertNull(context.getRootFile()); // 栈为空时，根文件也为空
    }

    @Test
    public void testRelativePath() throws IOException {
        // 创建测试文件
        Path rootDir = tempDir.resolve("root");
        Files.createDirectory(rootDir);
        Path subDir = rootDir.resolve("sub");
        Files.createDirectory(subDir);
        Path file = subDir.resolve("test.txt");
        Files.createFile(file);

        File rootFile = rootDir.toFile();
        File subFile = subDir.toFile();
        File testFile = file.toFile();

        context.pushFile(rootFile);
        assertEquals("", context.getRelativePath());

        context.pushFile(subFile);
        assertEquals("sub", context.getRelativePath());

        context.pushFile(testFile);
        assertEquals("sub/test.txt", context.getRelativePath());

        context.popFile();
        assertEquals("sub", context.getRelativePath());

        context.popFile();
        assertEquals("", context.getRelativePath());

        context.popFile();
        assertNull(context.getRelativePath());
    }

    @Test
    public void testRelativePathWithNullRootFile() {
        assertNull(context.getRelativePath());
    }

    @Test
    public void testGetRelativePathWithSpecificFile() throws IOException {
        // 创建测试文件
        Path rootDir = tempDir.resolve("root");
        Files.createDirectory(rootDir);
        Path subDir = rootDir.resolve("sub");
        Files.createDirectory(subDir);
        Path file = subDir.resolve("test.txt");
        Files.createFile(file);

        File rootFile = rootDir.toFile();
        File subFile = subDir.toFile();
        File testFile = file.toFile();

        context.pushFile(rootFile);
        context.pushFile(subFile);
        context.pushFile(testFile);

        // 测试获取指定文件的相对路径
        assertEquals("", context.getRelativePath(rootFile));
        assertEquals("sub", context.getRelativePath(subFile));
        assertEquals("sub/test.txt", context.getRelativePath(testFile));
    }

    @Test
    public void testGetRelativePathWithNullFile() {
        assertNull(context.getRelativePath(null));
    }

    @Test
    public void testGetRelativePathWithFileNotUnderRoot() throws IOException {
        // 创建两个不相关的目录
        Path rootDir = tempDir.resolve("root");
        Files.createDirectory(rootDir);
        Path otherDir = tempDir.resolve("other");
        Files.createDirectory(otherDir);

        File rootFile = rootDir.toFile();
        File otherFile = otherDir.toFile();

        context.pushFile(rootFile);
        // otherFile不在rootFile的目录树下，应该返回null
        assertNull(context.getRelativePath(otherFile));
    }

    @Test
    public void testEmptyStackOperations() {
        // 测试空栈的操作
        assertNull(context.getCurrentFile());
        assertNull(context.getRootFile());
        assertNull(context.popFile());
    }

    @Test
    public void testSingleFileInStack() throws IOException {
        Path rootDir = tempDir.resolve("root");
        Files.createDirectory(rootDir);
        File rootFile = rootDir.toFile();

        context.pushFile(rootFile);
        assertEquals(rootFile, context.getCurrentFile());
        assertEquals(rootFile, context.getRootFile());
        assertEquals(rootFile, context.popFile());
        assertNull(context.getCurrentFile());
        assertNull(context.getRootFile());
    }

    @Test
    public void testPushNullFile() {
        // 测试推入null文件，这应该会导致NullPointerException
        try {
            context.pushFile(null);
        } catch (NullPointerException e) {
            // 预期会抛出NullPointerException
        }
    }
}