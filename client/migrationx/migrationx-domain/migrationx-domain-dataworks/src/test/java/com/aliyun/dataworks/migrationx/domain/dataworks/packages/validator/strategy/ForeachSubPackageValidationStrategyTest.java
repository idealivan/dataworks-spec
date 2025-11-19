package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator.strategy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.ValidateMode;
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
 * ForeachSubPackageValidationStrategy Test Class
 *
 * @author 莫泣
 * @date 2025-08-29
 */
public class ForeachSubPackageValidationStrategyTest {

    private ForeachSubPackageValidationStrategy strategy;
    private Path tempDir;

    @Before
    public void setUp() throws IOException {
        strategy = new ForeachSubPackageValidationStrategy();
        tempDir = Files.createTempDirectory("foreach-subpackage-test");
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
        assertTrue(strategy.support(ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_TYPE));
        assertFalse(strategy.support("OTHER.type"));
        assertFalse(strategy.support(null));
    }

    @Test
    public void testValidateSubPackagesWithValidStructure() throws IOException {
        // Create valid CONTROLLER_TRAVERSE package structure
        Path packageDir = tempDir.resolve("controller-traverse-package");
        Files.createDirectory(packageDir);

        // Create CONTROLLER_TRAVERSE_START subpackage
        createSubPackage(packageDir, "start-package", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_START_TYPE);

        // Create CONTROLLER_TRAVERSE_END subpackage
        createSubPackage(packageDir, "end-package", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_END_TYPE);

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertTrue("Should have no validation failure results", results.stream().allMatch(result -> result.getValid() == null || result.getValid()));
    }

    @Test
    public void testValidateSubPackagesMissingStartPackage() throws IOException {
        // Create structure missing CONTROLLER_TRAVERSE_START subpackage
        Path packageDir = tempDir.resolve("controller-traverse-package");
        Files.createDirectory(packageDir);

        // Only create CONTROLLER_TRAVERSE_END subpackage
        createSubPackage(packageDir, "end-package", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_END_TYPE);

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
        assertTrue(results.get(0).getErrorMessage().contains("CONTROLLER_TRAVERSE_START subpackage"));
    }

    @Test
    public void testValidateSubPackagesMissingEndPackage() throws IOException {
        // Create structure missing CONTROLLER_TRAVERSE_END subpackage
        Path packageDir = tempDir.resolve("controller-traverse-package");
        Files.createDirectory(packageDir);

        // Only create CONTROLLER_TRAVERSE_START subpackage
        createSubPackage(packageDir, "start-package", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_START_TYPE);

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
        assertTrue(results.get(0).getErrorMessage().contains("CONTROLLER_TRAVERSE_END subpackage"));
    }

    @Test
    public void testValidateSubPackagesMissingBothPackages() throws IOException {
        // Create structure missing both required subpackages
        Path packageDir = tempDir.resolve("controller-traverse-package");
        Files.createDirectory(packageDir);

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertEquals(2, results.size());
        assertFalse(results.get(0).getValid());
        assertFalse(results.get(1).getValid());
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_START subpackage")));
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_END subpackage")));
    }

    @Test
    public void testValidateSubPackagesWithDuplicateStartPackages() throws IOException {
        // Create structure with duplicate CONTROLLER_TRAVERSE_START subpackages
        Path packageDir = tempDir.resolve("controller-traverse-package");
        Files.createDirectory(packageDir);

        // Create two CONTROLLER_TRAVERSE_START subpackages
        createSubPackage(packageDir, "start-package-1", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_START_TYPE);
        createSubPackage(packageDir, "start-package-2", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_START_TYPE);

        // Create one CONTROLLER_TRAVERSE_END subpackage
        createSubPackage(packageDir, "end-package", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_END_TYPE);

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results - Should report duplicate start packages
        assertNotNull(results);
        assertTrue("Should have validation failure results", results.stream().anyMatch(result -> result.getValid() != null && !result.getValid()));
        assertTrue("Should report duplicate start packages", results.stream().anyMatch(result ->
            result.getValid() != null && !result.getValid() && result.getErrorMessage()
                .contains("Multiple CONTROLLER_TRAVERSE_START subpackages exist")));
    }

    @Test
    public void testValidateSubPackagesWithDuplicateEndPackages() throws IOException {
        // Create structure with duplicate CONTROLLER_TRAVERSE_END subpackages
        Path packageDir = tempDir.resolve("controller-traverse-package");
        Files.createDirectory(packageDir);

        // Create one CONTROLLER_TRAVERSE_START subpackage
        createSubPackage(packageDir, "start-package", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_START_TYPE);

        // Create two CONTROLLER_TRAVERSE_END subpackages
        createSubPackage(packageDir, "end-package-1", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_END_TYPE);
        createSubPackage(packageDir, "end-package-2", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_END_TYPE);

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results - Should report duplicate end packages
        assertNotNull(results);
        assertTrue("Should have validation failure results", results.stream().anyMatch(result -> result.getValid() != null && !result.getValid()));
        assertTrue("Should report duplicate end packages", results.stream().anyMatch(result ->
            result.getValid() != null && !result.getValid() && result.getErrorMessage()
                .contains("Multiple CONTROLLER_TRAVERSE_END subpackages exist")));
    }

    @Test
    public void testValidateSubPackagesWithDuplicateStartAndEndPackages() throws IOException {
        // Create structure with duplicate CONTROLLER_TRAVERSE_START and CONTROLLER_TRAVERSE_END subpackages
        Path packageDir = tempDir.resolve("controller-traverse-package");
        Files.createDirectory(packageDir);

        // Create two CONTROLLER_TRAVERSE_START subpackages
        createSubPackage(packageDir, "start-package-1", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_START_TYPE);
        createSubPackage(packageDir, "start-package-2", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_START_TYPE);

        // Create two CONTROLLER_TRAVERSE_END subpackages
        createSubPackage(packageDir, "end-package-1", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_END_TYPE);
        createSubPackage(packageDir, "end-package-2", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_END_TYPE);

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results - Should report duplicate start and end packages
        assertNotNull(results);
        assertTrue("Should have validation failure results", results.stream().anyMatch(result -> result.getValid() != null && !result.getValid()));
        assertTrue("Should report duplicate start packages", results.stream().anyMatch(result ->
            result.getValid() != null && !result.getValid() && result.getErrorMessage()
                .contains("Multiple CONTROLLER_TRAVERSE_START subpackages exist")));
        assertTrue("Should report duplicate end packages", results.stream().anyMatch(result ->
            result.getValid() != null && !result.getValid() && result.getErrorMessage()
                .contains("Multiple CONTROLLER_TRAVERSE_END subpackages exist")));
    }

    // Test methods merged from ForeachSubPackageValidationStrategyAdditionalTest start here

    @Test
    public void testValidateSubPackagesWithEmptyDirectory() throws IOException {
        // Create empty directory
        Path packageDir = tempDir.resolve("empty-package");
        Files.createDirectory(packageDir);

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertEquals(2, results.size());
        assertFalse(results.get(0).getValid());
        assertFalse(results.get(1).getValid());
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_START subpackage")));
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_END subpackage")));
    }

    @Test
    public void testValidateSubPackagesWithNonExistentDirectory() throws IOException {
        // Create a non-existent directory path
        Path nonExistentDir = tempDir.resolve("non-existent");

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(nonExistentDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertEquals(2, results.size());
        assertFalse(results.get(0).getValid());
        assertFalse(results.get(1).getValid());
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_START subpackage")));
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_END subpackage")));
    }

    @Test
    public void testValidateSubPackagesWithInvalidPackageStructure() throws IOException {
        // Create directory with invalid package structure (no .dataworks subdirectory)
        Path packageDir = tempDir.resolve("invalid-package");
        Files.createDirectory(packageDir);

        // Create subdirectory but don't create .dataworks subdirectory
        Path subDir = packageDir.resolve("sub-package");
        Files.createDirectory(subDir);

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertEquals(2, results.size());
        assertFalse(results.get(0).getValid());
        assertFalse(results.get(1).getValid());
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_START subpackage")));
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_END subpackage")));
    }

    @Test
    public void testValidateSubPackagesWithEmptyDataworksDirectory() throws IOException {
        // Create package structure with empty .dataworks directory
        Path packageDir = tempDir.resolve("package-with-empty-dataworks");
        Files.createDirectory(packageDir);

        // Create subpackage but .dataworks directory is empty
        Path subPackageDir = packageDir.resolve("sub-package");
        Files.createDirectory(subPackageDir);
        Path dataworksDir = subPackageDir.resolve(AbstractSubPackageValidationStrategy.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);
        // Don't create any .type file

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertEquals(2, results.size());
        assertFalse(results.get(0).getValid());
        assertFalse(results.get(1).getValid());
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_START subpackage")));
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_END subpackage")));
    }

    @Test
    public void testValidateSubPackagesWithMixedValidAndInvalidPackages() throws IOException {
        // Create structure with mixed valid and invalid packages
        Path packageDir = tempDir.resolve("mixed-package");
        Files.createDirectory(packageDir);

        // Create valid CONTROLLER_TRAVERSE_START subpackage
        createSubPackage(packageDir, "start-package", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_START_TYPE);

        // Create invalid subpackage (no .dataworks directory)
        Path invalidSubPackage = packageDir.resolve("invalid-package");
        Files.createDirectory(invalidSubPackage);

        // Create valid CONTROLLER_TRAVERSE_END subpackage
        createSubPackage(packageDir, "end-package", ForeachSubPackageValidationStrategy.CONTROLLER_TRAVERSE_END_TYPE);

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results - Should have no errors, because only valid packages are considered
        assertNotNull(results);
        assertTrue("Should have no validation failure results", results.stream().allMatch(result -> result.getValid() == null || result.getValid()));
    }

    @Test
    public void testValidateSubPackagesWithFilesInsteadOfDirectories() throws IOException {
        // Create structure with files instead of directories
        Path packageDir = tempDir.resolve("package-with-files");
        Files.createDirectory(packageDir);

        // Create file instead of directory
        Files.createFile(packageDir.resolve("not-a-directory.txt"));

        // Execute validation
        ValidateContext context = new ValidateContext();
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertEquals(2, results.size());
        assertFalse(results.get(0).getValid());
        assertFalse(results.get(1).getValid());
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_START subpackage")));
        assertTrue(results.stream().anyMatch(result -> result.getErrorMessage().contains("CONTROLLER_TRAVERSE_END subpackage")));
    }

    @Test
    public void testValidateSubPackagesPreCheckWithValidStructure() throws IOException {
        // Create valid CONTROLLER_TRAVERSE package structure for pre-check (using schedule file instead of .dataworks)
        Path packageDir = tempDir.resolve("controller-traverse-package");
        Files.createDirectory(packageDir);

        // Create CONTROLLER_TRAVERSE_START subpackage without .dataworks folder (pre-check mode)
        createSubPackagePreCheck(packageDir, "start-package", "CONTROLLER_TRAVERSE");

        // Create CONTROLLER_TRAVERSE_END subpackage without .dataworks folder (pre-check mode)
        createSubPackagePreCheck(packageDir, "end-package", "CONTROLLER_TRAVERSE");

        // Execute validation with pre-check context
        ValidateContext context = new ValidateContext();
        context.setValidateMode(ValidateMode.PRE_VALIDATE);
        context.pushFile(packageDir.toFile());
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertTrue("Should have no validation failure results", results.stream().allMatch(result -> result.getValid() == null || result.getValid()));
    }

    @Test
    public void testValidateSubPackagesPreCheckMissingStartPackage() throws IOException {
        // Create structure missing CONTROLLER_TRAVERSE_START subpackage for pre-check
        Path packageDir = tempDir.resolve("controller-traverse-package");
        Files.createDirectory(packageDir);

        // Only create CONTROLLER_TRAVERSE_END subpackage without .dataworks folder (pre-check mode)
        createSubPackagePreCheck(packageDir, "end-package", "CONTROLLER_TRAVERSE");

        // Execute validation with pre-check context
        ValidateContext context = new ValidateContext();
        context.setValidateMode(ValidateMode.PRE_VALIDATE);
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
        assertTrue(results.get(0).getErrorMessage().contains("CONTROLLER_TRAVERSE_START subpackage"));
    }

    @Test
    public void testValidateSubPackagesPreCheckMissingEndPackage() throws IOException {
        // Create structure missing CONTROLLER_TRAVERSE_END subpackage for pre-check
        Path packageDir = tempDir.resolve("controller-traverse-package");
        Files.createDirectory(packageDir);

        // Only create CONTROLLER_TRAVERSE_START subpackage without .dataworks folder (pre-check mode)
        createSubPackagePreCheck(packageDir, "start-package", "CONTROLLER_TRAVERSE");

        // Execute validation with pre-check context
        ValidateContext context = new ValidateContext();
        context.setValidateMode(ValidateMode.PRE_VALIDATE);
        strategy.validateSubPackages(packageDir.toFile(), context);
        List<PackageValidatorResult> results = context.getResults();

        // Validate results
        assertNotNull(results);
        assertEquals(1, results.size());
        assertFalse(results.get(0).getValid());
        assertTrue(results.get(0).getErrorMessage().contains("CONTROLLER_TRAVERSE_END subpackage"));
    }

    /**
     * Create subpackage for pre-check mode
     *
     * @param parentDir parent directory
     * @param name      subpackage name
     * @param command   command type
     * @throws IOException IO exception
     */
    private void createSubPackagePreCheck(Path parentDir, String name, String command) throws IOException {
        Path subPackageDir = parentDir.resolve(name);
        Files.createDirectory(subPackageDir);

        // Determine the command based on the package name
        String packageCommand = "CONTROLLER_TRAVERSE_START";
        if (name.contains("end")) {
            packageCommand = "CONTROLLER_TRAVERSE_END";
        }

        // Create .schedule.json file with command info (for pre-check mode)
        String scheduleContent = "{\n" +
            "  \"version\": \"1.0.0\",\n" +
            "  \"kind\": \"CycleWorkflow\",\n" +
            "  \"spec\": {\n" +
            "    \"nodes\": [\n" +
            "      {\n" +
            "        \"name\": \"" + name + "\",\n" +
            "        \"script\": {\n" +
            "          \"runtime\": {\n" +
            "            \"command\": \"" + packageCommand + "\"\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}";
        Files.write(subPackageDir.resolve(name + ".schedule.json"), scheduleContent.getBytes());

        // Create script content file
        Files.createFile(subPackageDir.resolve(name));
    }

    /**
     * Create subpackage
     *
     * @param parentDir parent directory
     * @param name      subpackage name
     * @param type      subpackage type
     * @throws IOException IO exception
     */
    private void createSubPackage(Path parentDir, String name, String type) throws IOException {
        Path subPackageDir = parentDir.resolve(name);
        Files.createDirectory(subPackageDir);

        Path dataworksDir = subPackageDir.resolve(AbstractSubPackageValidationStrategy.DATAWORKS_FILE_NAME);
        Files.createDirectory(dataworksDir);

        Files.createFile(dataworksDir.resolve(type));
    }
}