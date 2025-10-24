package com.aliyun.dataworks.migrationx.domain.dataworks.packages.validator;

import java.io.File;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.aliyun.dataworks.migrationx.domain.dataworks.packages.enums.ValidateMode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.map.LRUMap;

/**
 * Validation Context Class, used to pass context information during the validation process
 *
 * @author 莫泣
 * @date 2025-08-26
 */
public class ValidateContext {

    /**
     * File stack, used to record file path hierarchy information during validation
     * Bottom of stack is root file, top of stack is currently validating file
     */
    private final Deque<File> fileStack = new ArrayDeque<>();

    private final Set<String> uuidSet = new HashSet<>();

    private final LRUMap<String, SpecificationWrapper> specificationCache = new LRUMap<>(100);

    /**
     * Validation results list, used to store all validation results during the validation process
     * -- GETTER --
     * Get all validation results
     */
    @Getter
    private final List<PackageValidatorResult> results = new ArrayList<>();

    @Getter
    @Setter
    private ValidateMode validateMode = ValidateMode.NORMAL;

    /**
     * Push file onto stack
     *
     * @param file file
     */
    public void pushFile(File file) {
        fileStack.push(file);
    }

    /**
     * Pop file from stack
     *
     * @return file
     */
    public File popFile() {
        return fileStack.poll();
    }

    /**
     * Get currently validating file (top element of stack)
     *
     * @return current file
     */
    public File getCurrentFile() {
        return fileStack.peek();
    }

    /**
     * Get root file (bottom element of stack)
     *
     * @return root file
     */
    public File getRootFile() {
        return fileStack.peekLast();
    }

    /**
     * Get current file's path relative to root file
     *
     * @return relative path, or null if unable to calculate
     */
    public String getRelativePath() {
        return getRelativePath(getCurrentFile());
    }

    /**
     * Get specified file's path relative to root file
     *
     * @param file specified file
     * @return relative path, or null if unable to calculate
     */
    public String getRelativePath(File file) {
        File rootFile = getRootFile();
        if (file == null || rootFile == null) {
            return null;
        }

        try {
            String currentPath = file.getCanonicalPath();
            String rootPath = rootFile.getCanonicalPath();

            if (currentPath.startsWith(rootPath)) {
                String relativePath = currentPath.substring(rootPath.length());
                // Remove leading file separator
                if (relativePath.startsWith(File.separator)) {
                    relativePath = relativePath.substring(1);
                }
                return relativePath;
            }
        } catch (Exception e) {
            // Ignore exception, return null
        }

        return null;
    }

    public boolean isNormalValidate() {
        return ValidateMode.NORMAL.equals(getValidateMode());
    }

    public boolean isPreCheck() {
        return ValidateMode.PRE_VALIDATE.equals(getValidateMode());
    }

    public boolean addUuidIfAbsent(String uuid) {
        return !uuidSet.contains(uuid) && uuidSet.add(uuid);
    }

    public void clear() {
        uuidSet.clear();
    }

    public void addSpecification(String path, SpecificationWrapper wrapper) {
        specificationCache.put(path, wrapper);
    }

    public SpecificationWrapper getSpecification(String path) {
        return specificationCache.get(path);
    }

    /**
     * Add validation result
     *
     * @param result validation result
     */
    public void addResult(PackageValidatorResult result) {
        if (result != null) {
            results.add(result);
        }
    }
}
