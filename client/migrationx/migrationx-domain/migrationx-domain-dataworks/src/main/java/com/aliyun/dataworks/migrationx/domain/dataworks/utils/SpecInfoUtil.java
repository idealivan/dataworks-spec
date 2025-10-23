package com.aliyun.dataworks.migrationx.domain.dataworks.utils;

import java.util.List;
import java.util.Map;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.SpecExtractInfoHandler;
import com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler.SpecInfoHandlerFactory;
import org.apache.commons.collections4.ListUtils;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-08-20
 */
public class SpecInfoUtil {

    private static final SpecInfoHandlerFactory SPEC_INFO_HANDLER_FACTORY;

    static {
        SPEC_INFO_HANDLER_FACTORY = SpecInfoHandlerFactory.getInstance();
    }

    private static SpecExtractInfoHandler getHandler(Specification<DataWorksWorkflowSpec> specification) {
        return SPEC_INFO_HANDLER_FACTORY.getHandler(specification);
    }

    public static SpecExtractInfoHandler getHandler(Class<? extends SpecExtractInfoHandler> clazz) {
        return ListUtils.emptyIfNull(SPEC_INFO_HANDLER_FACTORY.getHandlers()).stream()
            .filter(clazz::isInstance)
            .findFirst()
            .orElse(null);
    }

    public static List<Specification<DataWorksWorkflowSpec>> getInnerSpecifications(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getInnerSpecifications(specification);
    }

    public static SpecRefEntity getSpecRefEntity(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getSpecRefEntity(specification);
    }

    public static String getSpecId(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getSpecId(specification);
    }

    public static String getSpecCommand(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getSpecCommand(specification);
    }

    public static Integer getSpecCommandTypeId(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getSpecCommandTypeId(specification);
    }

    public static String getSpecName(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getSpecName(specification);
    }

    public static String getSpecPath(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getSpecPath(specification);
    }

    public static String getSpecLanguage(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getSpecLanguage(specification);
    }

    public static String getScriptContent(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getScriptContent(specification);
    }

    public static String getOwner(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getOwner(specification);
    }

    public static String getDescription(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getDescription(specification);
    }

    public static String getDataSourceName(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getDataSourceName(specification);
    }

    public static String getResourceGroupIdentifier(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getResourceGroupIdentifier(specification);
    }

    public static String getResourceGroupId(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getResourceGroupId(specification);
    }

    public static SpecScript getSpecScript(Specification<DataWorksWorkflowSpec> specification) {
        return getHandler(specification).getSpecScript(specification);
    }

    public static void resetUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        getHandler(specification).resetUuid(specification, uuidMap);
    }

    public static void replaceUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap) {
        getHandler(specification).replaceUuid(specification, uuidMap);
    }
}
