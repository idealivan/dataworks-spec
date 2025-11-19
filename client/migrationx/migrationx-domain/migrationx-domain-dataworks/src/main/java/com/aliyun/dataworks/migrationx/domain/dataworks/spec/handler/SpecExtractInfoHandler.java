package com.aliyun.dataworks.migrationx.domain.dataworks.spec.handler;

import java.util.List;
import java.util.Map;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.SpecRefEntity;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2024-09-24
 */
public interface SpecExtractInfoHandler extends SpecHandlerSupport {

    /**
     * get inner specifications
     *
     * @param specification specification
     * @return inner specifications
     */
    List<Specification<DataWorksWorkflowSpec>> getInnerSpecifications(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get spec ref entity
     *
     * @param specification specification
     * @return spec ref entity
     */
    SpecRefEntity getSpecRefEntity(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get domain id which spec refers
     *
     * @param specification specification
     * @return spec id
     */
    String getSpecId(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get spec command
     *
     * @param specification specification
     * @return spec command
     */
    String getSpecCommand(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get spec command type id
     *
     * @param specification specification
     * @return command type id
     */
    Integer getSpecCommandTypeId(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get entity name from specification
     *
     * @param specification specification
     * @return name
     */
    String getSpecName(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get spec path
     *
     * @param specification specification
     * @return path
     */
    String getSpecPath(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get spec language
     *
     * @param specification specification
     * @return language
     */
    String getSpecLanguage(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get script content
     *
     * @param specification specification
     * @return content
     */
    String getScriptContent(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get owner
     *
     * @param specification specification
     * @return owner
     */
    String getOwner(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get description
     *
     * @param specification specification
     * @return description
     */
    String getDescription(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get data source name
     *
     * @param specification specification
     * @return data source name
     */
    String getDataSourceName(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get resource group identifier
     *
     * @param specification specification
     * @return resource group identifier
     */
    String getResourceGroupIdentifier(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get resource group id
     *
     * @param specification specification
     * @return resource group id
     */
    String getResourceGroupId(Specification<DataWorksWorkflowSpec> specification);

    /**
     * get spec script
     *
     * @param specification specification
     * @return spec script
     */
    SpecScript getSpecScript(Specification<DataWorksWorkflowSpec> specification);

    /**
     * reset uuid for spec, and add id mapping in uuidMap
     *
     * @param specification specification
     * @param uuidMap       uuid map
     */
    void resetUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap);

    /**
     * replace uuid in spec by uuid map
     *
     * @param specification specification
     * @param uuidMap       uuidMap
     */
    void replaceUuid(Specification<DataWorksWorkflowSpec> specification, Map<String, String> uuidMap);

}
