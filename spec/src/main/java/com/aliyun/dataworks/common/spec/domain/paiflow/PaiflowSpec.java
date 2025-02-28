package com.aliyun.dataworks.common.spec.domain.paiflow;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;

/**
 * @author 戒迷
 * @date 2025/1/10
 */
@Data
public class PaiflowSpec extends AbstractPaiflowBase {

    private String apiVersion;
    private Map<String, Object> metadata;
    /**
     * 当前DAG/节点的spec
     */
    private PaiflowSpec spec;
    /**
     * dag会有的spec.pipelines
     */
    private List<PaiflowSpec> pipelines;

    /**
     * pipeline上节点上的参数
     */
    private PaiflowArguments arguments;

    /**
     * 输入定义
     */
    private PaiflowArguments inputs;

    /**
     * 输出定义
     */
    private PaiflowArguments outputs;

    /**
     * pipeline里节点的依赖
     */
    private List<String> dependencies;

    private LinkedHashMap<String, Object> container;
    private List<LinkedHashMap<String, Object>> initContainers;
    private List<LinkedHashMap<String, Object>> sideCarContainers;
    private List<LinkedHashMap<String, Object>> volumes;
}
