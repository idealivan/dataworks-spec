package com.aliyun.dataworks.common.spec.domain.dw.codemodel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.alibaba.fastjson2.JSON;

import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.dw.types.CodeProgramType;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.domain.enums.SpecVersion;
import com.aliyun.dataworks.common.spec.domain.enums.VariableScopeType;
import com.aliyun.dataworks.common.spec.domain.enums.VariableType;
import com.aliyun.dataworks.common.spec.domain.noref.SpecDepend;
import com.aliyun.dataworks.common.spec.domain.noref.SpecFlowDepend;
import com.aliyun.dataworks.common.spec.domain.paiflow.PaiflowArguments;
import com.aliyun.dataworks.common.spec.domain.paiflow.PaiflowArgumentsWrapper;
import com.aliyun.dataworks.common.spec.domain.paiflow.PaiflowArtifact;
import com.aliyun.dataworks.common.spec.domain.paiflow.PaiflowParameter;
import com.aliyun.dataworks.common.spec.domain.paiflow.PaiflowScriptContent;
import com.aliyun.dataworks.common.spec.domain.paiflow.PaiflowSpec;
import com.aliyun.dataworks.common.spec.domain.ref.SpecNode;
import com.aliyun.dataworks.common.spec.domain.ref.SpecScript;
import com.aliyun.dataworks.common.spec.domain.ref.SpecVariable;
import com.aliyun.dataworks.common.spec.domain.ref.runtime.SpecScriptRuntime;
import com.aliyun.dataworks.common.spec.parser.SpecParserContext;
import com.aliyun.dataworks.common.spec.utils.UuidUtils;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

/**
 * @author 戒迷
 * @date 2025/2/5
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Accessors(chain = true)
@ToString
@Slf4j
public class PaiflowYamlCode extends AbstractBaseCode implements YamlFormCode {

    public static final String PAIFLOW_CONF_KEY_ARGUMENT_PARAMETER = "parameters";
    private static final Pattern PAIFLOW_ARTIFACT_REGEX_PATTERN = Pattern.compile("\\{\\{pipelines\\.([^.]+)\\.outputs\\.artifacts\\.([^.]+)}}");
    private static final String METADATA_PAIFLOW_KEY = "paiflow";

    private PaiflowScriptContent paiflowScriptContent;

    public static Yaml getYaml() {
        // 按块输出
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);

        // 忽略掉不存在的属性
        Constructor constructor = new Constructor(new LoaderOptions());
        constructor.getPropertyUtils().setSkipMissingProperties(true);

        // 实现空值不输出
        Representer representer = new Representer(options) {
            @Override
            protected NodeTuple representJavaBeanProperty(Object javaBean, Property property, Object propertyValue, Tag customTag) {
                return propertyValue == null ? null : super.representJavaBeanProperty(javaBean, property, propertyValue, customTag);
            }
        };

        return new Yaml(constructor, representer, options);
    }

    /**
     * 用于解析paiflow的pipelines的内部节点，paiflow的dag转化为paiflow组合节点
     *
     * @param pipelineManifests 内部节点的paiflow manifest，依赖该字段获取节点outputs
     * @return dataworks flowspec
     */
    public Specification<DataWorksWorkflowSpec> getSpec(List<String> pipelineManifests) {

        Yaml yaml = getYaml();
        List<PaiflowSpec> pipelinePaiflowSpecs = ListUtils.emptyIfNull(pipelineManifests).stream()
            .map(x -> yaml.loadAs(x, PaiflowSpec.class))
            .map(x -> (PaiflowSpec)x)
            .collect(Collectors.toList());

        // 从paiflowArguments中获取参数信息，构建出map是为了构建paiflowConf中的parameters时只放入在paiflowArguments中存在的参数
        Map<String, PaiflowParameter> parameterMap = Optional.ofNullable(paiflowScriptContent.getPaiflowArguments())
            .map(PaiflowArgumentsWrapper::getArguments)
            .map(PaiflowArguments::getParameters)
            .orElse(Collections.emptyList())
            .stream()
            .collect(Collectors.toMap(PaiflowParameter::getName, x -> x));

        Specification<DataWorksWorkflowSpec> specObj = new Specification<>();
        DataWorksWorkflowSpec dwSpec = new DataWorksWorkflowSpec();
        specObj.setVersion(SpecVersion.V_1_1_0.getLabel());
        specObj.setKind(SpecKind.PAIFLOW.getLabel());
        specObj.setContext(new SpecParserContext());
        Map<String, Object> metadata = Maps.newHashMap();
        metadata.put(METADATA_PAIFLOW_KEY, paiflowScriptContent.getPaiflowPipeline().getMetadata());
        dwSpec.setMetadata(metadata);
        specObj.setSpec(dwSpec);

        // paiflow内部节点的依赖flow
        List<SpecFlowDepend> flow = new ArrayList<>();

        // 构建出一个key为nodeName, value为nodeObj的引用的Map对象，nodeName在一个paiflow中是唯一的，最后构建出来flow，需要根据nodeName替换过程中生成的id
        Map<String, SpecNode> nodeMap = Maps.newHashMap();
        List<SpecNode> nodes = Optional.ofNullable(paiflowScriptContent.getPaiflowPipeline()).map(PaiflowSpec::getSpec)
            .map(x -> {
                if (CollectionUtils.isEmpty(x.getPipelines())) {
                    PaiflowSpec singlePipeline = new PaiflowSpec();
                    singlePipeline.setApiVersion(paiflowScriptContent.getPaiflowPipeline().getApiVersion());
                    singlePipeline.setMetadata(paiflowScriptContent.getPaiflowPipeline().getMetadata());
                    singlePipeline.setSpec(x);
                    return Collections.singletonList(singlePipeline);
                } else {
                    return x.getPipelines();
                }
            })
            .orElse(Collections.emptyList())
            .stream()
            // 最关键的构建在这里
            .map(node -> buildSpecNodeByPaiflowSpec(node, parameterMap, nodeMap, flow, pipelinePaiflowSpecs))
            .filter(Objects::nonNull).collect(Collectors.toList());

        // 根据依赖关系，替换掉nodeId，只能最后统一处理，因为nodeId是处理过程中生成的，不能保证nodeName已经生成nodeId了
        flow.forEach(x -> x.getDepends()
            .stream().filter(depend -> Objects.nonNull(depend.getNodeId()))
            .filter(depend -> Objects.nonNull(nodeMap.get(depend.getNodeId().getName())))
            .forEach(depend -> depend.getNodeId().setId(nodeMap.get(depend.getNodeId().getName()).getId())));

        dwSpec.setNodes(nodes);
        dwSpec.setFlow(flow);
        return specObj;
    }

    /**
     * 构建paiflow内部节点的arguments
     *
     * @param script paiflow内部节点script
     * @return paiflow内部节点的arguments
     */
    public PaiflowArguments buildPaiflowArguments(SpecScript script) {
        PaiflowArguments paiflowArguments = new PaiflowArguments();

        // 不管paiflowPipline是单节点，还是dag，都从最外层的inputs中获取需要的inputs信息（包括parameters和artifacts）
        PaiflowArguments paiflowInputs = Optional.ofNullable(this.getPaiflowScriptContent())
            .map(PaiflowScriptContent::getPaiflowPipeline)
            .map(PaiflowSpec::getSpec)
            .map(PaiflowSpec::getInputs).orElse(null);
        if (null == paiflowInputs) {
            return paiflowArguments;
        }

        // 从DataWorks Spec中取出上下文参数，用于拼接artifacts参数
        List<PaiflowArtifact> artifacts = Optional.ofNullable(paiflowInputs.getArtifacts()).orElse(Collections.emptyList()).stream()
            .map(x -> {
                PaiflowArtifact paiflowArtifact = new PaiflowArtifact();
                paiflowArtifact.setName(x.getName());
                paiflowArtifact.setMetadata(JSON.parseObject(JSON.toJSONString(x.getMetadata())));
                paiflowArtifact.setValue(String.format("${%s}", paiflowArtifact.getName()));
                return paiflowArtifact;
            }).collect(Collectors.toList());

        paiflowArguments.setParameters(Optional.ofNullable(script.getRuntime()).map(SpecScriptRuntime::getPaiflowConf)
            .map(x -> x.get(PAIFLOW_CONF_KEY_ARGUMENT_PARAMETER))
            .map(x -> JSON.parseArray(JSON.toJSONString(x), PaiflowParameter.class))
            .orElse(null));
        paiflowArguments.setArtifacts(artifacts);
        return paiflowArguments;
    }

    @Override
    public String getContent() {
        if (null == this.getPaiflowScriptContent()) {
            return "";
        }
        return getYaml().dumpAsMap(this.getPaiflowScriptContent());
    }

    public String getSchedulerContent() {
        if (null == this.getPaiflowScriptContent()) {
            return "";
        }
        return JSON.toJSONString(this.getPaiflowScriptContent());
    }

    @Override
    public PaiflowYamlCode parse(String code) {
        if (StringUtils.isBlank(code)) {
            return new PaiflowYamlCode();
        }
        Object yamlObj = getYaml().load(code);
        this.setPaiflowScriptContent(JSON.parseObject(JSON.toJSONString(yamlObj), PaiflowScriptContent.class));
        return this;
    }

    @Override
    public List<String> getProgramTypes() {
        return Collections.singletonList(CodeProgramType.PAI_FLOW.getName());
    }

    @Override
    public boolean support(String programType) {
        return programType.startsWith(CodeProgramType.PAI_FLOW.getName());
    }

    private SpecNode buildSpecNodeByPaiflowSpec(PaiflowSpec node, Map<String, PaiflowParameter> parameterMap, Map<String, SpecNode> nodeMap,
        List<SpecFlowDepend> flow, List<PaiflowSpec> pipelinePaiflowSpecs) {

        PaiflowSpec nodeSpec = node.getSpec();
        if (null == nodeSpec) {
            log.error("invalid node spec, without spec:{}", node);
            return null;
        }

        Map<String, Object> nodeMetadata = node.getMetadata();
        SpecNode nodeObj = new SpecNode();
        SpecScript script = new SpecScript();
        PaiflowArguments arguments = nodeSpec.getArguments();
        SpecScriptRuntime specScriptRuntime = new SpecScriptRuntime();

        Map<String, Object> metadata = Maps.newHashMap();
        metadata.put(METADATA_PAIFLOW_KEY, nodeMetadata);
        nodeObj.setMetadata(metadata);
        nodeObj.setScript(script);

        // 设置节点的名称，DataWorks的名称不支持-，需要转为下划线
        nodeObj.setName(getNodeName(String.valueOf(nodeMetadata.get("name"))));
        nodeObj.setId(UuidUtils.genUuidWithoutHorizontalLine());
        nodeMap.put(nodeObj.getName(), nodeObj);

        // 从paiflow中获取参数，填充到DataWorks Spec中的paiflowConf中
        Map<String, Object> paiflowConf = Maps.newHashMap();

        specScriptRuntime.setPaiflowConf(paiflowConf);
        script.setRuntime(specScriptRuntime);

        // 从paiflow中获取artifacts，填充到DataWorks Spec中的上下文输入参数，并根据上下文输入参数，建立节点间的依赖
        SpecFlowDepend specFlowDepend = new SpecFlowDepend();
        specFlowDepend.setNodeId(nodeObj);

        Optional<PaiflowSpec> paiflowManifestSpec = ListUtils.emptyIfNull(pipelinePaiflowSpecs).stream()
            .filter(x -> Objects.equals(x.getMetadata().get("identifier"), nodeMetadata.get("identifier"))
                && Objects.equals(x.getMetadata().get("provider"), nodeMetadata.get("provider"))
                && Objects.equals(x.getMetadata().get("version"), nodeMetadata.get("version"))
            ).findFirst()
            .map(PaiflowSpec::getSpec);

        List<PaiflowParameter> parametersWithValue = Optional.ofNullable(arguments).map(PaiflowArguments::getParameters)
            .orElse(Collections.emptyList()).stream()
            .map(x -> Optional.ofNullable(parameterMap.get(x.getFromParameterName())).map(parameterWithValue -> {
                PaiflowParameter paiflowParameter = new PaiflowParameter();
                paiflowParameter.setName(x.getName());
                paiflowParameter.setValue(parameterWithValue.getValue());
                return paiflowParameter;
            }).orElse(null))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        List<PaiflowParameter> parametersWithDefaultValue = paiflowManifestSpec
            .map(PaiflowSpec::getInputs)
            .map(PaiflowArguments::getParameters)
            .orElse(Collections.emptyList()).stream()
            .filter(x -> parametersWithValue.stream().noneMatch(y -> StringUtils.equals(x.getName(), y.getName())))
            .map(x -> {
                PaiflowParameter paiflowParameter = new PaiflowParameter();
                paiflowParameter.setName(x.getName());
                paiflowParameter.setValue(x.getValue());
                if ("Map".equalsIgnoreCase(x.getType())) {
                    paiflowParameter.setValue(Maps.newHashMap());
                }
                return paiflowParameter;
            }).collect(Collectors.toList());

        paiflowConf.put(PAIFLOW_CONF_KEY_ARGUMENT_PARAMETER, parametersWithValue.isEmpty() ? parametersWithDefaultValue : parametersWithValue );

        // 从paiflow的静态manifest中获取input artifact，填充到DataWorks Spec中的script parameters
        List<SpecVariable> variableParameters = paiflowManifestSpec
            .map(PaiflowSpec::getInputs)
            .map(PaiflowArguments::getArtifacts)
            .orElse(Collections.emptyList()).stream()
            .map(x -> buildPaiOutputSpecVariable(x.getName(), null)).collect(Collectors.toList());
        Map<String, SpecVariable> variableParameterMap = variableParameters.stream().collect(Collectors.toMap(SpecVariable::getName, x -> x));

        // 节点的依赖需要根据名称去重，因此定义为map
        LinkedHashMap<String, SpecDepend> nodeDepends = new LinkedHashMap<>();
        // 从paiflow的运行参数重获取arguments，将arguments中的from参数替换为DataWorks Spec中的上下文输入参数
        List<SpecVariable> inputVariableList = Optional.ofNullable(arguments).map(PaiflowArguments::getArtifacts)
            .orElse(Collections.emptyList()).stream()
            .filter(x -> variableParameterMap.containsKey(x.getName()))
            .map(x -> buildSpecVariableByPaiflowArtifact(x, variableParameterMap.get(x.getName()), nodeDepends))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        nodeObj.setInputs(new ArrayList<>(inputVariableList));

        // 从paiflow的静态manifest中获取output artifacts，填充到DataWorks Spec中的上下文输出参数
        List<SpecVariable> outputContextList = paiflowManifestSpec
            .map(PaiflowSpec::getOutputs)
            .map(PaiflowArguments::getArtifacts)
            .orElse(Collections.emptyList()).stream()
            .map(x -> buildPaiOutputSpecVariable(x.getName(), null)).collect(Collectors.toList());
        nodeObj.setOutputs(new ArrayList<>(outputContextList));

        script.setParameters(variableParameters);
        if (MapUtils.isNotEmpty(nodeDepends)) {
            specFlowDepend.setDepends(new ArrayList<>(nodeDepends.values()));
            flow.add(specFlowDepend);
        }
        return nodeObj;
    }

    private SpecVariable buildSpecVariableByPaiflowArtifact(PaiflowArtifact paiflowArtifact, SpecVariable parameterVariable,
        LinkedHashMap<String, SpecDepend> nodeDepends) {
        // 只能处理pipelines中的内容
        Matcher matcher = PAIFLOW_ARTIFACT_REGEX_PATTERN.matcher(paiflowArtifact.getFrom());

        if (!matcher.find()) {
            return null;
        }

        // 声明输入参数
        String fromNodeName = getNodeName(matcher.group(1));
        String fromArtifactName = matcher.group(2);

        SpecDepend specDepend = new SpecDepend();
        SpecNode dependNode = new SpecNode();
        dependNode.setName(fromNodeName);
        specDepend.setNodeId(dependNode);
        nodeDepends.putIfAbsent(fromNodeName, specDepend);

        // 变量上建立关联
        parameterVariable.setReferenceVariable(buildPaiOutputSpecVariable(fromArtifactName, specDepend));
        return parameterVariable.getReferenceVariable();
    }

    private SpecVariable buildPaiOutputSpecVariable(String variableName, SpecDepend specDepend) {
        SpecVariable variable = new SpecVariable();
        variable.setName(variableName);
        variable.setType(VariableType.PAI_OUTPUT);
        variable.setScope(VariableScopeType.NODE_CONTEXT);
        variable.setNode(specDepend);
        return variable;
    }

    private String getNodeName(String paiflowNodeName) {
        return paiflowNodeName.replace("-", "_");
    }

}
