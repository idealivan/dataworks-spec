package com.aliyun.dataworks.common.spec.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.common.spec.domain.enums.SpecKind;
import com.aliyun.dataworks.common.spec.exception.SpecException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Desc:
 *
 * @author 莫泣
 * @date 2025-07-15
 */
@Slf4j
public class SpecValidateUtilTest {

    @Test
    public void testValidateBlank() {
        List<String> validate1 = SpecValidateUtil.validate(null);
        Assert.assertEquals("spec json is blank", validate1.get(0));

        List<String> validate2 = SpecValidateUtil.validate("");
        Assert.assertEquals("spec json is blank", validate2.get(0));

        List<String> validate3 = SpecValidateUtil.validate("   ");
        Assert.assertEquals("spec json is blank", validate3.get(0));
    }

    @Test
    public void testValidateErrorJson() {
        Assert.assertThrows(SpecException.class, () -> SpecValidateUtil.validate("{{null}}"));
    }

    @Test
    public void testValidateEmptyNodes() {
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        specification.setVersion("1.1.0");
        specification.setKind(SpecKind.NODE.getLabel());

        DataWorksWorkflowSpec spec = new DataWorksWorkflowSpec();
        specification.setSpec(spec);
        spec.setNodes(List.of());

        String specJson = SpecUtil.writeToSpec(specification);
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(1, errors.size());
    }

    @Test
    public void testValidateAssign() throws IOException {
        String specJson = readJson("spec/examples/json/assign.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(4, errors.size());
    }

    @Test
    public void testValidateAssignment() throws IOException {
        String specJson = readJson("spec/examples/json/assignment.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(0, errors.size());
    }

    @Test
    public void testValidateCdh() throws IOException {
        String specJson = readJson("spec/examples/json/cdh.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(0, errors.size());
    }

    @Test
    public void testValidateCombinedNode() throws IOException {
        String specJson = readJson("spec/examples/json/combined_node.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(6, errors.size());
    }

    @Test
    public void testValidateDatasource() throws IOException {
        String specJson = readJson("spec/examples/json/datasource.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(2, errors.size());
    }

    @Test
    public void testValidateDoWhile() throws IOException {
        String specJson = readJson("spec/examples/json/dowhile.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(0, errors.size());
    }

    @Test
    public void testValidateDqcRule() throws IOException {
        String specJson = readJson("spec/examples/json/dqc.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(2, errors.size());
    }

    @Test
    public void testValidateEmr() throws IOException {
        String specJson = readJson("spec/examples/json/emr.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(1, errors.size());
    }

    @Test
    public void testValidateExample() throws IOException {
        String specJson = readJson("spec/examples/json/example.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(25, errors.size());
    }

    @Test
    public void testValidateFileResource() throws IOException {
        String specJson = readJson("spec/examples/json/file_resource.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(6, errors.size());
    }

    @Test
    public void testValidateFunction() throws IOException {
        String specJson = readJson("spec/examples/json/function.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(6, errors.size());
    }

    @Test
    public void testValidateInnerFlow() throws IOException {
        String specJson = readJson("spec/examples/json/innerflow.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(12, errors.size());
    }

    @Test
    public void testValidateJoin() throws IOException {
        String specJson = readJson("spec/examples/json/join.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(10, errors.size());
    }

    @Test
    public void testValidateManualFlow() throws IOException {
        String specJson = readJson("spec/examples/json/manual_flow.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(9, errors.size());
    }

    @Test
    public void testValidatePaiflow() throws IOException {
        String specJson = readJson("spec/examples/json/paiflow.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(0, errors.size());
    }

    @Test
    public void testValidateParamHub() throws IOException {
        String specJson = readJson("spec/examples/json/param_hub.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(2, errors.size());
    }

    @Test
    public void testValidateParameterNode() throws IOException {
        String specJson = readJson("spec/examples/json/parameter_node.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(10, errors.size());
    }

    @Test
    public void testValidateRealCase() throws IOException {
        String specJson = readJson("spec/examples/json/real_case.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(36, errors.size());
    }

    @Test
    public void testValidateRealCaseExpanded() throws IOException {
        String specJson = readJson("spec/examples/json/real_case_expanded.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(20, errors.size());
    }

    @Test
    public void testValidateScriptRuntimeTemplate() throws IOException {
        String specJson = readJson("spec/examples/json/script_runtime_template.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(2, errors.size());
    }

    @Test
    public void testValidateSimple() throws IOException {
        String specJson = readJson("spec/examples/json/simple.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(10, errors.size());
    }

    @Test
    public void testValidateTable() throws IOException {
        String specJson = readJson("spec/examples/json/table.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(3, errors.size());
    }

    @Test
    public void testValidateBranchNewTemplate() throws IOException {
        String specJson = readJson("spec/examples/json/branch_new.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(0, errors.size());
    }

    @Test
    public void testValidateJoinNewTemplate() throws IOException {
        String specJson = readJson("spec/examples/json/join_new.json");
        List<String> errors = SpecValidateUtil.validate(specJson);
        log.info("errors: {}", errors);
        Assert.assertEquals(0, errors.size());
    }

    private String readJson(String fileName) throws IOException {
        InputStream resourceAsStream = getClass().getResourceAsStream("/" + fileName);
        return IOUtils.toString(resourceAsStream, Charset.defaultCharset());
    }

}
