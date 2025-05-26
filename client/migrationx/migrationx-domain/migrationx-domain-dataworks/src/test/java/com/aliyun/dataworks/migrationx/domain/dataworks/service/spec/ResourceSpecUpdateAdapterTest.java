package com.aliyun.dataworks.migrationx.domain.dataworks.service.spec;

import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.File;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.entity.client.FileNodeCfg;
import com.aliyun.dataworks.migrationx.domain.dataworks.objects.types.NodeUseType;
import com.aliyun.dataworks.migrationx.domain.dataworks.service.spec.entity.FileDetailEntityAdapter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

public class ResourceSpecUpdateAdapterTest {
    @Mock
    Logger log;
    @InjectMocks
    ResourceSpecUpdateAdapter resourceSpecUpdateAdapter;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testUpdateSpecificationCreateResource() throws Exception {
        File file = new File();
        FileNodeCfg fileNodeCfg = new FileNodeCfg();
        fillFile(file);
        fillNodeCfg(fileNodeCfg);

        FileDetailEntityAdapter adapter = new FileDetailEntityAdapter();
        adapter.setFile(file);
        adapter.setFileNodeCfg(fileNodeCfg);
        Specification<DataWorksWorkflowSpec> specification = new Specification<>();
        new ResourceSpecUpdateAdapter().updateSpecification(adapter, specification);

        Assert.assertEquals("{\n"
            + "\t\"version\":\"1.1.0\",\n"
            + "\t\"kind\":\"Resource\",\n"
            + "\t\"spec\":{\n"
            + "\t\t\"fileResources\":[\n"
            + "\t\t\t{\n"
            + "\t\t\t\t\"name\":\"test_file_name.jar\",\n"
            + "\t\t\t\t\"id\":\"123456789\",\n"
            + "\t\t\t\t\"script\":{\n"
            + "\t\t\t\t\t\"content\":\"select 1;\",\n"
            + "\t\t\t\t\t\"path\":\"test/path/test_file_name.jar\",\n"
            + "\t\t\t\t\t\"runtime\":{\n"
            + "\t\t\t\t\t\t\"command\":\"ODPS_SQL\",\n"
            + "\t\t\t\t\t\t\"engine\":\"MaxCompute\"\n"
            + "\t\t\t\t\t}\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"runtimeResource\":{\n"
            + "\t\t\t\t\t\"resourceGroup\":\"resgroup\"\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"type\":\"jar\",\n"
            + "\t\t\t\t\"file\":{\n"
            + "\t\t\t\t\t\"storage\":{\n"
            + "\t\t\t\t\t\t\"type\":\"local\"\n"
            + "\t\t\t\t\t},\n"
            + "\t\t\t\t\t\"path\":\"/home/admin\"\n"
            + "\t\t\t\t},\n"
            + "\t\t\t\t\"datasource\":{\n"
            + "\t\t\t\t\t\"name\":\"odps_test\"\n"
            + "\t\t\t\t}\n"
            + "\t\t\t}\n"
            + "\t\t]\n"
            + "\t},\n"
            + "\t\"metadata\":{\n"
            + "\t\t\"owner\":\"111222\",\n"
            + "\t\t\"uuid\":\"123456789\"\n"
            + "\t}\n"
            + "}", SpecUtil.writeToSpec(specification));
    }

    private void fillFile(File file) {
        file.setFileId(123456789L);
        file.setFileFolderPath("test/path");
        file.setFileName("test_file_name.jar");
        file.setFileType(10);
        file.setOwner("111222");
        file.setContent("select 1;");
        file.setUseType(NodeUseType.SCHEDULED.getValue());
        file.setFileDesc("test_description");
        file.setConnName("odps_test");
        file.setStorageUri("/home/admin");
    }

    private void fillNodeCfg(FileNodeCfg fileNodeCfg) {
        fileNodeCfg.setResourceGroupIdentifier("resgroup");
    }
}

// Generated with love by TestMe :) Please report issues and submit feature requests at: http://weirddev.com/forum#!/testme