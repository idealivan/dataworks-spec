/*
 * Copyright (c) 2024, Alibaba Cloud;
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.dataworks.common.spec.domain.ref;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONWriter.Feature;

import com.aliyun.dataworks.common.spec.SpecUtil;
import com.aliyun.dataworks.common.spec.domain.DataWorksWorkflowSpec;
import com.aliyun.dataworks.common.spec.domain.Specification;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author 聿剑
 * @date 2024/7/29
 */
public class SpecNodeTest {
    @Test
    public void testEquals() {
        SpecNode n1 = new SpecNode();
        n1.setId("n1");
        n1.setName("n1");
        SpecDatasource ds1 = new SpecDatasource();
        ds1.setName("ds1");
        ds1.setType("odps");
        n1.setDatasource(ds1);

        SpecNode n2 = new SpecNode();
        n2.setId("n1");
        n2.setName("n1");
        SpecDatasource ds2 = new SpecDatasource();
        ds2.setName("ds2");
        ds2.setType("odps");
        n2.setDatasource(ds2);

        Assert.assertNotEquals(n1, n2);

        ds2.setName("ds1");
        Assert.assertEquals(ds1, ds2);
        Assert.assertEquals(n1, n2);

        n2.setName("n2");
        Assert.assertNotEquals(n1, n2);
    }

    @Test
    public void testEqualsOutputInput() {
        SpecNode n1 = new SpecNode();
        n1.setId("n1");
        n1.setName("n1");
        SpecDatasource ds1 = new SpecDatasource();
        ds1.setName("ds1");
        ds1.setType("odps");
        n1.setDatasource(ds1);

        SpecNode n2 = new SpecNode();
        n2.setId("n1");
        n2.setName("n1");
        SpecDatasource ds2 = new SpecDatasource();
        ds2.setName("ds2");
        ds2.setType("odps");
        n2.setDatasource(ds2);

        Assert.assertNotEquals(n1, n2);

        ds2.setName("ds1");
        Assert.assertEquals(ds1, ds2);
        Assert.assertEquals(n1, n2);

        SpecNodeOutput in1 = new SpecNodeOutput();
        in1.setData("output1");
        in1.setRefTableName("refTable1");
        n1.setInputs(Collections.singletonList(in1));

        SpecNodeOutput in2 = new SpecNodeOutput();
        in2.setData("output1");
        in2.setRefTableName("refTable1");
        n2.setInputs(Collections.singletonList(in2));
        Assert.assertEquals(n1, n2);
    }

    @Test
    public void testTimeoutUnit() {
        String spec = "{\n"
            + "    \"version\": \"1.0.0\",\n"
            + "    \"kind\": \"CycleWorkflow\",\n"
            + "    \"nodes\": [\n"
            + "      {\n"
            + "        \"id\": \"c05cc423ac8046a7b18ccc9dd88ef27e\",\n"
            + "        \"recurrence\": \"Normal\",\n"
            + "        \"timeout\": 3,\n"
            + "        \"timeoutUnit\": \"seconds\",\n"
            + "        \"instanceMode\": \"T+1\",\n"
            + "        \"rerunMode\": \"Allowed\",\n"
            + "        \"rerunTimes\": 3,\n"
            + "        \"rerunInterval\": 180000,\n"
            + "        \"script\": {\n"
            + "          \"language\": \"odps\",\n"
            + "          \"runtime\": {\n"
            + "            \"engine\": \"MaxCompute\",\n"
            + "            \"command\": \"ODPS_SQL\"\n"
            + "          },\n"
            + "          \"parameters\": [\n"
            + "            {\n"
            + "              \"name\": \"bizdate\",\n"
            + "              \"scope\": \"NodeParameter\",\n"
            + "              \"type\": \"System\",\n"
            + "              \"value\": \"$[yyyymmdd]\"\n"
            + "            }\n"
            + "          ]\n"
            + "        },\n"
            + "        \"trigger\": {\n"
            + "          \"id\": \"ddb2d936a16a4a45bc34b68c30d05f84\",\n"
            + "          \"type\": \"Scheduler\",\n"
            + "          \"cron\": \"00 00 00 * * ?\",\n"
            + "          \"startTime\": \"1970-01-01 00:00:00\",\n"
            + "          \"endTime\": \"9999-01-01 00:00:00\",\n"
            + "          \"timezone\": \"Asia/Shanghai\"\n"
            + "        },\n"
            + "        \"runtimeResource\": {\n"
            + "          \"resourceGroup\": \"dataphin_scheduler_pre\"\n"
            + "        },\n"
            + "        \"name\": \"p_param_2\",\n"
            + "        \"owner\": \"064152\",\n"
            + "        \"inputs\": {},\n"
            + "        \"outputs\": {\n"
            + "          \"outputs\": [\n"
            + "            {\n"
            + "              \"type\": \"Output\",\n"
            + "              \"data\": \"c05cc423ac8046a7b18ccc9dd88ef27e\",\n"
            + "              \"refTableName\": \"p_param_2\"\n"
            + "            }\n"
            + "          ]\n"
            + "        },\n"
            + "        \"functions\": [],\n"
            + "        \"fileResources\": []\n"
            + "      }\n"
            + "    ],\n"
            + "    \"flow\": [\n"
            + "      {\n"
            + "        \"nodeId\": \"c05cc423ac8046a7b18ccc9dd88ef27e\",\n"
            + "        \"depends\": [\n"
            + "          {\n"
            + "            \"type\": \"Normal\",\n"
            + "            \"output\": \"dw_scheduler_pre.test_sql002\"\n"
            + "          }\n"
            + "        ]\n"
            + "      }\n"
            + "    ]\n"
            + "  }";

        Specification<DataWorksWorkflowSpec> specObj = SpecUtil.parseToDomain(spec);
        assertNotNull(specObj);

        DataWorksWorkflowSpec specification = specObj.getSpec();
        String str = SpecUtil.writeToSpec(specObj);
        System.out.println(str);

        specification.getFlow().forEach(flow -> System.out.println(JSON.toJSONString(flow.getDepends().get(0).getOutput(), Feature.PrettyFormat)));
        assertEquals(TimeUnit.SECONDS, specification.getNodes().get(0).getTimeoutUnit());
    }
}
