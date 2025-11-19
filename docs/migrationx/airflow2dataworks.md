### 从Github下载转换工具dataworks-flowspec
下载地址： `https://github.com/idealivan/dataworks-spec`  
分支： `pr/doc_refined_20251119`

### 在Airflow环境执行DAG导出WorkFlow操作
将DAG到WorkfFlow转换工具（python脚本）部署到Airflow环境，根据实际情况调整如下目录信息，其中

+ `-m`参数是转换配置，配置Operator到DataWorks节点类型映射关系，以及节点所使用的资源组(通过`"settings"."workflow.converter.target.schedule.resGroupIdentifier"`设置)
    - 注意，_**不配置资源组信息，导入到DataWorks时会失败**_
+ `-d`参数是需要转换的DAG文件所处的目录（也可以指向具体的DAG文件，意味着只转换对应的DAG文件，指向目录时，转换目录下所有DAG）
+ `-o`参数是转换WorkFlow文件的存储路径（json文件，这些文件需要提交到DataWorks，形成DataWorks工作流定义）

```shell
export PYTHON3=/PythonHome/versions/3.9.12/bin/python3
# dataworks-spec/client/migrationx/migrationx-transformer/src/main/python
export PARSER_HOME=/path/to/dataworks-spec/client/migrationx/migrationx-transformer/src/main/python/

cd ${PARSER_HOME}
cp ${PARSER_HOME}/airflow_dag_parser/parser.py ${PARSER_HOME}/parser.py
PYTHON3 parser.py \
  -m /path/to/converting_config/flowspec-airflowV2-transformer-config.json \
  -d /path/to/airflow/dag/file/location/ \
  -o /path/for/saving/exported_workflows/
# restore parsing directory
rm ${PARSER_HOME}/parser.py
```

#### 配置文件样例（[flowspec-airflowV2-transformer-config.json](https://github.com/idealivan/dataworks-spec/blob/master/client/migrationx/migrationx-transformer/src/main/conf/flowspec-airflowV2-transformer-config.json)）
```json
{
  // 此处可以替换为实际的目录
  "workflowPathPrefix": "Airflow导入/",
  "typeMapping": {
    "EmptyOperator": "VIRTUAL",
    "ExternalTaskSensor": "VIRTUAL",
    "BashOperator": "SSH",
    "PythonOperator": "SSH",
    "SQLExecuteQueryOperator": "StarRocks",
    "MySqlOperator": "StarRocks",
    "BranchPythonOperator": "CONTROLLER_BRANCH",
    "HiveOperator": "ODPS_SQL",
    "SqoopOperator": "EMR_SQOOP",
    "SparkSqlOperator": "EMR_SPARK_SQL",
    "SparkSubmitOperator": "EMR_SPARK",
    "PostgresOperator": "Postgresql",
    "default": "PYTHON"
  },
  "settings": {
    // 此处需要替换为实际使用的资源组标识
    "workflow.converter.target.schedule.resGroupIdentifier": "Serverless_res_group_***_***" 
  }
}

```

### 将导出的WorkfFlow导入到DataWorks
将上述步骤`-o`参数所指定目录下的WorkFlow文件导入到DataWorks，对应的Java Main函数入口是:[DataWorksMigrationSpecificationImportWriter.java](https://github.com/idealivan/dataworks-spec/blob/master/client/migrationx/migrationx-writer/src/main/java/com/aliyun/dataworks/migrationx/writer/DataWorksMigrationSpecificationImportWriter.java)

+ 该入口方法接受如下参数
    - `-e,--endpoint <arg>`
        * DataWorks OpenAPI endpoint, example:  dataworks.cn-shanghai.aliyuncs.com
    - `-f,--flowspecFolder <arg>`
        * Flowspec file folder
    - `-i,--accessKeyId <arg>`
        * Access key id
    - `-k,--accessKey <arg>`
        * Access key secret
    - `-p,--projectId <arg>`
        * DataWorks Project ID
    - `-r,--regionId <arg>`
        * Region id, example: cn-shanghai
+ 参数样例参考

```shell
-e dataworks.cn-shanghai.aliyuncs.com # 其中cn-shanghai需要替换为实际的region
-f /path/for/saving/exported_workflows/  # 替换为实际的WorkFlow存储位置
-r cn-shanghai # 替换为所使用DataWorks的Region
-i ACCESS_ID  # 替换为实际用户的AK_ID
-k ACCESS_KEY # 替换为实际用户的AK_SECRET
-p 1234567890  # 替换为实际的工作空间ID（数字型）
```

### 在DataWorks IDE（新版本）确认导入工作流信息
+ [https://dataworks.data.aliyun.com/cn-shanghai/ide?defaultProjectId=1234567890](https://dataworks.data.aliyun.com/cn-shanghai/ide?defaultProjectId=1234567890) （region和projectId替换为实际的值）





