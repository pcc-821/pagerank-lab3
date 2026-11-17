# 实验三 PageRank 算法实现

山东大学「大数据管理与分析」课程实验。HBase 部分使用 Shell 和 Java API，PageRank 部分在单节点 Hadoop 上分别使用 Hadoop Streaming MapReduce 和 Spark RDD，均提交到 YARN。

## 文件

| 文件 | 用途 |
| --- | --- |
| `hbase-shell.hbase` | 导入 Student、Course、SC 并演示 Shell 操作 |
| `HBaseLab.java` | 实验要求的五个 Java 方法及操作演示 |
| `graph.txt` | 两种 PageRank 实现共用的有向图 |
| `pagerank_mr.py` | MapReduce 驱动、Mapper 和 Reducer |
| `pagerank_spark.py` | PySpark RDD 实现 |

## 环境

实验环境为 VirtualBox 内的 Ubuntu 22.04.5 LTS（4 核、4 GB 内存）、Hadoop 3.4.3、HBase 2.6.7 Hadoop 3 版、Spark 3.5.9 Hadoop 3 版和 Python 3.10。Hadoop 3.4.3 可使用 Java 11 运行。HBase 与 Spark 也支持 Java 11。

在仓库根目录执行命令。先启动已有的 HDFS 与 YARN，并确认 HDFS 安全模式为 OFF，YARN 至少有一个 RUNNING 节点。以下路径按实验环境设置，可按本机安装位置调整。

```bash
export PATH="/usr/local/hadoop/bin:$PATH"
export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
hdfs dfsadmin -safemode get
yarn node -list
```

## HBase Shell 和 Java

启动 HBase 后，先准备测试表，再执行导入脚本。脚本会清空 `lab3_smoke` 用于演示。Java 演示会重建 `lab3_java_demo`。请在实验实例中执行。

```bash
hbase_bin="$HOME/opt/hbase-2.6.7/bin/hbase"
printf "%s\n" "create('lab3_smoke', 'info') unless list.include?('lab3_smoke')" | "$hbase_bin" shell -n
"$hbase_bin" shell -n hbase-shell.hbase
mkdir -p build
hbase_cp="$("$hbase_bin" --internal-classpath classpath)"
javac --release 8 -cp "$hbase_cp" -d build HBaseLab.java &&
  printf '89\n' | HBASE_CLASSPATH="$PWD/build" "$hbase_bin" HBaseLab
```

`Student`、`Course`、`SC` 的行键依次为姓名、课程号、学号。姓名行键保留了学号同为 `2015003` 的 Mary 和 Lisi。`SC` 使用 `Score:课程名` 存成绩，Java 扫描 `Score:Math` 时对缺少该列的 `2015002` 返回 `null`。`89` 是 Java 演示传给 `modifyData` 的新成绩。

## Hadoop Streaming MapReduce

```bash
python3 pagerank_mr.py run graph.txt
```

驱动程序为每轮提交一个 YARN 作业并读取 HDFS 中间状态来汇总无出链网页的 PR 总量。第 10 轮的格式化结果同时写到 HDFS 的运行目录和本地 `mr-result.txt`。

## Spark on YARN

下面的参数适用于单节点虚拟机。启动 Spark 的当前终端应已配置 `HADOOP_CONF_DIR`，同一目录中的 `graph.txt` 会先上传到 HDFS。

```bash
export PYSPARK_PYTHON=/usr/bin/python3
export SPARK_HOME="${SPARK_HOME:-$HOME/opt/spark-3.5.9-bin-hadoop3}"
run_dir="/user/$(id -un)/pagerank-spark-$(date +%Y%m%d-%H%M%S)-$$"
hdfs dfs -mkdir -p "$run_dir" &&
  hdfs dfs -put graph.txt "$run_dir/graph.txt" &&
  "$SPARK_HOME/bin/spark-submit" --master yarn --deploy-mode client \
    --driver-memory 512m --num-executors 1 --executor-cores 1 --executor-memory 512m \
    --conf spark.driver.host=127.0.0.1 --conf spark.yarn.submit.file.replication=1 \
    pagerank_spark.py "$run_dir/graph.txt" "$run_dir/output" &&
  hdfs dfs -cat "$run_dir/output/part-*" > spark-result.txt
diff -u mr-result.txt spark-result.txt
```

Spark 缓存邻接表和当前轮次的 RDD，用 `reduceByKey` 汇总入链贡献，再用 `leftOuterJoin` 保留没有收到入链贡献的网页。

## 数据与验证

`graph.txt` 的每行格式为 `网页<Tab>出链1,出链2`。示例有 6 个节点，`E` 只在出链列表中出现，`F` 的出链为空。两种程序都用均匀初值 `1/N`、阻尼系数 `0.85`，每轮把无出链节点的 PR 均摊给所有节点。第 10 轮按 `(网页,PR值)` 输出，保留 10 位小数。示例输入的参考结果如下。

```text
(A,0.3376709690)
(B,0.1808686069)
(C,0.3509545889)
(D,0.0381009115)
(E,0.0543040123)
(F,0.0381009115)
```
