# Spark基础小题

## 选择题

1)Scala程序编译后的文件以什么结尾：
`A、.class` B、java C、.scala D、.sc
A
2)以下哪种Scala方法可以正确计算数组a的长度：
A、count() B、take（1） C、tail(） `D、length()`
D
3)Scala中如何取出字符串str的后4个字符作为输出？
A、str.get（str.length-5，str-1）B、str.tail（str.length-5，str.length-1）
`C、str.substring（str.length-4，str.length）`D、str.cat（str.length-5，str.length-1）
C
4)下面哪个方法不是用于创建RDD？
A、makeRDD B、parallelize C、textFile `D、saveAsTextFile`
D
5)下面哪个不是RDD的特点：
A、可分区 B、可序列化 `C、可修改` D、可持久化
C
6)下面哪一组全部都是Spark的转换操作：
A、map、take、reduceByKey
B、map、filter、collect
C、map、union、reduceByKey
`D、jion、map、take`
C
7)Spark SQL可以处理的数据源包括：
A、Hive表
B、数据文件、Hive表
C、数据文件、Hive表、RDD
`D、数据文件、Hive表、RDD、外部数据库`
D
8)关于DataFrame的说法错误的是：
A、DataFrame是由SchemaRDD发展而来
`B、DataFrame直接继承了RDD`
C、DataFrame实现了RDD的绝大多数功能
D、DataFrame是一个分布式Row对象的数据集合
B
9)DataFrame的groupBy方法返回的结果是什么类型：
A、DataFrame   B、Column   C、RDD   `D、GroupedData`
D
10)Spark Job 默认的调度模式 ( )
`A FIFO`   B FAIR   C 无   D 运行时指定
A
11)DataFrame 和 RDD 最大的区别 ( )
A.科学统计支持 `B.多了 schema` C.存储方式不一样 D.外部数据源支持
B
12)默认的存储级别 ( )
`A MEMORY_ONLY` B MEMORY_ONLY_SER
C MEMORY_AND_DISK D MEMORY_AND_DISK_SER
A

## 简答

1)对比Hadoop 框架和Spark框架，Spark 有哪些优势？

>  1）基于内存计算，减少低效的磁盘交互；
>
> 2）高效的调度算法，基于DAG ；
>
> 3）容错机制 Linage ，精华部分就是 DAG 和 Lingae 

- Spark的计算模式也是MapReduce，但不局限于Map和Reduce操作，还提供了集操作类型，编程模型比MapReduce更灵活。
- Spark提供内存计算，中间结果直接放到内存中，带来了更高的迭代运算效率
- Spark基于DAG的任务调度执行机制，要优于MapReduce的迭代执行机制

2)什么是宽依赖，什么是窄依赖？哪些算子是宽依赖，哪些是窄依赖？

> RDD 和它依赖的parent RDD(s) 的关系有两种不同的类型，即窄依赖（ narrow dependency ）和宽依赖（ wide 
>
> dependency ）。 
>
> 1）窄依赖指的是每一个parent RDD 的 Partition 最多被子 RDD 的一个 Partition 使用
>
> 2）宽依赖指的是多个子 RDD 的 Partition 会依赖同一个 parent RDD 的 Partition 

- [Spark 中的宽依赖和窄依赖](https://blog.csdn.net/houmou/article/details/52531205?spm=1001.2101.3001.6650.9&utm_medium=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7Edefault-9.no_search_link&depth_1-utm_source=distribute.pc_relevant.none-task-blog-2%7Edefault%7ECTRLIST%7Edefault-9.no_search_link)

- [算子的分类和 宽依赖算子、窄依赖算子](https://blog.csdn.net/aoayyu826824/article/details/102345785)

3)简述spark的工作机制。

> 答：
>
> 1. 用户在 client 端提交作业后，会由Driver 运行 main 方法并创建 spark context 上下文
>
> 2. 执行 rdd 算子，形成dag 图输入 dagscheduler 
> 3. 按照 rdd 之间的依赖关系划分stage 输入 task scheduler 。task scheduler 会将 stage 划分为 task set 分发到各个节点的executor 中执行。

4)简述RDD机制。

>答： 
>
>1. rdd 分布式弹性数据集，简单的理解成一种数据结构，是spark 框架上的通用货币。
>2. 所有算子都是基于rdd来执行的，不同的场景会有不同的rdd 实现类，但是都可以进行互相转换。
>3. rdd 执行过程中会形成dag 图，然后形成 lineage 保证容错性等。
>4. 从物理的角度来看rdd 存储的是 block 和 node之间的映射。

5)spark的有几种部署模式？简述每种模式特点。 P91

> 1）==本地模式==
>
> Spark 不一定非要跑在hadoop 集群，可	以在本地，起多个线程的方式来指定。将Spark 应用以多线程的方式直
>
> 接运行在本地，一般都是为了方便调试，本地模式分三类
>
> · local ：只启动一个 executor 
>
> · local[k]: 启动 k 个 executor 
>
> · local[*] ：启动跟 cpu 数目相同的 executor 2571
>
> 2)==standalone 模式==
>
> 分布式部署集群，自带完整的服务，资源管理和任务监控是 Spark 自己监控，这个模式也是其他模式的基础，
>
> 3)==Spark on yarn 模式==
>
> 分布式部署集群，资源和任务监控交给 yarn 管理，但是目前仅支持粗粒度资源分配方式，包含 cluster 和 client
>
> 运行模式， cluster 适合生产， driver 运行在集群子节点，具有容错功能， client 适合调试， dirver 运行在客户
>
> 端。
>
> 4）==Spark On Mesos 模式==
>
> 官方推荐这种模式（当然，原因之一是血缘关系）。正是由于 Spark 开发之初就考虑到支持 Mesos ，因此，目
>
> 前而言， Spark 运行在 Mesos 上会比运行在 YARN 上更加灵活，更加自然。

- Local
- Standalone
- Spark on Mesos
- Spark on YARN

6)简述spark的优势和劣势。

> 优势：
>
> - 1)速度快
>
> - 2)其次， Spark 是一个灵活的运算框架，适合做批次处理、工作流、交互式分析、流量处理等不同类型的应用，因此 spark 也可以成为一个用途广泛的运算引擎，并在未来取代 MapReduce 的地位
>
> - 3)最后， Spark 可以与 Hadoop 生态系统的很多组件互相操作。Spark 可以运行在新一代资源管理框架YARN 上，它还可以读取已有并存放在 Hadoop 上的数据，这是个非常大的优势
>
> 劣势：
>
> - 1)稳定性方面
>
> - 2)不能处理大数据
>
> - 3)不能支持复杂的 SQL 统计

# Spark基础练习题（一）

## 题目

读取文件的数据test.txt
一共有多少个小于20岁的人参加考试？
一共有多少个等于20岁的人参加考试？
一共有多少个大于20岁的人参加考试？
一共有多个男生参加考试？
一共有多少个女生参加考试？
12班有多少人参加考试？
13班有多少人参加考试？
语文科目的平均成绩是多少？
数学科目的平均成绩是多少？
英语科目的平均成绩是多少？
单个人平均成绩是多少？
12班平均成绩是多少？
12班男生平均总成绩是多少？
12班女生平均总成绩是多少？
13班平均成绩是多少？
13班男生平均总成绩是多少？
13班女生平均总成绩是多少？
全校语文成绩最高分是多少？
12班语文成绩最低分是多少？
13班数学最高成绩是多少？
总成绩大于150分的12班的女生有几个？
总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？

## 数据

```ABAP
12 宋江 25 男 chinese 50
12 宋江 25 男 math 60
12 宋江 25 男 english 70
12 吴用 20 男 chinese 50
12 吴用 20 男 math 50
12 吴用 20 男 english 50
12 杨春 19 女 chinese 70
12 杨春 19 女 math 70
12 杨春 19 女 english 70
13 李逵 25 男 chinese 60
13 李逵 25 男 math 60
13 李逵 25 男 english 70
13 林冲 20 男 chinese 50
13 林冲 20 男 math 60
13 林冲 20 男 english 50
13 王英 19 女 chinese 70
13 王英 19 女 math 80
13 王英 19 女 english 70
```

## 答案

```scala
val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("test"))
1、读取文件的数据test.txt
val file = sc.textFile("test.txt")

2、一共有多少个小于20岁的人参加考试
println(file.groupBy { x => val datas = x.split(" "); datas(0) + "_" + datas(1) + "_" + datas(2) }.filter(_._1.split("_")(2).toInt < 20).count())=

3、一共有多少个等于20岁的人参加考试
println(file.groupBy { x => val datas = x.split(" "); datas(0) + "_" + datas(1) + "_" + datas(2) }.filter(_._1.split("_")(2).toInt == 20).count())

4、一共有多少个大于20岁的人参加考试
println(file.groupBy { x => val datas = x.split(" "); datas(0) + "_" + datas(1) + "_" + datas(2) }.filter(_._1.split("_")(2).toInt > 20).count())

5、一共有多个男生参加考试
println(file.groupBy { x => val datas = x.split(" "); datas(0) + "_" + datas(1) + "_" + datas(3) }.filter(_._1.split("_")(2).equals("男")).count())

6、一共有多少个女生参加考试
println(file.groupBy { x => val datas = x.split(" "); datas(0) + "_" + datas(1) + "_" + datas(3) }.filter(_._1.split("_")(2).equals("女")).count())

7、12班有多少人参加考试
println(file.groupBy { x => val datas = x.split(" "); datas(0) + "_" + datas(1) }.filter(_._1.split("_")(0).equals("12")).count())

8、13班有多少人参加考试
println(file.groupBy { x => val datas = x.split(" "); datas(0) + "_" + datas(1) }.filter(_._1.split("_")(0).equals("13")).count())

9、语文科目的平均成绩是多少
println(file.filter(_.split(" ")(4).equals("chinese")).map(_.split(" ")(5).toFloat).mean())

10、数学科目的平均成绩是多少
println(file.filter(_.split(" ")(4).equals("math")).map(_.split(" ")(5).toFloat).mean())

11、英语科目的平均成绩是多少
println(file.filter(_.split(" ")(4).equals("english")).map(_.split(" ")(5).toFloat).mean())

12、单个人平均成绩是多少
println(file.map(x => {
  val line = x.split(" ");
  (line(0) + "_" + line(1), line(5).toInt)
}).map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2)).collect().mkString(","))

13、12班平均成绩是多少
println(file.filter(_.split(" ")(0).equals("12")).map(x => {
val line = x.split(" "); (line(0), line(5).toInt)
}).map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2)).map(_._2).collect().mkString(","))

14、12班男生平均总成绩是多少
println(file.filter { x => var datas = x.split(" "); datas(0).equals("12") && datas(3).equals("男") }.map(x => {val line = x.split(" "); (line(0), line(5).toInt)})
.map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2)).map(_._2).collect().mkString(","))

15、12班女生平均总成绩是多少？
println(file.filter { x => var datas = x.split(" "); datas(0).equals("12") && datas(3).equals("女") }.map(x => {val line = x.split(" "); (line(0), line(5).toInt)})
.map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2)).map(_._2).collect().mkString(","))

16、13班平均成绩是多少？
println(file.filter(_.split(" ")(0).equals("13")).map(x => {val line = x.split(" "); (line(0), line(5).toInt)
}).map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2)).map(_._2).collect().mkString(","))

17、13班男生平均总成绩是多少
println(file.filter { x => var datas = x.split(" "); datas(0).equals("13") && datas(3).equals("男") }.map(x => {val line = x.split(" "); (line(0), line(5).toInt)}).map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2)).map(_._2).collect().mkString(","))

18、13班女生平均总成绩是多少
println(file.filter { x => var datas = x.split(" "); datas(0).equals("13") && datas(3).equals("女") }.map(x => {val line = x.split(" "); (line(0), line(5).toInt)}).map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2)).map(_._2).collect().mkString(","))

19、全校语文成绩最高分是多少
println(file.filter {_.split(" ")(4).equals("chinese")}.map(_.split(" ")(5)).max())
    
20、12班语文成绩最低分是多少
println(file.filter { x => var datas = x.split(" "); datas(4).equals("chinese") && datas(0).equals("12") }.map(_.split(" ")(5)).min())
    
21、13班数学最高成绩是多少
println(file.filter { x => var datas = x.split(" "); datas(4).equals("math") && datas(0).equals("13") }.map(_.split(" ")(5)).max())

22、总成绩大于150分的12班的女生有几个
//方式一：无shuffle
println(file.filter { x => var datas = x.split(" "); datas(3).equals("女") && datas(0).equals("12") }.groupBy(_.split(" ")(1)).map {
  case (name, list) => {
     list.map(_.split(" ")(5).toFloat).sum
  }
}.filter(_ > 150).count())

//方式二：有shuffle
println(file.filter { x => var datas = x.split(" "); datas(3).equals("女") && datas(0).equals("12") }
.map { x => var datas = x.split(" "); (datas(1), datas(5).toInt) }
.reduceByKey(_ + _).filter(_._2 > 150).count())

23、总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少
val data1 = file.map(x => {val line = x.split(" "); (line(0) + "," + line(1) + "," + line(3), line(5).toInt)}) //
val data2 = file.map(x => {val line = x.split(" "); (line(0) + "," + line(1) + "," + line(3) + "," + line(4), line(5).toInt)})

//过滤出总分大于150的,并求出平均成绩
val com1: RDD[(String, Int)] = data1.map(a => (a._1, (a._2, 1))).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).filter(x => (x._2._1 > 150)).map(t => (t._1, t._2._1 / t._2._2))

//过滤出 数学大于等于70，且年龄大于等于19岁的学生
val com2: RDD[(String, Int)] = data2
.filter(x => {val datas = x._1.split(","); datas(3).equals("math") && x._2 > 70})
.map(x => {val datas = x._1.split(","); (datas(0) + "," + datas(1) + "," + datas(2), x._2.toInt)})

println(com1.join(com2).map(x => (x._1, x._2._1)).collect().mkString(","))

sc.stop()
```

# Spark基础练习题（二）

## 题目

1、创建一个1-10数组的RDD，将所有元素*2形成新的RDD

2、创建一个10-20数组的RDD，使用mapPartitions将所有元素*2形成新的RDD

3、创建一个元素为 1-5 的RDD，运用 flatMap创建一个新的 RDD，新的 RDD 为原 RDD 每个元素的 平方和三次方 来组成 1,1,4,8,9,27..

4、创建一个 4 个分区的 RDD数据为Array(10,20,30,40,50,60)，使用glom将每个分区的数据放到一个数组

5、创建一个 RDD数据为Array(1, 3, 4, 20, 4, 5, 8)，按照元素的奇偶性进行分组

6、创建一个 RDD（由字符串组成）Array("xiaoli", "laoli", "laowang", "xiaocang", "xiaojing", "xiaokong")，过滤出一个新 RDD（包含“xiao”子串）

7、创建一个 RDD数据为1 to 10，请使用sample不放回抽样

8、创建一个 RDD数据为1 to 10，请使用sample放回抽样

9、创建一个 RDD数据为Array(10,10,2,5,3,5,3,6,9,1),对 RDD 中元素执行去重操作

10、创建一个分区数为5的 RDD，数据为0 to 100，之后使用coalesce再重新减少分区的数量至 2

11、创建一个分区数为5的 RDD，数据为0 to 100，之后使用repartition再重新减少分区的数量至 3

12、创建一个 RDD数据为1,3,4,10,4,6,9,20,30,16,请给RDD进行分别进行升序和降序排列

13、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，求并集

14、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，计算差集，两个都算

15、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，计算交集

16、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，计算 2 个 RDD 的笛卡尔积

17、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 5和11 to 15，对两个RDD拉链操作

18、创建一个RDD数据为List(("female",1),("male",5),("female",5),("male",2))，请计算出female和male的总数分别为多少

19、创建一个有两个分区的 RDD数据为List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8))，取出每个分区相同key对应值的最大值，然后相加

20、 创建一个有两个分区的 pairRDD数据为Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))，根据 key 计算每种 key 的value的平均值

21、统计出每一个省份广告被点击次数的 TOP3，数据在access.log文件中
数据结构：时间戳，省份，城市，用户，广告 字段使用空格分割。
样本如下：
1516609143867 6 7 64 16
1516609143869 9 4 75 18
1516609143869 1 7 87 12

22、读取本地文件words.txt,统计出每个单词的个数，保存数据到 hdfs 上

23、读取 people.json 数据的文件, 每行是一个 json 对象，进行解析输出

24、保存一个 SequenceFile 文件，使用spark创建一个RDD数据为Array(("a", 1),("b", 2),("c", 3))，保存为SequenceFile格式的文件到hdfs上

25、读取24题的SequenceFile 文件并输出

26、读写 objectFile 文件，把 RDD 保存为objectFile，RDD数据为Array(("a", 1),("b", 2),("c", 3))，并进行读取出来

27、使用内置累加器计算Accumulator.txt文件中空行的数量

28、使用Spark广播变量
用户表：
id name age gender(0|1)
001,刘向前,18,0
002,冯  剑,28,1
003,李志杰,38,0
004,郭  鹏,48,2
要求，输出用户信息，gender必须为男或者女，不能为0,1
使用广播变量把Map("0" -> "女", "1" -> "男")设置为广播变量，最终输出格式为
001,刘向前,18,女
003,李志杰,38,女
002,冯  剑,28,男
004,郭  鹏,48,男

29、mysql创建一个数据库bigdata0407，在此数据库中创建一张表
CREATE TABLE `user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(32) NOT NULL COMMENT '用户名称',
  `birthday` date DEFAULT NULL COMMENT '生日',
  `sex` char(1) DEFAULT NULL COMMENT '性别',
  `address` varchar(256) DEFAULT NULL COMMENT '地址',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
数据依次是：姓名 生日 性别 省份
请使用spark将以上数据写入mysql中，并读取出来

30、在hbase中创建一个表student，有一个 message列族
create 'student', 'message'
scan 'student', {COLUMNS => 'message'}
给出以下数据，请使用spark将数据写入到hbase中的student表中,并进行查询出来
数据如下：
依次是：姓名 班级 性别 省份，对应表中的字段依次是：name,class,sex,province

## 数据

```ABAP
因为有几个文件数据较多，所以不方便在这里贴出来，所以把它上传到了百度云上，对应的资源在这里
链接: https://pan.baidu.com/s/1XMdS2BHOdCHaaC8y-OiEOw 提取码: 1234 复制这段内容后打开百度网盘手机App，操作更方便哦

如果链接失效请私信我！

```

## 答案

```scala
val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("HomeWork20200407"))
// 1、创建一个1-10数组的RDD，将所有元素*2形成新的RDD
 val data1 = sc.makeRDD(1 to 10)
 val data1Result = data1.map(_ * 2)
 
 // 2、创建一个10-20数组的RDD，使用mapPartitions将所有元素*2形成新的RDD
 val data2 = sc.makeRDD(10 to 20)
 val data2Result = data2.mapPartitions(_.map(_ * 2))
 
 // 3、创建一个元素为 1-5 的RDD，运用 flatMap创建一个新的 RDD，新的 RDD 为原 RDD 每个元素的 平方和三次方 来组成 1,1,4,8,9,27..
 val data3 = sc.makeRDD(1 to 5)
 val data3Result = data3.flatMap(x => Array(math.pow(x, 2), math.pow(x, 3)))
 
 // 4、创建一个 4 个分区的 RDD数据为Array(10,20,30,40,50,60)，使用glom将每个分区的数据放到一个数组
 val data4 = sc.makeRDD(Array(10, 20, 30, 40, 50, 60))
 val data4Result = data4.glom()
 
 // 5、创建一个 RDD数据为Array(1, 3, 4, 20, 4, 5, 8)，按照元素的奇偶性进行分组
 val data5 = sc.makeRDD(Array(1, 3, 4, 20, 4, 5, 8))
 val data5Result = data5.groupBy(x => if (x % 2 == 0) "偶数" else "奇数")
 
 // 6、创建一个 RDD（由字符串组成）Array("xiaoli", "laoli", "laowang", "xiaocang", "xiaojing", "xiaokong")，过滤出一个新 RDD（包含“xiao”子串）
 val data6 = sc.makeRDD(Array("xiaoli", "laoli", "laowang", "xiaocang", "xiaojing", "xiaokong"))
 val data6Result = data6.filter(_.contains("xiao"))
 
 // 7、创建一个 RDD数据为1 to 10，请使用sample不放回抽样
 val data7 = sc.makeRDD(1 to 10)
 val data7Result = data7.sample(false, 0.5, 1)
 
 // 8、创建一个 RDD数据为1 to 10，请使用sample放回抽样
 val data8 = sc.makeRDD(1 to 10)
 val data8Result = data8.sample(true, 0.5, 1)
 
 // 9、创建一个 RDD数据为Array(10,10,2,5,3,5,3,6,9,1),对 RDD 中元素执行去重操作
 val data9 = sc.makeRDD(Array(10, 10, 2, 5, 3, 5, 3, 6, 9, 1))
 val data9Result = data9.distinct()
 
 // 10、创建一个分区数为5的 RDD，数据为0 to 100，之后使用coalesce再重新减少分区的数量至 2
 val data10 = sc.makeRDD(0 to 100, 5)
 val data10Result = data10.coalesce(2)
 
 // 11、创建一个分区数为5的 RDD，数据为0 to 100，之后使用repartition再重新减少分区的数量至 3
 val data11 = sc.makeRDD(0 to 100, 5)
 val data11Result = data11.repartition(3)
 
 // 12、创建一个 RDD数据为1,3,4,10,4,6,9,20,30,16,请给RDD进行分别进行升序和降序排列
 val data12 = sc.makeRDD(Array(1, 3, 4, 10, 4, 6, 9, 20, 30, 16))
 val data12Result1 = data12.sortBy(x => x)
 val data12Result2 = data12.sortBy(x => x, false)
 
 // 13、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，求并集
 val data13_1 = sc.makeRDD(1 to 6)
 val data13_2 = sc.makeRDD(4 to 10)
 val data13Result = data13_1.union(data13_2)
 
 // 14、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，计算差集，两个都算
 val data14_1 = sc.makeRDD(1 to 6)
 val data14_2 = sc.makeRDD(4 to 10)
 val data14Result_1 = data14_1.subtract(data14_2)
 val data14Result_2 = data14_2.subtract(data14_1)
 
 // 15、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，计算交集
 val data15_1 = sc.makeRDD(1 to 6)
 val data15_2 = sc.makeRDD(4 to 10)
 val data15Result_1 = data15_1.intersection(data15_2)
 
 // 16、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 6和4 to 10，计算 2 个 RDD 的笛卡尔积
 val data16_1 = sc.makeRDD(1 to 6)
 val data16_2 = sc.makeRDD(4 to 10)
 val data16Result = data16_1.cartesian(data16_2)
 
 // 17、创建两个RDD，分别为rdd1和rdd2数据分别为1 to 5和11 to 15，对两个RDD拉链操作
 val data17_1 = sc.makeRDD(1 to 5)
 val data17_2 = sc.makeRDD(11 to 15)
 val data17Result = data17_1.zip(data17_2)
 
 // 18、创建一个RDD数据为List(("female",1),("male",5),("female",5),("male",2))，请计算出female和male的总数分别为多少
 val data18 = sc.makeRDD(List(("female", 1), ("male", 5), ("female", 5), ("male", 2)))
 val data18Result = data18.reduceByKey(_ + _)
 
 // 19、创建一个有两个分区的 RDD数据为List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8))，取出每个分区相同key对应值的最大值，然后相加
 /**
  * (a,3),(a,2),(c,4)
  * (b,3),(c,6),(c,8)
  */
 val data19 = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
 data19.glom().collect().foreach(x => println(x.mkString(",")))
 val data19Result = data19.aggregateByKey(0)(math.max(_, _), _ + _)
 
 // 20、创建一个有两个分区的 pairRDD数据为Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98))，根据 key 计算每种 key 的value的平均值
 val data20 = sc.makeRDD(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)))
 val data20Result = data20.groupByKey().map(x => x._1 -> x._2.sum / x._2.size)
 //或val data20Result = data20.map(x => (x._1, (x._2, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2))
 
 // 21、统计出每一个省份广告被点击次数的 TOP3，数据在access.log文件中
 // 数据结构：时间戳，省份，城市，用户，广告 字段使用空格分割。
 val file1 = sc.textFile("input20200407/access.log")
 file1.map { x => var datas = x.split(" "); (datas(1), (datas(4), 1)) }.groupByKey().map {
   case (province, list) => {
     val tuples = list.groupBy(_._1).map(x => (x._1, x._2.size)).toList.sortWith((x, y) => x._2 > y._2).take(3)
     (province, tuples)
   }
 }.collect().sortBy(_._1).foreach(println)
 
 // 22、读取本地文件words.txt,统计出每个单词的个数，保存数据到 hdfs 上
 val file2 = sc.textFile("file:///C:/Develop/HomeWork/input20200407/words.txt")
 file2.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile("hdfs://mycluster:8020/20200407_wordsOutput")
 
 // 23、读取 people.json 数据的文件, 每行是一个 json 对象，进行解析输出
 import scala.util.parsing.json.JSON
 val file3 = sc.textFile("input20200407/people.json")
 val result: RDD[Option[Any]] = file3.map(JSON.parseFull)
 
 // 24、保存一个 SequenceFile 文件，使用spark创建一个RDD数据为Array(("a", 1),("b", 2),("c", 3))，保存为SequenceFile格式的文件到hdfs上
 val data24 = sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3)))
 data24.saveAsSequenceFile("hdfs://mycluster:8020/20200407_SequenceFile")
 
 // 25、读取24题的SequenceFile 文件并输出
 val data25: RDD[(String,Int)] = sc.sequenceFile[String,Int]("hdfs://mycluster:8020/20200407_SequenceFile/part-00000")
 
 // 26、读写 objectFile 文件，把 RDD 保存为objectFile，RDD数据为Array(("a", 1),("b", 2),("c", 3))，并进行读取出来
 val data26_1 = sc.makeRDD(Array(("a", 1), ("b", 2), ("c", 3)))
 data26_1.saveAsObjectFile("output20200407/20200407_objectFile")
 val data26_2 = sc.objectFile("output20200407/20200407_objectFile")
 
 // 27、使用内置累加器计算Accumulator.txt文件中空行的数量
 val data27 = sc.textFile("input20200407/Accumulator.txt")
 var count = sc.longAccumulator("count")
 data27.foreach { x => if (x == "") count.add(1) }
 println(count.value)
 
 /**
  * 28、使用Spark广播变量
  * 用户表：
  * id name age gender(0|1)
  * 001,刘向前,18,0
  * 002,冯  剑,28,1
  * 003,李志杰,38,0
  * 004,郭  鹏,48,1
  * 要求，输出用户信息，gender必须为男或者女，不能为0,1
  * 使用广播变量把Map("0" -> "女", "1" -> "男")设置为广播变量，最终输出格式为
  * 001,刘向前,18,女
  * 003,李志杰,38,女
  * 002,冯  剑,28,男
  * 004,郭  鹏,48,男
  */
 val data28 = sc.textFile("input20200407/user.txt")
 val sex = sc.broadcast(Map("0" -> "女", "1" -> "男"))
 data28.foreach { x => var datas = x.split(","); println(datas(0) + "," + datas(1) + "," + datas(2) + "," + sex.value(datas(3))) }

 /**
  * 29、mysql创建一个数据库bigdata0407，在此数据库中创建一张表
  * CREATE TABLE `user` (
  * `id` int(11) NOT NULL AUTO_INCREMENT,
  * `username` varchar(32) NOT NULL COMMENT '用户名称',
  * `birthday` date DEFAULT NULL COMMENT '生日',
  * `sex` char(1) DEFAULT NULL COMMENT '性别',
  * `address` varchar(256) DEFAULT NULL COMMENT '地址',
  * PRIMARY KEY (`id`)
  * ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
  * 数据如下：
  * 依次是：姓名 生日 性别 省份
  * 安荷 1998/2/7 女 江苏省
  * 白秋 2000/3/7 女 天津市
  * 雪莲 1998/6/7 女 湖北省
  * 宾白 1999/7/3 男 河北省
  * 宾实 2000/8/7 男 河北省
  * 斌斌 1998/3/7 男 江苏省
  * 请使用spark将以上数据写入mysql中，并读取出来。
  */
 val data29 = sc.textFile("input20200407/users.txt")
 val driver = "com.mysql.jdbc.Driver"
 val url = "jdbc:mysql://localhost:3306/bigdata0407"
 val username = "root"
 val password = "root"
/**
 * MySQL插入数据
 */
 data29.foreachPartition {
   data =>
     Class.forName(driver)
     val connection = java.sql.DriverManager.getConnection(url, username, password)
     val sql = "INSERT INTO `user` values (NULL,?,?,?,?)"
     data.foreach {	
       tuples => {
         val datas = tuples.split(" ")
         val statement = connection.prepareStatement(sql)
         statement.setString(1, datas(0))
         statement.setString(2, datas(1))
         statement.setString(3, datas(2))
         statement.setString(4, datas(3))
         statement.executeUpdate()
         statement.close()
       }
     }
     connection.close()
 }
/**
 * MySQL查询数据
  */
 var sql = "select * from `user` where id between ? and ?"
 val jdbcRDD = new JdbcRDD(sc,
   () => {
     Class.forName(driver)
     java.sql.DriverManager.getConnection(url, username, password)
   },
   sql,
   0,
   44,
   3,
   result => {
     println(s"id=${result.getInt(1)},username=${result.getString(2)}" +
       s",birthday=${result.getDate(3)},sex=${result.getString(4)},address=${result.getString(5)}")
   }
 )
 jdbcRDD.collect() 


 /**
  * 30、在hbase中创建一个表student，有一个 message列族
  * create 'student', 'message'
  * scan 'student', {COLUMNS => 'message'}
  * 给出以下数据，请使用spark将数据写入到hbase中的student表中,并进行查询出来
  * 数据如下：
  * 依次是：姓名 班级 性别 省份，对应表中的字段依次是：name,class,sex,province
  */
 //org.apache.hadoop.hbase.mapreduce.TableInputFormat
 val conf = HBaseConfiguration.create()
 conf.set("hbase.zookeeper.quorum", "node01:2181,node02:2181,node03:2181")
 conf.set(TableInputFormat.INPUT_TABLE, "student")
 /**
  * HBase插入数据
  */
 val dataRDD: RDD[String] = sc.textFile("input20200407/student.txt")
 val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
   //飞松	3	女	山东省
   case line => {
     val datas = line.split("\t")
     val rowkey = Bytes.toBytes(datas(0))
     val put = new Put(rowkey)
     put.addColumn(Bytes.toBytes("message"), Bytes.toBytes("name"), Bytes.toBytes(datas(0)))
     put.addColumn(Bytes.toBytes("message"), Bytes.toBytes("class"), Bytes.toBytes(datas(1)))
     put.addColumn(Bytes.toBytes("message"), Bytes.toBytes("sex"), Bytes.toBytes(datas(2)))
     put.addColumn(Bytes.toBytes("message"), Bytes.toBytes("province"), Bytes.toBytes(datas(3)))
     (new ImmutableBytesWritable(rowkey), put)
   }
 }
 val jobConf = new JobConf(conf)
 //org.apache.hadoop.hb8ase.mapred.TableOutputFormat
 jobConf.setOutputFormat(classOf[TableOutputFormat])
 jobConf.set(TableOutputFormat.OUTPUT_TABLE, "student")
 putRDD.saveAsHadoopDataset(jobConf)


 /**
  * HBase查询数据
  */
 val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
   classOf[ImmutableBytesWritable],
   classOf[Result])
 hbaseRDD.foreach {
   case (rowKey, result) => {
     val cells: Array[Cell] = result.rawCells()
     for (cell <- cells) {
       println(Bytes.toString(CellUtil.cloneRow(cell)) + "\t" +
         Bytes.toString(CellUtil.cloneFamily(cell)) + "\t" +
         Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t" +
         Bytes.toString(CellUtil.cloneValue(cell))
       )
     }
   }
 }
```

