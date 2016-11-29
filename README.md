# usercf_recommendation
实验性基于用户的协同过滤推荐系统

集群的HDFS以64MB为单位组成一个块，每个块在每个从节点上各存一个副本，共两个副本。集群中只能同时运行2个map任务，两个从节点各只能同时运行1个map任务。这大大限制了整个集群的运行速度。

第1步：将数据输入整理，为计算欧式相似矩阵准备数据。map阶段将原始数据（矩阵）的itemID作为key，userID+score组合作为value写入reduce阶段。reduce阶段将做双层for()循环，将所有的userID两两组对作为key，将对应的score作为value输出，为第2步计算相似度做准备。

输入数据集大小：692.1MB，共11个块；

MapReduce过程总用时：1hrs, 26mins, 30sec；

map任务共11个，每个map任务平均用时9sec；

reduce任务共1个，reduce任务用时1hrs, 25min, 11sec；

Slave1完成了2个map任务，1个reduce任务；

Slave2完成了9个map任务；\n
map过程输入记录30287614个，输出记录30287611个，输出数据474214924B；
reduce过程输入记录30287611个，输出记录3210728535个；
输出数据集大小：54.1G，共867个块。
第2步：依据第1步的输出数据，计算欧式相似矩阵。map阶段将第1步reduce阶段的key、value原封不动地传给reduce阶段。reduce阶段以userID的两两组对为key，聚合了对应的userID作出的所有评分(score)为values，依据公式对每两个用户之间的相似度进行计算。以userID的两两组对为key，以对应的这两个userID之间的相似度(similarity)为value输出。
输入数据集大小：54.1G，共867个块；
MapReduce过程总用时：2hrs, 43mins, 11sec； 
map任务共866个，每个map任务平均用时15sec；
reduce任务共1个，reduce任务用时45mins, 47sec；
Suhffle 用时1hrs, 33mins, 52sec；
Slave1完成了个538map任务；
Slave2完成了个328map任务，1个reduce任务；
map过程输入记录3210728535个，输出记录3210728535个，输出数据58119867290B；
reduce过程输入记录3210728535个，输出记录20046个；
输出数据集大小：350.3K，共1个块。
第3步：依据第2步的输出数据，根据欧式相似度找出用户相似度最高的前10个用户。map阶段将第2步userID两两组对的前一个userID单独拿出来作为key，以另一个userID以及该userID与作为key的userID之间的相似度作为value写入reduce阶段。reduce阶段则以所有的userID作为key，聚合了该userID与其他所有的userID之间的相似度。按照相似度由大到小排序，选择前10个相似度最高的userID及其对应的相似度的值作为value输出。
输入数据集大小：350.3K，共1个块； 
MapReduce过程总用时： 10sec；
map任务共1个，map任务用时1sec；
reduce任务共1个，reduce任务用时1sec
Slave1完成了1个map任务，1个reduce任务；
map过程输入记录20046个，输出记录20046个，输出数据358710B；
reduce过程输入记录20046个，输出记录142个；
输出数据集大小：21.04K，共1个块。
第4步：依据第3步的输出数据以及原始数据，计算出每个用户与相似度最高的10个用户之间未买过的物品进行推荐。map阶段针对第3步输出的数据做了item数量的for()循环，以每次循环的标记i的值（即itemID）作为key，以每条记录中提取出的所有的userID组合（根据第3步的输出，userID的组合中第一个userID是需要对其进行推荐的，其后的所有userID都是第一个userID的相似用户，在reduce中根据后面所有userID对某一物品的评分计算并判断是否对第一个userID推荐该物品）并带上特定标记的值作为value写入reduce阶段；针对原始数据，则以itemID作为key，userID+score组合作为value并带上特定标记的值作为value写入reduce阶段。reduce阶段则以所有的itemID作为key，聚合了所有需要判断是否向其推荐该商品的userID（在原始矩阵中该userID对该itemID的评分为0）以及它的相似用户。在相似用户中从所有标记为来自原始矩阵的记录中提取相应userID对应的评分，根据公式计算被推荐userID对该itemID可能的预测评分，最终以被推荐userID为key，以相应itemID及被推荐userID对该itemID可能的预测评分组合为value输出。
输入数据集大小： 21.04K+692.1M，共1+11个块；
MapReduce过程总用时： 8mins, 35sec；
map任务共12个，每个map任务平均用时22sec；
reduce任务共1个，reduce任务用时2min, 25sec；
Shuffle 用时5min, 31sec；
Slave1完成了6个map任务；
Slave2完成了6个map任务，1个reduce任务；
map过程输入记录30287756个，输出记录71148111个，输出数据2079861933B；
reduce过程输入记录71148111个，输出记录10573813个；
输出数据集大小：，151.14M，共3个块。
第5步：依据第4步的输出数据，根据每个userID对应的所有itemID可能的预测评分，排序并推荐前300个预测评分最高的物品。map阶段将第4步reduce阶段的key、value原封不动地传给reduce阶段。reduce阶段则以所有的userID为key，聚合了所有它未作出过评分的itemID及其对该itemID计算得到的可能的预测评分，依据评分高低排序，选择前300个预测评分最高的itemID及其预测评分输出，得到推荐结果。
输入数据集大小：151.14M，共3个块；
MapReduce过程总用时： 33sec；
map任务共个，每个map任务平均用时10sec；
reduce任务共1个，reduce任务用时8sec；
Shuffle用时8sec；
Slave1完成了1个map任务；
Slave2完成了2个map任务，1个reduce任务；
map过程输入记录10573813个，输出记录10573813个，输出数据166304289B；
reduce过程输入记录10573813个，输出记录116个；
输出数据集大小：455.77K，共1个块。
