
## 概述
该仓库为cs186  2021年课程的全部资料。用以支撑个人完成该课程内容的学习。

- RookieDB：项目的基本框架代码，直接修改在上面
- Resources：课件、任务资料等

## 资料收集

[cs186-资料合集](https://github.com/PKUFlyingPig/CS186)

## 进度

| 项目  | 完成情况 | 耗时   |
| ----- | -------- | ------ |
| proj0 | ok       | 0.5h   |
| proj1 | ok       | 6-6.5h |
| proj2 | ok         |  15-20h      |
| proj3 | ok         |  30h      |
| proj4 | ok         |  40h      |
| proj5 |          |        |
| proj6 |          |        |

## 杂记

（待施工，等我都做完了再来总结）

### proj0

做了很多前期工作吧，比如找各种github仓库啊，阅读课程目录和项目的注意事项啊啥的。开始的时候还是充满信心的好吧

### proj1

#### 前期准备
这个proj是需要额外的作业文件支持的，参考的repo中只有实现，没有最初版本的proj文件。哇，为了找最初版本的文件夹好放心的做，找了我足足一个晚上啊。没想到最后在[官方目录](https://github.com/berkeley-cs186/sp23-proj1)下面就有，又气又笑啊。

然后proj6也是类似的配置，好在找到proj1后信心大增，在github一个大佬的目录下找到了原始版本。至此，所有的准备工作都已经准备完全，只欠我自己这个东风啦。

#### 卡壳问题
- q4ii
不太明白是如何进行确定每个桶的范围，这个范围指的是差值区间是相等的。
我们可以通过计算出每个区间的范围，然后再查找对应的数据来做。

### proj2

#### 卡壳问题

Task2：
- sync：这个函数的作用主要是将内存中的更新写到磁盘上，所以有更新后都需要调用
- put：put过程中要注意children List更新的位置

Task3：
- 主要是是greaterthanEqual的设计上，我是设计了一种新的构造函数
- 另外要使用上 get 和 getleftmostNode ，不然过不了IO限制
- get不用使用sync，会额外增加 IO

Task4:
- 注意 bulkload 也要使用Sync（）！！！


### proj3

#### Part 1

- BNLJ
注意每次重新设置迭代器时，要把对应的细分的迭代器也要**重置**

- GHJ
刚开始看的时候感觉很复杂，不知道该怎么动手。然后又看了几遍Note，实际上的算法流程是很简单的。主要是理清楚 在run中进行递归的思路，其他就没什么问题。

#### Part2

- join
写起来稍微感觉有点模糊，但是这个课程的文档写的是真好啊，基本上该干什么都写清楚了，需要做的是熟悉代码框架和明白需要做的逻辑。

多表的join主要就是一个动态规划的过程，从 set（i - 1）中尝试添加单表，然后通过这个状态转移到 set（i）


### proj4

#### Part 1

- Task 1
	- substitutablility matrix : 这里所说的权限，不能从后续其他事务还能不能获取其他锁来理解。而是应该认为，某个事务以substitute lock 来请求，能干的事情会不会被影响，是针对单一事务来考虑的。


注意 Part 1 中虽然只让通过前几个测试用例，但是全部通过对于下一部分是很重要的，可以保证至少Lock 管理部分没有问题。

在这一个Part中，重要是 **修改资源上的锁时，要和事务上的锁记录一起修改**，这样才能保证两者记录是一致的。

#### Part 2

- Task 1
同样的，要注意修改 当前层级 的锁时，要级联修改子节点和父节点中的锁的记录信息

- Task 2
进行多粒度锁控制时，需要分析好锁的升级情况。
在进行测试时，发现一种隐式情况，即隐式升级为SIX锁的情况，需要我们在LockContext的promote过程进行一些判断，判断是否需要升级成SIX锁

### proj5

- Task 1

1、**prevLSN** 主要是用于undo阶段的，所以不是表面的先前LSN，而是当前事务的上一个操作，和lastLSN很像，但是lastLSN是在transaction Table中的，而这个是在log record中

2、appendToLog需要使用返回的值作为新的LSN，不能直接用record 的getLSN()

- Task 6
**Recory mode**
Redo 阶段
判断需不需要redo的，有一个条件 `pageLSN < currLSN`

因为pageLSN 记录的是对该页的最后一个操作，如果这个数字大于等于 currLSN，说明当前操作已经刷盘，所以无需热都


