## 参考

[[n02-DisksFiles.pdf]]

## 文件记录方式

### Heap Files
- 无序
- 在写入上更有优势

#### Linked List

![[LinkedList.png]]

分成两个部分，如上所示
- Full pages， 这些页中没有剩余空间
- With free space， 还存有剩余空间

**如何遍历**
最坏情况下需要查找完整个链表才能判断是否可以加入新数据

#### Page Dictionary

![[Page Dictionary.png]]

每个page的位置和剩余空间都写在header page中

- 查找更快，查一个header相当于查了好几个page
- page不需要相邻，利用率更高

#### 两者的遍历区别

![[Diff_List_Dictionary.png]]


### Sorted Files

- 有序
- 读的效率更高，只要读 Log N 次
- 插入更慢，可能会引起后续所有文件的变动

![[Sorted Files.png]]


## Record Type

### 固定长度

![[FLR.png]]
- 因为固定长度，所以可以根据 offset 查找
- 使用**bitmap** 可以快速的确定可以存放的位置

### 可变长度

![[VLR-1.png]]
![[VLR2.png]]


- 先把固定长度的放到前面，可变的长度通过 **header** 里的offset确定
- page存储上用 footer来记录 pages上的存储信息
	- 槽的数量
	- 指向位置的指针
	- Entries（具体的记录信息），包括记录所在的指针和长度