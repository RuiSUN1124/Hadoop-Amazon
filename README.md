# Hadoop-Amazon
### D&amp;K LoT Lab1
* The map process for each order(each line in input file): 
```
(ItemBought,MapWritable<otherItem,one>)
```
* The reduce process: compute the sum of frequency that 2 items(one is the ItemBought) appear at the same time. We get:
```
(ItemBought,MapWritable<otherItem,sum>)
```
* The sort of otherItem,sum, I used the sortedMapWritable.
* The format for output is:
```
<ItemBought> [<The mostly recommended Item:frequency1> <The 2nd mostly recommended Item:frequency2>...]
```
For example:
