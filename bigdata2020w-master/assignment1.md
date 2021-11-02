1. For Pairs implementation, there are two mapreduce jobs. The first stage is used for counting the total number of lines and \ 
the number of lines where each word occurs. For each line the mapper emits(*,1) for line counting, and for each word occuring in \
the line the mapper emits(word,1) and won't emit twice for the same word. The reducer just add up the values for any key. The second \
stage is used for counting PMI. The mapper emits((x,y),1) for each co-ocurring pair of words in a line(only one for each pair). \
In reduce phase, the setup function first reads from the previous output of stage one and gets the \
total number of lines and creats a map storing the line-occurence of each word, where map(x)=# of occurence.\
In the reduce function, for each key (x,y) we first compute sum of all  values and get total number of co-ocurrence,\
call it sum, then compute PMIusing number of lines,map(x), map(y) and sum, finally we emit((x,y),(PMI,sum)). \

For Stripes implementation, there are also two mapreduce jobs. The first stage is exactly the same. The second \
stage is used for counting PMI. The mapper emits(x,Map) for each word x in a line, where Map maps other words \
 in the same line to (1,1), since we only count once for each co-ocurring pair of words in a line.
In reduce phase, the setup function is also the same.In the reduce function, for each key x we first compute sum \ 
 of all maps(that is, sum up all the pairs for the same key).Now Map(y)=(sum,sum) where sum=total number of co-ocurrence \
of pair(x,y). Then compute PMI using number of lines,map(x), map(y) and sum, and replace Map(y) with (PMI,sum). Finally \
we emit (x,Map).

2. Pair:28.397s Stripe:16.355s in student environment
3. Pair:31.548s Stripe:17.503s in student environment
4. 77198(counting each distinct pair once and (x,y) is considered different from (y,x))


5. highest:
(maine, anjou)  (3.6331422, 12.0)
(anjou, maine)  (3.6331422, 12.0)


   lowest:
(thy, you)      (-1.5303968, 11.0)
(you, thy)      (-1.5303967, 11.0)


6. tears:
(tears, shed)   (2.1117902, 15.0)
(tears, salt)   (2.052812, 11.0)
(tears, eyes)   (1.165167, 23.0)


   death:
(death, father's)       (1.120252, 21.0)
(death, die)    (0.7541594, 18.0)
(death, life)   (0.73813456, 31.0)
 
7.
(hockey, defenceman)    (2.4180872, 153.0)
(hockey, winger)        (2.3700917, 188.0)
(hockey, sledge)        (2.352185, 93.0)
(hockey, goaltender)    (2.2537384, 199.0)
(hockey, ice)   (2.2093477, 2160.0)


8.
(data, cooling) (2.0979042, 74.0)
(data, encryption)      (2.0443723, 53.0)
(data, array)   (1.9926307, 50.0)
(data, storage) (1.9878386, 110.0)
(data, database)        (1.8893089, 99.0)

