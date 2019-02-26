# SON_Implementation

SON algorithm	implemented using	the	Apache	Spark	Framework.
The goal  is to find	frequent	itemsets in	two	datasets,	one	simulated	dataset(small1.csv,small2.csv)and	one	real-world	
dataset	generated	 from	Yelp dataset and to apply the algorithm	on large	datasets more	efficiently in	a	distributed	environment.

##### Task1: 
command to run code :
spark-submit	firstname_lastname_task1.py		<case	number>	<support>	<input_file_path>	<output_file_path>

case 1:
calculating	 the	 combinations	 of	 frequent	 businesses	 (as	 singletons,	 pairs,	 triples,	 etc.)	that	 are
qualified	as	frequent	given	a	support	threshold.	Creating	a	basket	for	each	user	containing	the	
business	ids	reviewed	by	this	user.	If	a	business	was	reviewed	more	than	once by	a	reviewer,	we	consider	
this	product	was	rated	only	once.	More	specifically,	the	business	ids	within	each	basket	are	unique.	The	
generated	baskets	are	similar	to:
  
 ```
  user1:	[business11, business12, business13, ...]
  user2:	[business21, business22, business23, ...]
  user3:	[business31, business32, business33, ...]
 ```
 
case2:
calculating	the	combinations	of	frequent	users	(as	singletons,	pairs,	triples,	etc.)	that	are	qualified	
as	frequent	given	a	support	threshold. Creating a	basket	for	each	business	containing the	user
ids that	commented	on	this	business.	Similar	to	case	1,	the	user	ids	within	each	basket	are	unique.	The	
generated	baskets	are	similar	to:
```
business1:	[user11, user12, user13, ...]
business2:	[user21, user22, user23, ...]
business3:	[user31, user32, user33, ...]
```
##### Task2:
Exploring	the	Yelp	dataset	to	find	the	frequent	business	sets	(only	case	1).	Jointly	
using	the	business.json	and	review.json	to	generate	the	input	user-business	CSV	file.

command to run code:
spark-submit	firstname_lastname_task2.py		<filter	threshold>	<support>	<input_file_path>	<output_file_path>
threshold- To find out	qualified	users	who	reviewed	more	than	k businesses.	(k is	the	filter	threshold)
  
#### Input files 
In task 1, the algorithm is tested with a small	simulated	CSV	file (small1.csv, small2.csv)

In task2, the yelp dataset is used (https://www.yelp.com/dataset) from which I generated a file to store the user ids and business ids from the business.json and review.json file.

This is the code to convert the yelp data into userid,business_id format :
```
busi_rdd = sc.textFile("business.json").map(lambda x : json.loads(x))
revi_rdd=sc.textFile("review.json").map(lambda x:json.loads(x))
rdd1=busi_rdd.map(lambda x:(x['business_id'],x['state'])).filter(lambda x:x[1]=='NV')
rdd2=revi_rdd.map(lambda x:(x['business_id'],x['user_id']))
rdd_fi=rdd1.join(rdd2).map(lambda x:(x[1][1],x[0])).collect()
with open('yelp.csv', 'w')as f:
     f.write("user_id,business_id\n")
     for x in rdd_fi:
         f.write(x[0]+","+x[1]+"\n")
```
