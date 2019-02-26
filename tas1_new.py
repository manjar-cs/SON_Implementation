import time
import pyspark
import itertools
import sys
import math
st = time.time()
sc = pyspark.SparkContext()

def apriori(basket):
    chunk = list(basket)
    lisst = []
    count={}
    temp=[]

    local_support = math.ceil(support * (len(chunk) / float(num_baskets)))
    for i in chunk:
        for j in i:

            lisst.append(j)

    for x in lisst:

        if x in count.keys() and count[x]< local_support:
            count[x]+=1
        elif x not in count.keys():
            count[x]=1

    for k,v in count.items():
        if v>=local_support:
            temp.append(k)
            frequentitemset_p1.append((tuple(set([k])), 1))

    generate_pairs(chunk,temp,local_support,2)

    yield frequentitemset_p1

def generate_pairs(chunk,items,local_support,K):


    sets_candidates=set()
    count_ = {}
    if(K==2):

        pairs = itertools.combinations(sorted(items),2)
        pair_list = list(pairs)

        for j in chunk:
            for i in pair_list:
                if (set(i).issubset(j)):
                    if i in count_ and count_[i]<local_support:
                        count_[i] += 1
                    elif i not in count_:
                        count_[i] = 1

        for k, v in count_.items():
            if v >= local_support:
                sets_candidates.add(k)
        for c in sets_candidates:
            frequentitemset_p1.append((c,1))

    K+=1
    if (K > 2):
        while len(count_) != 0:
            list_item = []
            count_ = {}
            for i in sets_candidates:
                for j in sets_candidates:

                    x =  tuple(sorted(set(i+j)))
                    if (len(x) == K) and (x not in list_item):
                        combos=itertools.combinations(x,K-1)
                        combos=list(combos)
                        w=0
                        for h in combos:

                            if h in sets_candidates:
                                w+=1
                        if w==len(combos):
                            list_item.append(x)


            for i in list_item:
                for j in chunk:

                    #print("iii,jjj",i,j)
                    if (set(i).issubset(j)):
                        if i in count_ and count_[i] < local_support:
                            count_[i] += 1
                        elif i not in count_:
                            count_[i] = 1

            sets_candidates = []
            for k, v in count_.items():

                if v >= local_support:
                    sets_candidates.append(k)
                    frequentitemset_p1.append((k,1))
            K+=1

def phase2(candidates):

    frequentitemset_p2=[]

    freq_itemset_p2={}
    #c=tuple(candidates)
    #print("cc",candidates)
    key = candidates[0]

    for j in rdd_collect:
        if(isinstance(key,tuple)==False):
            if key in j:
                if key in freq_itemset_p2 and freq_itemset_p2[key]< support:
                    freq_itemset_p2[key] += 1
                elif key not in freq_itemset_p2:
                    freq_itemset_p2[key] = 1
        else:
            if(set(key).issubset(j)):
                if key in freq_itemset_p2 and freq_itemset_p2[key]< support:
                    freq_itemset_p2[key]+=1
                elif key not in freq_itemset_p2:
                    freq_itemset_p2[key]=1

    for k,v in freq_itemset_p2.items():
        frequentitemset_p2.append((k,v))
    return  frequentitemset_p2




if __name__== '__main__':


    rdd = sc.textFile(sys.argv[3])#"/Users/manjari/PycharmProjects/553_NEW/small1.csv")
    header = rdd.first()

    support=int(sys.argv[2])

    frequentitemset_p1 = []

    case=int(sys.argv[1])



    if case==1:
        # Forming the baskets
        rdd_2 = rdd.filter(lambda x:x!=header).map(lambda x:x.split(",")).map(lambda x:(x[0],x[1])).groupByKey().mapValues(set).map(lambda x:x[1]).cache()
        rdd_collect=rdd_2.collect()
        num_baskets=len(rdd_collect)
        rdd_a1 = rdd_2.mapPartitions(apriori).flatMap(lambda x:x).reduceByKey(lambda x,y:x+y).cache()
        rdd_final=rdd_a1.map(phase2).flatMap(lambda x: x).filter(lambda x: x[1] >= support).map(lambda x: x[0]).collect()
    elif case==2:
        rdd_2 = rdd.filter(lambda x: x != header).map(lambda x: x.split(",")).map(lambda x:(x[1],x[0])).groupByKey().mapValues(set).map(lambda x: x[1]).cache()
        rdd_collect = rdd_2.collect()
        num_baskets=len(rdd_collect)
        rdd_a1 = rdd_2.mapPartitions(apriori).flatMap(lambda x: x).reduceByKey(lambda x, y: x + y).cache()
        rdd_final = rdd_a1.map(phase2).flatMap(lambda x: x).filter(lambda x: x[1] >= support).map(lambda x: x[0]).collect()

    l1=sorted(rdd_final)
    #print(l1)
    l2=sorted(l1 , key=len)

    l1_cand=sorted(rdd_a1.map(lambda x: x[0]).collect())
    l2_cand=sorted(l1_cand , key=len)

    fname=sys.argv[4]+"manjari_chinniyansubramani_task1.txt"

    k=1
    result=""
    for cand in l2_cand:
        if len(cand)>k:
            result= result[:-1]+"\n\n"
        if (len(cand)==1):
            result=result+"('"+cand[0]+"'),"
        else:

            result=result+str(cand)+","
        k = len(cand)
    #print(result[:-1])
       # f.write("\n")
    with open(fname,'w') as f:
        f.write("Candidates:\n")
        f.write(result[:-1])
        f.write("\n\nFrequent Itemsets:\n")
        result=""
        for cand in l2:
            if len(cand) > k:
                result = result[:-1] + "\n\n"
            if (len(cand) == 1):
                result = result + "('" + cand[0] + "'),"
            else:

                result = result + str(cand) + ","
            k = len(cand)
        f.write(result[:-1]+"\n\n")


end=time.time()
print("Duration",end - st)
