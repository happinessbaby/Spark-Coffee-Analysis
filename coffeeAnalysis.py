from collections import defaultdict

## returns bevarages sold on each branch
def readBranch(file):
    with open(file) as file:
        c = 0
        bev = defaultdict(list)
        for line in file:
            words = line.split(",")
            c +=1
            bev[c].append(words[0])
            if c == 5:
                c = 0
    file.close()
    return bev
# readBranch("BranchC.txt")


## beverageCounts returns number of days each beverage is sold
## for example: some beverages are sold daily, some are sold every other day
## days returns the beverages sold on a particular days
## lengthofDays returns the number of drinks sold on each day
def CountConsum(file):
    days = defaultdict(list)
    lengthofDays = defaultdict(int)
    beverageCounts = defaultdict(int)
    i=1
    with open(file) as file:
        for line in file:
            words = line.split(",")
            if words[0] in days[i]:
                i+=1
                continue
            beverageCounts[words[0]]+=1
            days[i].append(words[0])
    file.close()
    for day, drinks in days.items():
        lengthofDays[day]=len(drinks)
    return beverageCounts, lengthofDays

print(CountConsum("CountACut.txt"))


## validates the XYXY pattern in consumer counts
# def findCommon(fileA, fileB):
#     dictA = CountConsum(fileA)
#     dictB = CountConsum(fileB)
#     common28=[]
#     common33=[]
#     for k1, v1 in dictA.items():
#         for k2, v2 in dictB.items():
#             if len(v1)==len(v2) and len(v1)==28:
#                 common30.append(len(set(v1).intersection(set(v2))))
#             if len(v1)==len(v2) and len(v1)==33:
#                 common31.append(len(set(v1).intersection(set(v2))))
#     return common28, common33
#
#
# print(findCommon("CountB.txt", "CountC.txt"))


## finds replicas of entires in consumers counts
def findReplica(fileA, fileB, fileC):
    setA, setB, setC = set(), set(), set()
    with open(fileA) as file:
        for line in file:
            setA.add(line)
    with open(fileB) as file:
        for line in file:
            setB.add(line)
    with open(fileC) as file:
        for line in file:
            setC.add(line)
    print("\n\nintersection between A and B")
    replicaAB = setA.intersection(setB)
    print(replicaAB)
    print("\n\nintersection between A and C")
    replicaAC = setA.intersection(setC)
    print(replicaAC)
    print("\n\nintersection between B and C")
    replicaBC = setB.intersection(setC)
    print(replicaBC)
    return replicaAB, replicaAC, replicaBC

# findReplica("CountA.txt", "CountB.txt", "CountC.txt")
