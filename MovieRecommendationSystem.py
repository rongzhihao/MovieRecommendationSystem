from pyspark import SparkContext
import math
from operator import add
from pyspark.sql.functions import row_number
import sys
import collections

movie = collections.defaultdict(set)
pair = collections.defaultdict(set)
usernum = 671
movienum = 9066
def getminhash(iterator):
    iterator = list(iterator)
    a = {}
    for e in iterator:
        a[e[0]] = [float('inf') for i in range(20)]
    for i in range(20):
        for e in iterator:
            for ee in e[1]:
                x = (51*int(ee)+131*i) % 1000
                #x = 0
                a[e[0]][i] = min(a[e[0]][i],x)
    b = []
    for e in a:
        for i,f in enumerate(a[e]):
            b.append((i,e,f))
    yield b
def getminhash2(iterator):
    iterator = list(iterator)
    a = {}
    for e in iterator:
        a[e[0]] = [float('inf') for i in range(20)]
    for i in range(20):
        for e in iterator:
            for ee in e[1]:
                x = (37*int(ee)+41*i) % 123
                #x = 0
                a[e[0]][i] = min(a[e[0]][i],x)
    b = []
    for e in a:
        for i,f in enumerate(a[e]):
            b.append((i,e,f))
    yield b
def lsh(iterator):
    pair = collections.defaultdict(set)
    iterator = list(iterator)
    a = collections.defaultdict(dict)
    for (x,y) in iterator:
        a[y[0]][x] = y[1]
        if y[0] == candidateUser:
            #print y[1]
            #print a[y[0]]
   # print candidateUser
    for x in a:
       if x != candidateUser:
            flag = True
            for c in a[x]:
                if a[x][c] != a[candidateUser][c]:
                    flag = False
                    break
            if flag:
                pair[candidateUser].add(x)
    yield pair
def lsh2(iterator):
    pair = collections.defaultdict(set)
    iterator = list(iterator)
    a = collections.defaultdict(dict)
    for x,y in iterator:
        a[y[0]][x] = y[1]
    for x in a:
       if x != candidateMovie:
            flag = True
            for c in a[x]:
                if a[x][c] != a[candidateMovie][c]:
                    flag = False
                    break
            if flag:
                pair[candidateMovie].add(x)
    yield pair
def mypartition(x):
    return x / 5
def mypar(x):
    return x / 2
sc = SparkContext(appName="inf553")
lines = sc.textFile(sys.argv[1])
candidateUser = sys.argv[4]
lines = lines.map(lambda x:x.split('\t')).map(lambda x: (x[0], x[1], float(x[2])))
lines = lines.filter(lambda x: x[2]>=3).map(lambda x:(x[0],(x[1],)))
lines = lines.partitionBy(100)
minhash = lines.mapPartitions(getminhash).flatMap(lambda x:x)
minhash = minhash.map(lambda x:(x[0],(x[1],x[2])))
gminhash = minhash.partitionBy(10,mypar)
gminhash = gminhash.mapPartitions(lsh).collect()
lines2 = sc.textFile(sys.argv[1])
candidateMovie = sys.argv[5]
lines2 = lines2.map(lambda x:x.split('\t')).map(lambda x: (x[0], x[1], float(x[2])))
lines2 = lines2.filter(lambda x: x[2]>=3).map(lambda x:(x[1],(x[0],)))
lines2 = lines2.partitionBy(100)
minhash2 = lines2.mapPartitions(getminhash2).flatMap(lambda x:x)
minhash2 = minhash2.map(lambda x:(x[0],(x[1],x[2])))
gminhash2 = minhash2.partitionBy(4,mypartition)
gminhash2 = gminhash2.mapPartitions(lsh2).collect()
fi = open(sys.argv[1],'r')
line = fi.readline()
user = collections.defaultdict(set)
moviepair = collections.defaultdict(set)
#userid = {}
while line:
    x = line.split('\t')
    if len(x) < 2:
        line = fi.readline()
        continue
    # if x[-1][-1] == '\n':
    #     x[-1] = x[-1][:-1]
    userid = x[0]
    movieid = x[1]
    rating = float(x[2])
    if rating >= 3:
        movie[userid].add(movieid)
        user[movieid].add(userid)
    line = fi.readline()
pair = collections.defaultdict(set)
for e in gminhash:
    for f in e:
        for g in e[f]:
            pair[f].add(g)
for e in gminhash2:
    for f in e:
        for g in e[f]:
            moviepair[f].add(g)
fileout = open(sys.argv[2],'w')
fileout2 = open(sys.argv[3],'w')
similaruser = collections.defaultdict(list)
similarmovie = collections.defaultdict(list)
#print pair
link = {}
fopen = open('link.txt','r')
line = fopen.readline()
while line:
    x = line.split('\t')
    if len(x) < 3:
        line = fopen.readline()
        continue
    if x[2][-1] == '\n':
        x[2] = x[2][:-1]
    link[x[0]] = x[2]
    line = fopen.readline()
fopen = open("movie.txt", "r")
dict = {}
movie_set = set()
user_set = []
count = 0
for line in fopen:
    item = line.split("\t")
    if len(item) < 5:
        continue
    id = item[2]
    name = item[4]
    dict[id] = name
    count += 1
fopen.close()
#print movie[candidateUser]
fileout.write('Input movies:')
for e in movie[candidateUser]:
    fileout.write(link[e]+'\n')
fileout.write('\n')
fileout2.write('Input movies:')
fileout2.write(link[candidateMovie]+'\n')
for e in [candidateUser]:
    if e not in pair:
        print "You don't have enough movies with positive ratings."
        continue
    a = []
    for f in pair[e]:
        jac = len(movie[e].intersection(movie[f])) / float(len(movie[e].union(movie[f])))
        a.append([jac,f])
    a.sort(key = lambda x:(-x[0],int(x[1])))
    #print len(a)
    threemovie = collections.defaultdict(int)
    for i in range(min(len(a),25)):
        for f in movie[a[i][1]]:
            threemovie[f] += 1
    out = sorted(threemovie.keys(),key=lambda x:(-threemovie[x],int(x)))
    i = 0
    #print len(out)
    print 'Hello, user ' + e + ', based on the movies you review, we recommend:'
    while len(similaruser[candidateUser]) < 15 and i < len(out):
        if out[i] not in movie[e] and out[i] in link and link[out[i]] in dict:
            similaruser[e].append(out[i])
        i += 1
    #fileout.write('User:\n')
    for g in similaruser:
        #fileout.write('User'+g+'\n')
        for f in similaruser[g]:
            #fileout.write(f+'\n')
            print dict[link[f]]
            fileout.write(link[f] + ": " + dict[link[f]] + '\n')
print ""
for e in [candidateMovie]:
    if e not in moviepair:
        continue
    a = []
    for f in moviepair[e]:
        jac = len(user[e].intersection(user[f])) / float(len(user[e].union(user[f])))
        a.append([jac,f])
    a.sort(key = lambda x:(-x[0],int(x[1])))
    #print len(a)
    if e not in link:
        print 'You are watching movie '+e+'. Based on this movie, we recommend:'
    else:
        print 'You are watching movie '+dict[link[e]]+'. Based on this movie, we recommend:'
    for f in a:
        if f[1] not in link:
            continue
        if link[f[1]] not in dict:
            continue
        similarmovie[e].append(f[1])
        if len(similarmovie[e]) >= 15:
            break
    #fileout.write('Movie:\n')
    for g in similarmovie:
        #fileout.write('Movie'+g+'\n')
        for f in similarmovie[g]:
            #fileout2.write(f+'\n')
            print dict[link[f]]
            fileout2.write(link[f] + ": " + dict[link[f]] + '\n')
fileout.close()
