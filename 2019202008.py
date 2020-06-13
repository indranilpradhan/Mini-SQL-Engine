import csv
import sys
import re
import sqlparse
import os
import numbers

metadata = {}

def search(List, n):  
    for i in range(len(List)): 
        if List[i] == n: 
            return i
    return -1

def Average(lst): 
    return sum(lst) / len(lst)

def unique(list1):  
    ulist = [] 
    for ele in list1: 
        if ele not in ulist: 
            ulist.append(ele)  
    for ele in ulist: 
        print(ele)

def removeDuplicates(listofElements):
    uniqueList = []
    for elem in listofElements:
        if elem not in uniqueList:
            uniqueList.append(elem)       
    return uniqueList

def findindexes(columns,list1):
    indexs = []
    for i in columns:
        flag = 0
        for j in range(len(list1)):
            if(i == list1[j]):
                flag = 1
                indexs.append(j)
        if(flag == 0):
            indexs.append(-1)
    return indexs

def evaluate(table,strng):
    print(strng)
    if(len(strng.split(" ")[0].strip().split(".")) == 2):
        col = strng.split(" ")[0].strip().split(".")[1].strip()
    else:
        col = strng.split(" ")[0].strip()
    operator = strng.split(" ")[1].strip()
#    print("operator ",operator)
    val = strng.split(" ")[2].strip()
#    print("val ",val)
    if col not in metadata[table]:
        sys.exit("Column not found")
    columns = []
    columns.append(col)
    indexes = []
#    print("colimns ",columns)
    indexes = findindexes(columns,metadata[table.strip()])
#    print("index ",indexes)
    tablepath = "/home/indranil/Documents/files/"+table+".csv"
    freader = open(tablepath,"r")
    data =[]
    if(operator == "<"):
        for row in freader:
    #        print("rowind ",row.split(",")[indexes[0]].strip())
            if(int(row.split(",")[indexes[0]].strip()) < int(val.strip())):
                data.append(row.strip().split(","))
    elif(operator == "<="):
        for row in freader:
            if(int(row.split(",")[indexes[0]].strip()) <= int(val.strip())):
                data.append(row.strip().split(","))
    elif(operator == ">"):
        for row in freader:
            if(int(row.split(",")[indexes[0]].strip()) > int(val.strip())):
                data.append(row.strip().split(","))
    elif(operator == ">="):
        for row in freader:
            if(int(row.split(",")[indexes[0]].strip()) >= int(val.strip())):
                data.append(row.strip().split(","))
    elif(operator == "="):
        for row in freader:
   #         print("rowind ",row[indexes[0]])
            if(row.split(",")[indexes[0]].strip() == val.strip()):
                print("val2 ",row)
                data.append(row.strip().split(","))
    freader.close()
    #print("data ",data)
    return data

def evaluatejoin(jointablecol):
    table1 = list(jointablecol.keys())[0].strip()
    table2 = list(jointablecol.keys())[1].strip()
    tablepath1 = "/home/indranil/Documents/files/"+table1+".csv"
    freader1 = open(tablepath1,"r")
    tablepath2 = "/home/indranil/Documents/files/"+table2+".csv"
   # freader2 = open(tablepath2,"r")
    col1 = []
    col2 = []
    col1.append(jointablecol[table1])
    col2.append(jointablecol[table2])
    indexes1 = []
    indexes2 = []
    indexes1 = findindexes(col1,metadata[table1])
    indexes2 = findindexes(col2,metadata[table2])
    data = []
    header = []
    for i in metadata[table1]:
        header.append(table1+"."+i)
    for i in metadata[table2]:
        header.append(table2+"."+i)
    data.append(header)
#    print("header ",header)
    for row1 in freader1:
        freader3 = open(tablepath2,"r")
        for row2 in freader3:
        #    print("row2 ",(row2))
            if(int(row1.strip().split(",")[indexes1[0]]) == int(row2.strip().split(",")[indexes2[0]])):
                temp = []
                temp.extend(row1.strip().split(","))
                temp.extend(row2.strip().split(","))
        #        print("temp ",temp)
                data.append(temp)
        freader3.close()
    #print("data ",data)
    return data

def evaluatecrossjoin(jointablecol):
    table1 = jointablecol[0].strip()
    table2 = jointablecol[1].strip()
    tablepath1 = "/home/indranil/Documents/files/"+table1+".csv"
    freader1 = open(tablepath1,"r")
    tablepath2 = "/home/indranil/Documents/files/"+table2+".csv"
   # freader2 = open(tablepath2,"r")
    data = []
    header = []
    for i in metadata[table1]:
        header.append(table1+"."+i)
    for i in metadata[table2]:
        header.append(table2+"."+i)
    data.append(header)
#    print("header ",header)
    for row1 in freader1:
        freader3 = open(tablepath2,"r")
        for row2 in freader3:
        #    print("row2 ",(row2))
            temp = []
            temp.extend(row1.strip().split(","))
            temp.extend(row2.strip().split(","))
        #        print("temp ",temp)
            data.append(temp)
        freader3.close()
    #print("data ",data)
    return data
    

def evaluateand(lst1,lst2):
    tup1 = map(tuple, lst1) 
    tup2 = map(tuple, lst2)  
    return list(map(list, set(tup1).intersection(tup2)))

def evaluateor(lst1,lst2):
    tup1 = map(tuple, lst1) 
    tup2 = map(tuple, lst2)  
    return list(map(list, set(tup1).union(tup2)))

def main():
    try:
        fileo = open('/home/indranil/Documents/files/metadata.txt','r')
        istable = 0
        table = ""
        iscolumn =0
        for line in fileo:        
            if line.strip() == "<begin_table>":
                istable = 1
                continue
            if istable == 1:
                table = line.strip() 
                metadata[table] = []
                iscolumn =1
                istable = 0
                continue
            if iscolumn == 1 and line.strip() != "<end_table>":
                metadata[table].append(line.strip())
                continue
            if line.strip() == "<end_table>":
                iscolumn =0
                table =""
        query = (sys.argv[1]).strip()
        parsed = sqlparse.parse(query)[0].tokens    
    #    print(parsed)
        querytype = sqlparse.sql.Statement(parsed).get_type()
    #    print(querytype)
    #    print(metadata)
        identifiers = []
        l = sqlparse.sql.IdentifierList(parsed).get_identifiers()
        for i in l:
        #    print(str(i))
            identifiers.append(str(i))
        if(querytype != "SELECT"):
            return
        if(len(identifiers) == 4):
            print(identifiers[3])
            if(len(identifiers[3].strip().split(",")) == 2):
                tablelist = identifiers[3].strip().split(",")
                for i in tablelist:
                    if i.strip() not in metadata.keys():
                        print("Table not found")
                        return
                result = []
                result = evaluatecrossjoin(tablelist)
                if(identifiers[1].strip() == "*"):
                #    print("result ",result)                       
                    for i in result:
                        for j in i:
                            print(j+" ",end='')
                        print("\n") 
                else:
                    for i in identifiers[1].strip().split(","):
                        j =[]
                        j = i.split(".")
                    #    print("tab ",j[0])
                    #    print("col ",j[1])
                        if j[1] not in metadata[j[0]]:
                            print("column not found")
                            return
                    delindexes = []
                #    print("here")
                    for j in range(len(result[0])):
                        flag = 0
                    #    print("jind", j)
                    #    print("j", result[0][j])
                        for i in identifiers[1].strip().split(","):
                    #        print("i", i)
                            if(i.strip() == result[0][j]):
                                flag =1
                                break
                        if(flag == 1):
                            delindexes.append(j)
                #    print("delindexe ",delindexes)
                    for i in result:
                        for j in delindexes:
                            print(i[j]+" ",end='')
                        print("\n")
            
            else:
                if identifiers[3].strip() not in metadata.keys():
                    print("Table not found")
                    return
                tablepath = "/home/indranil/Documents/files/"+identifiers[3].strip()+".csv"
                aggcols = re.sub(u"[\(\)]",' ',identifiers[1]).split()
#               print(aggcols)
                if(os.path.exists(tablepath) == False):
                    return
                f = open(tablepath,'r')
                freader = csv.reader(f)
#               print(freader)
                if(aggcols[0]=="*"):
                    for i in metadata[identifiers[3]]:
                        print(i+" ",end='')
                    print("\n")
                    for row in freader:
                        for data in row:
                            print(data+" ",end='')
                        print("\n")
            
                elif(aggcols[0] == "max" or aggcols[0] == "min" or aggcols[0] == "sum" or aggcols[0] == "avg" or aggcols[0] == "distinct"):
                    if(search(metadata[identifiers[3].strip()],aggcols[1]) == -1):
                        print("Column not not found")
                        return
                    print(identifiers[1])
                    colnum = search(metadata[identifiers[3].strip()],aggcols[1])
                    data=[]
                    for row in freader:
                        data.append(int(row[colnum]))
                    if(aggcols[0] == "max"):
                        print(max(data))
                    elif(aggcols[0] == "min"):
                        print(min(data))
                    elif(aggcols[0] == "sum"):
                        print(sum(data))
                    elif(aggcols[0] == "avg"):
                        print(Average(data))
                    elif(aggcols[0] == "distinct"):
                        unique(data)
            
                else:
                    if(len(identifiers[3].split(",")) == 1):
                        columns = identifiers[1].split(",")
                        indexes = findindexes(columns,metadata[identifiers[3].strip()])
                        if(len(indexes) == 0):
                            print("No columns found")
                            return
                        elif -1 in indexes:
                            print("Provide proper column name")
                            return
                        for i in columns:
                            print(i+" ",end='')
                        print("\n")
                        for row in freader:
                            for i in indexes:
                                print(row[i]+' ',end='')
                            print('\n')
        
        if(len(identifiers) > 4):
        #    print(identifiers)
            if(len(identifiers[3].split(",")) == 1):
                res = []
            #    print("without join")
                if(identifiers[3].strip()) not in metadata.keys():
                    print("Table not found")
                    return
                wherestr = identifiers[4].split("where")[1].strip()
            #    print(wherestr)
                if(len(wherestr.split("AND")) == 2):
                    fpart = wherestr.split("AND")[0].strip()
                    spart = wherestr.split("AND")[1].strip()
                    flist = evaluate(identifiers[3].strip(), fpart)
                    slist = evaluate(identifiers[3].strip(), spart)
 #                   print("flist ",flist)
 #                   print("slist ",slist)
                    res = evaluateand(flist,slist)
                elif(len(wherestr.split("OR")) == 2):
                    fpart = wherestr.split("OR")[0].strip()
                    spart = wherestr.split("OR")[1].strip()
                    flist = evaluate(identifiers[3].strip(), fpart)
                    slist = evaluate(identifiers[3].strip(), spart)
                    res = evaluateor(flist,slist)
                else:
            #        print("wherestr", wherestr)
                    res = evaluate(identifiers[3].strip(), wherestr.strip())
#                print("res ",res)
                if(identifiers[1].strip() == "*"):
                    for i in metadata[identifiers[3]]:
                        print(i+" ",end='')
                    print("\n")
                    for row in res:
                        for data in row:
                            print(data+" ",end='')
                        print("\n")
                else:            
                    for i in identifiers[1].split(","):
                        if i not in metadata[identifiers[3].strip()]:
                            print("Column not found")
                            return
                    columns = identifiers[1].split(",")
                    indexes = []
                    indexes = findindexes(columns,metadata[identifiers[3].strip()])
#                    print("index ",indexes)
                    if(len(indexes) == 0):
                        print("No columns found")
                        return
                    elif -1 in indexes:
                        print("Provide proper column name")
                        return
                    for i in columns:
                        print(i+" ",end='')
                    print("\n")
                    for row in res:
                        for i in indexes:
                            print(row[i]+' ',end='')
                        print('\n')
                        
            if(len(identifiers[3].split(",")) == 2):
            #    print("here",len(identifiers[4].strip().split("where")[1].strip().split(" ")[0].strip().split(".")))
                if(len(identifiers[4].strip().split("where")[1].strip().split(" ")[0].strip().split(".")) == 2):
                    print("within join")
                    tables = []
                    jointablecol = {}
                    tables = identifiers[3].split(",")
                    for i in tables:
                        if i not in metadata.keys(): 
                            print("Table not found")
                            return    
                    wherestr = identifiers[4].split("where")[1].strip()
            #        print("wherestr ",wherestr)
                    andres = []
                    joinstr = ""
                    if(len(wherestr.split("AND")) == 2):
                        joinstr = wherestr.split("AND")[0].strip()
                        andstr = wherestr.split("AND")[1].strip()
                        table = andstr.split(" ")[0].strip().split(".")[0].strip()
                        if table not in metadata.keys():
                            print("Table not found")
                            return
                        colu = []
                        colu.append(andstr.split(" ")[0].strip().split(".")[1].strip())
                        if colu[0] not in metadata[table]:
                            print("Column not found")
                            return
                #    print("colu ",colu)
                        indexescol = []
                #    print(metadata[table])
                        indexescol = findindexes(colu, metadata[table])
                #    print("indexcol ",indexescol)
                        tempandres = []
                        tempandres = evaluate(table, andstr)
                        for i in tempandres:
                            andres.append(i[indexescol[0]])
                        tablecol = andstr.split(" ")[0].strip()
                        andres.insert(0,tablecol)
                
                    if joinstr == "":
                        joinstr = wherestr
                
                    for i in joinstr.split("="):
                        jointablecol[i.split(".")[0].strip()] = i.split(".")[1].strip()
                
                    data = []
                    data = evaluatejoin(jointablecol)
                    result = []
            #        print("andres ",andres)
                    if(len(andres) != 0):
                        index = -1
                        for i in range(len(data[0])):
                            if(data[0][i].strip() == andres[0].strip()):
                                index = i
                                break
            #            print("index ",index)
                        for i in data:
                            for j in andres:
                        #    print("i ",i)
                        #    print("j ",j)
                                if(i[index].strip() == j.strip()):
                                    result.append(i)
            #            print("res ",result)
                    else:
                        result = data
                    
                    if(identifiers[1].strip() == "*"):
                        if joinstr.split("=")[0].strip().split(".")[1].strip() != joinstr.split("=")[1].strip().split(".")[1].strip():
                            for i in result:
                                for j in i:
                                    print(j+" ",end='')
                                print("\n")
                        else:
                            todelete = joinstr.split("=")[0].strip()
                            index = -1
                #    print("result ",result)
                            for i in range(len(result[0])):
                                if(result[0][i].strip() == todelete.strip()):
                                    index = i
                                    break
                    
                            for i in result:
                                del i[index]
                        
                            for i in result:
                                for j in i:
                                    print(j+" ",end='')
                                print("\n") 
                    else:
                        for i in identifiers[1].strip().split(","):
                            j =[]
                            j = i.split(".")
                    #    viewtablecol[j[0]].append(j[1])
                    #    print("tab ",j[0])
                    #    print("col ",j[1])
                            if j[0] not in metadata.keys():
                                print("Table not found")
                                return
                            if j[1] not in metadata[j[0]]:
                                print("column not found")
                                return
                        delindexes = []
                #    print("here")
                        for j in range(len(result[0])):
                            flag = 0
                    #    print("jind", j)
                    #    print("j", result[0][j])
                            for i in identifiers[1].strip().split(","):
                    #        print("i", i)
                                if(i.strip() == result[0][j]):
                                    flag =1
                                    break
                            if(flag == 1):
                                delindexes.append(j)
                #        print("delindexe ",delindexes)
                        for i in result:
                            for j in delindexes:
                                print(i[j]+" ",end='')
                            print("\n")

                elif(len(identifiers[4].strip().split("where")[1].split(" ")[0].strip().split(".")) == 1):
            #        print("new part")
                    tables = []
                    jointablecol = {}
                    tables = identifiers[3].split(",")
                    for i in tables:
                        if i not in metadata.keys(): 
                            print("Table not found")
                            return
                    wherestr = identifiers[4].split("where")[1].strip()
            #        print("wherestr ",wherestr)
                    tables = identifiers[3].strip().split(",")
                    crossjoin = []
                    crossjoin = evaluatecrossjoin(tables)
                    #print("crossjoin ",crossjoin)
                    result = []
                    tempres = []
                    if(len(wherestr.split("AND")) == 2):
                        fpart = wherestr.split("AND")[0].strip()
                        spart = wherestr.split("AND")[1].strip()
                        colu = []
                        colu.append(fpart.split(" ")[0].strip())
                        colu.append(spart.split(" ")[0].strip())
                        for i in colu:
                            count = 0
                            for j in tables:
                                if i.strip() in metadata[j]:
                                    count = count+1
                            if count > 1:
                                print("Ambiguous Column")
                                return
                            elif count == 0:
                                print("Column not found")
                                return
                        fpartandres = []
                        if colu[0] in metadata[tables[0].strip()]:
                            tempandres = []
                            tempandres = evaluate(tables[0].strip(), fpart)
                            colu1 = []
                            colu1 = colu[0].strip()
                            indexescol = []
                            indexescol = findindexes(colu1,metadata[tables[0].strip()])
                            for i in tempandres:
                                fpartandres.append(i[indexescol[0]])
                            temp = tables[0].strip()+"."+colu[0]
                            fpartandres.insert(0,temp)
                        else:
                            tempandres = []
                            tempandres = evaluate(tables[1].strip(), fpart)
                            colu1 = []
                            colu1 = colu[0].strip()
                            indexescol = []
                            indexescol = findindexes(colu1,metadata[tables[1].strip()])
                            for i in tempandres:
                                fpartandres.append(i[indexescol[0]])
                            temp = tables[1].strip()+"."+colu[0]
                            fpartandres.insert(0,temp)
                        
                        spartandres = []
                        if colu[1] in metadata[tables[0].strip()]:
                            tempandres = []
                            tempandres = evaluate(tables[0].strip(), spart)
                            colu1 = []
                            colu1 = colu[1].strip()
                            indexescol = []
                            indexescol = findindexes(colu1,metadata[tables[0].strip()])
                            for i in tempandres:
                                spartandres.append(i[indexescol[0]])
                            temp = tables[0].strip()+"."+colu[1]
                            spartandres.insert(0,temp)
                        else:
                            tempandres = []
                            tempandres = evaluate(tables[1].strip(), spart)
                            colu1 = []
                            colu1 = colu[1].strip()
                            indexescol = []
                            indexescol = findindexes(colu1,metadata[tables[1].strip()])
                            for i in tempandres:
                                spartandres.append(i[indexescol[0]])
                            temp = tables[1].strip()+"."+colu[1]
                            spartandres.insert(0,temp)
                        
                        if(len(fpartandres) > 0 and len(spartandres) > 0):
                            index1 = -1
                            for i in range(len(crossjoin[0])):
                                if(crossjoin[0][i].strip() == fpartandres[0].strip()):
                                    index1 = i
                                    break
                #            print("index ",index1)
                            for i in crossjoin:
                                for j in fpartandres:
                        #    print("i ",i)
                        #    print("j ",j)
                                    if(i[index1].strip() == j.strip()):
                                        tempres.append(i)
                            index2 = -1
                            for i in range(len(crossjoin[0])):
                                if(crossjoin[0][i].strip() == spartandres[0].strip()):
                                    index2 = i
                                    break
                #            print("index ",index2)
                            for i in tempres:
                                for j in spartandres:
                        #    print("i ",i)
                        #    print("j ",j)
                                    if(i[index2].strip() == j.strip()):
                                        result.append(i)
                        elif(len(fpartandres) > 0):
                            index1 = -1
                            for i in range(len(crossjoin[0])):
                                if(crossjoin[0][i].strip() == fpartandres[0].strip()):
                                    index1 = i
                                    break
                #            print("index ",index1)
                            for i in crossjoin:
                                for j in fpartandres:
                        #    print("i ",i)
                        #    print("j ",j)
                                    if(i[index1].strip() == j.strip()):
                                        result.append(i)
                        elif(len(spartandres) > 0):
                            index1 = -1
                            for i in range(len(crossjoin[0])):
                                if(crossjoin[0][i].strip() == spartandres[0].strip()):
                                    index1 = i
                                    break
                #            print("index ",index1)
                            for i in crossjoin:
                                for j in spartandres:
                        #    print("i ",i)
                        #    print("j ",j)
                                    if(i[index1].strip() == j.strip()):
                                        result.append(i)
                                        
                    elif(len(wherestr.split("OR")) == 2):
                        fpart = wherestr.split("OR")[0].strip()
                        spart = wherestr.split("OR")[1].strip()
                        colu = []
                        colu.append(fpart.split(" ")[0].strip())
                        colu.append(spart.split(" ")[0].strip())
                        for i in colu:
                            count = 0
                            for j in tables:
                                if i.strip() in metadata[j]:
                                    count = count+1
                            if count > 1:
                                print("Ambiguous Column")
                                return
                            elif count == 0:
                                print("Column not found")
                                return
                        fpartandres = []
                        if colu[0] in metadata[tables[0].strip()]:
                            tempandres = []
                            tempandres = evaluate(tables[0].strip(), fpart)
                            colu1 = []
                            colu1 = colu[0].strip()
                            indexescol = []
                            indexescol = findindexes(colu1,metadata[tables[0].strip()])
                            for i in tempandres:
                                fpartandres.append(i[indexescol[0]])
                            temp = tables[0].strip()+"."+colu[0]
                            fpartandres.insert(0,temp)
                        else:
                            tempandres = []
                            tempandres = evaluate(tables[1].strip(), fpart)
                            colu1 = []
                            colu1 = colu[0].strip()
                            indexescol = []
                            indexescol = findindexes(colu1,metadata[tables[1].strip()])
                            for i in tempandres:
                                fpartandres.append(i[indexescol[0]])
                            temp = tables[1].strip()+"."+colu[0]
                            fpartandres.insert(0,temp)
                        
                        spartandres = []
                        if colu[1] in metadata[tables[0].strip()]:
                            tempandres = []
                            tempandres = evaluate(tables[0].strip(), spart)
                            colu1 = []
                            colu1 = colu[1].strip()
                            indexescol = []
                            indexescol = findindexes(colu1,metadata[tables[0].strip()])
                            for i in tempandres:
                                spartandres.append(i[indexescol[0]])
                            temp = tables[0].strip()+"."+colu[1]
                            spartandres.insert(0,temp)
                        else:
                            tempandres = []
                            tempandres = evaluate(tables[1].strip(), spart)
                            colu1 = []
                            colu1 = colu[1].strip()
                            indexescol = []
                            indexescol = findindexes(colu1,metadata[tables[1].strip()])
                            for i in tempandres:
                                spartandres.append(i[indexescol[0]])
                            temp = tables[1].strip()+"."+colu[1]
                            spartandres.insert(0,temp)
                        temp2res = []
            #            print("fpart",fpartandres)
            #            print("spart",spartandres)
                        if(len(fpartandres) > 0 and len(spartandres) > 0):
                            index1 = -1
                            for i in range(len(crossjoin[0])):
                                if(crossjoin[0][i].strip() == fpartandres[0].strip()):
                                    index1 = i
                                    break
            #                print("index ",index1)
                            for i in crossjoin:
                                for j in fpartandres:
                        #    print("i ",i)
                        #    print("j ",j)
                                    if(i[index1].strip() == j.strip()):
                                        result.append(i)
                            index2 = -1
                            for i in range(len(crossjoin[0])):
                                if(crossjoin[0][i].strip() == spartandres[0].strip()):
                                    index2 = i
                                    break
            #                print("index ",index2)
                            for i in crossjoin:
                                for j in spartandres:
                        #    print("i ",i)
                        #    print("j ",j)
                                    if(i[index2].strip() == j.strip()):
                                        result.append(i)
                #            result = [list(x) for x in set(tuple(x) for x in temp2res)]
                        elif(len(fpartandres) > 0):
                            index1 = -1
                            for i in range(len(crossjoin[0])):
                                if(crossjoin[0][i].strip() == fpartandres[0].strip()):
                                    index1 = i
                                    break
                #            print("index ",index1)
                            for i in crossjoin:
                                for j in fpartandres:
                        #    print("i ",i)
                        #    print("j ",j)
                                    if(i[index1].strip() == j.strip()):
                                        result.append(i)
                        elif(len(spartandres) > 0):
                            index1 = -1
                            for i in range(len(crossjoin[0])):
                                if(crossjoin[0][i].strip() == spartandres[0].strip()):
                                    index1 = i
                                    break
                #            print("index ",index1)
                            for i in crossjoin:
                                for j in spartandres:
                        #    print("i ",i)
                        #    print("j ",j)
                                    if(i[index1].strip() == j.strip()):
                                        result.append(i)
                #        print("result or",result)
                    else:
                #        print("not and or")
                        fpart = wherestr.strip()
                        colu = []
                        colu.append(fpart.split(" ")[0].strip())
                        for i in colu:
                            count = 0
                            for j in tables:
                                if i.strip() in metadata[j]:
                                    count = count+1
                            if count > 1:
                                print("Ambiguous Column")
                                return
                            elif count == 0:
                                print("Column not found")
                                return
                        fpartandres = []
                        if colu[0] in metadata[tables[0].strip()]:
                            tempandres = []
                            tempandres = evaluate(tables[0].strip(), fpart)
                            colu1 = []
                            colu1 = colu[0].strip()
                            indexescol = []
                            indexescol = findindexes(colu1,metadata[tables[0].strip()])
                            for i in tempandres:
                                fpartandres.append(i[indexescol[0]])
                            temp = tables[0].strip()+"."+colu[0]
                            fpartandres.insert(0,temp)
                        else:
                            tempandres = []
                            tempandres = evaluate(tables[1].strip(), fpart)
                            colu1 = []
                            colu1 = colu[0].strip()
                            indexescol = []
                            indexescol = findindexes(colu1,metadata[tables[1].strip()])
                            for i in tempandres:
                                fpartandres.append(i[indexescol[0]])
                            fpartandres = evaluate(tables[1].strip(), fpart)
                            temp = tables[1].strip()+"."+colu[0]
                            fpartandres.insert(0,temp)
                #        print("fpartandres ",fpartandres)
                        if(len(fpartandres) > 0):
                            index1 = -1
                            for i in range(len(crossjoin[0])):
                                if(crossjoin[0][i].strip() == fpartandres[0].strip()):
                                    index1 = i
                                    break
                #            print("index ",index1)
                            for i in crossjoin:
                                for j in fpartandres:
                        #    print("i ",i)
                        #    print("j ",j)
                                    if(i[index1].strip() == j.strip()):
                                        result.append(i)
                        #print("result ",result)
                    if(identifiers[1].strip() == "*"):
                        for i in result:
                            for j in i:
                                print(j+" ",end='')
                            print("\n") 
                    else:
                        printindexes = []
                #        print("in printing")
                        for i in identifiers[1].strip().split(","):
                            if i .strip() not in metadata[tables[0].strip()] and i.strip() not in metadata[tables[1].strip()]:
                                print("column not found")
                                return
                            elif i.strip() in metadata[tables[0].strip()] and i.strip() in metadata[tables[1].strip()]:
                                tab1col = tables[0].strip()+"."+i.strip()
                                tab2col = tables[1].strip()+"."+i.strip()
                                if tab1col in printindexes or tab2col in printindexes:
                                    pass
                                else:
                                    printindexes.append(tab1col)
                            elif i.strip() in metadata[tables[0].strip()] and i.strip() not in metadata[tables[1].strip()]:
                                tab1col = tables[0].strip()+"."+i.strip()
                                printindexes.append(tab1col)
                            elif i.strip() not in metadata[tables[0].strip()] and i.strip() in metadata[tables[1].strip()]:
                                tab1col = tables[1].strip()+"."+i.strip()
                                printindexes.append(tab1col)
                #        print("print index", printindexes)
                        delindexes = []
                        for j in range(len(result[0])):
                            flag = 0
                            for i in printindexes:
                            #    print("i", i)
                                if(i.strip() == result[0][j].strip()):
                                    flag =1
                                    break
                            if(flag == 1):
                                delindexes.append(j)
                #        print("delindexe ",delindexes)
                        dupres =[]
                        for i in result:
                            tempdup = []
                            for j in delindexes:
                                tempdup.append(i[j])
                            dupres.append(tempdup)
                        dupresult = []
                        dupresult = removeDuplicates(dupres)
                        for i in dupresult:
                            for j in i:
                                print(j+" ",end='')
                            print("\n") 
                    
    except:
        print("Unable to process")

if __name__ == "__main__":
	main()
