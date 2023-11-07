
import json
import os
import requests
import math
import numpy as np
import asyncio

def initNamenode():
    res = requests.put(url + namenodeField + jsonSuffix, '""')
    # added directory and inumber sections, and block under inumber section
    res_inum = requests.put(url + namenodeField + '/' + inumSection + jsonSuffix, '""')
    res_dir = requests.put(url + namenodeField + '/' + dirSection + jsonSuffix, '""')
    if res and res_inum and res_dir:
      print('Create the namenode successfully!')
    inumber = 0
    # inodeSection
    data = {
        "type": 'DIRECTORY',
        "name": '/'
    }
    requests.put(url + namenodeField + '/' + inumSection + '/' + 'i' + str(inumber) + jsonSuffix, json.dumps(data))
    # dirSection
    requests.put(url + namenodeField + '/' + dirSection + '/' + 'i' + str(inumber) + jsonSuffix, '""')

def writeToNamenode(Type, path, location = '""', size = 0):
    resq = requests.get(url + namenodeField + '/' + inumSection + jsonSuffix).json()
    # print(resq)
    #generate inumber 
    if len(path) == 1: #root
        inumber = 0
    else: 
        inumberList = list(resq.keys())
        inumberList = [int(i[1:]) for i in inumberList]
        inumber = max(inumberList) + 1
    
    #generate name
    name = path.rsplit('/', 1)[1]
    name = '/' if not name else name
    parent = path.split('/')[-2] 
    parent = '/' if not parent else parent

    ##inodeSection
    if Type == 'FILE': 
        data = {
            "type": Type,
            "name": name, 
            "size": size,
            "replication": 2,
            "perferredBlockSize": perferredBlockSize
        }

    else:
        data = {
            "type": Type,
            "name": name
        }
    res = requests.put(url + namenodeField + '/' + inumSection + '/' + 'i' + str(inumber) + jsonSuffix, json.dumps(data))
    if Type == 'FILE':
        res_block = requests.put(url + namenodeField + '/' + inumSection + '/' + 'i' + str(inumber) + '/' + 'blocks' + jsonSuffix, json.dumps(location))
    # print(res_block)
    # print(res)
    # if res:
    #     print('Write metadata to namenode successfully!')
    
    ##dirSection
    if Type == 'DIRECTORY': 
        requests.put(url + namenodeField + '/' + dirSection + '/' + 'i' + str(inumber) + jsonSuffix, '""')
    #add value to parent
    # print(parent)
    for k, v in resq.items():
        if v['name'] == parent:
            # print(1)
            requests.patch(url + namenodeField + '/' + dirSection + '/' + k + jsonSuffix, json.dumps({'i' + str(inumber):  f'{name}'}))

def initDatanode():
    #init datanode 1
    res = requests.put(datanode_1 + jsonSuffix, '""')
    if res:
        print('Create the datanode 1 successfully!')
    #init datanode 2
    res = requests.put(datanode_2 + jsonSuffix, '""')
    if res:
        print('Create the datanode 2 successfully!')
    #init datanode 3
    res = requests.put(datanode_3 + jsonSuffix, '""')
    if res:
        print('Create the datanode 3 successfully!')
    #init datanode 4
    res = requests.put(datanode_4 + jsonSuffix, '""')
    if res:
        print('Create the datanode 4 successfully!')

def allocateBlocks():
    status = [] #record the number of blocks in each datanode
    status.append(len(requests.get(datanode_1 + jsonSuffix).json().keys()) if requests.get(datanode_1 + jsonSuffix).json() else 0)
    status.append(len(requests.get(datanode_2 + jsonSuffix).json().keys()) if requests.get(datanode_2 + jsonSuffix).json() else 0)
    status.append(len(requests.get(datanode_3 + jsonSuffix).json().keys()) if requests.get(datanode_3 + jsonSuffix).json() else 0)
    status.append(len(requests.get(datanode_4 + jsonSuffix).json().keys()) if requests.get(datanode_4 + jsonSuffix).json() else 0)
    # print(status)

    status = np.array(status)
    minNum = min(status) #find the minimum number of blocks in each datanode
    # print(minNum)
    minList = [] #record which datanode(s) can be relocated
    for i, n in enumerate(status):
        if n == minNum:
            minList.append(i)
    # print(minList)
    if len(minList) == 1: #there is only one datanode with minimum number of blocks
        return int(minList[0] + 1)
    else: #there are more than one datanode with the same minimum number of blocks
        return int(np.random.choice(minList) + 1)


def put(source, destination):
    name = source.rsplit('/', 1)[1]
    destination += name if destination == '/' else '/' + name
    with open(source, 'r') as f:
        file = f.read()
    size = len(file)
    location = {}
    #partition the data
    numOfBlocks = math.ceil(size / perferredBlockSize)
    i = 0
    while i < numOfBlocks:
        #allocate a datanode to store the data
        datanode = allocateBlocks()
        # print('i:', i)
        # print('datanode:', datanode)
        #generate block number 
        if i == 0:
            resq = requests.get(url + namenodeField + '/' + inumSection + jsonSuffix).json()
            #find maximum inumber for file
            inumberList = []
            for k, v in resq.items():
                if v['type'] == 'FILE': 
                    inumberList.append(int(k[1:]))
            if len(inumberList) == 0:
                blockNum = 0
            else:
                maxInumber = max(inumberList)
                # print(maxInumber)
                res = requests.get(url + namenodeField + '/' + inumSection + '/' + 'i' + str(maxInumber) + '/' + 'blocks' + jsonSuffix).json()    
                # print(res)
                blockNumList = list(res.keys())
                blockNumList = [int(i[1:]) for i in blockNumList]
                blockNum = max(blockNumList) + 1
        else:
            blockNum += 1
        # print('blockNum:', blockNum)
        
        blocks = [] #record block locations
        #store blocks of data to datanodes
        if i + 1 == numOfBlocks:
            requests.patch(datanodeMapping[datanode] + jsonSuffix, json.dumps({'b' + str(blockNum): str(file[i * perferredBlockSize : size])}))
            blocks.append(datanode)
            #replication
            datanode2 = allocateBlocks()
            requests.patch(datanodeMapping[datanode2] + jsonSuffix,json.dumps({'b' + str(blockNum): str(file[i * perferredBlockSize : size])}))
            blocks.append(datanode2)
        else:
            requests.patch(datanodeMapping[datanode] + jsonSuffix, json.dumps({'b' + str(blockNum): str(file[i * perferredBlockSize : (i + 1) * perferredBlockSize])}))
            blocks.append(datanode)
            #replication
            datanode2 = allocateBlocks()
            requests.patch(datanodeMapping[datanode2] + jsonSuffix, json.dumps({'b' + str(blockNum): str(file[i * perferredBlockSize : (i + 1) * perferredBlockSize])}))
            blocks.append(datanode2)
        # print('datanode2:', datanode2)
        location['b' + str(blockNum)] = blocks
        i += 1

    # print(location)
    # path = destination + '/' + source.split('/')[-1]
    writeToNamenode('FILE', destination, location, size)

# emulating mkdir, rmdir
def mkdir(path):
    writeToNamenode('DIRECTORY', path)


def ls(path):
    #find the inumber of the directory
    resq = requests.get(url + namenodeField + '/' + inumSection + jsonSuffix).json()
    name = path.split('/')[-1] if path.split('/')[-1] else '/'
    inumber = ''
    # print(name)
    for k, v in resq.items():
        if v['name'] == name:
            # print(1)
            inumber = k
            break
    dir = requests.get(url + namenodeField + '/' + dirSection + '/' + k + jsonSuffix).json()
    return list(dir.values()) if dir else []

def rm(path):
    ##find the inumber of the file
    resq = requests.get(url + namenodeField + '/' + inumSection + jsonSuffix).json()
    name = path.split('/')[-1]
    inumber = ''
    # print(name)
    for k, v in resq.items():
        if v['name'] == name:
            # print(1)
            inumber = k
            break
    
    ##find the datanodes which stored the blocks of the file
    blocks = {} #record datanode number 
    blockNum = list(requests.get(url + namenodeField + '/' + inumSection + '/' + inumber + '/' + 'blocks' + jsonSuffix).json().keys())
    for i in blockNum:
        blocks[i] = requests.get(url + namenodeField + '/' + inumSection + '/' + inumber + '/' + 'blocks' + '/' + i + jsonSuffix).json()
    # print(blocks)

    # delete blocks in datanode
    for blockNum, datanodeNum in blocks.items():
        for i in datanodeNum:
            if len(requests.get(datanodeMapping[i] + jsonSuffix).json().keys()) == 1: 
                requests.put(datanodeMapping[i] + '/' + jsonSuffix, '""')
            else:
                requests.delete(datanodeMapping[i] + '/' + blockNum + jsonSuffix)
    
    ##delete metadata
    #find parent dir
    parent = path.split('/')[-2] if path.split('/')[-2] else '/'
    inumberP = ''
    for k, v in resq.items():
        if v['name'] == parent:
            # print(1)
            inumberP = k
            break
    #remove the entry in dirSection
    res = requests.get(url + namenodeField + '/' + dirSection + '/' + inumberP + jsonSuffix).json()
    if len(list(res.keys())) == 1:
        requests.put(url + namenodeField + '/' + dirSection + '/' + inumberP + jsonSuffix, '""')   
    else: 
        requests.delete(url + namenodeField + '/' + dirSection + '/' + inumberP + '/' + inumber + jsonSuffix)
    #remove the entry in inodeSection
    requests.delete(url + namenodeField + '/' + inumSection + '/' + inumber + jsonSuffix)

def cat(path):
    # get info in namenode from source path
    # getting parent and name
    name = path.rsplit('/', 1)[1]
    name = '/' if not name else name
    get_data = ""
    blk = {}
    # finding data in datanode by blocknumber from namenodeinfo
    resp = requests.get(url + '/' + namenodeField  + '/' + inumSection + jsonSuffix).json()
    for k, v in resp.items():
        if v['name'] == name:
            blk = v['blocks']
    for k, v in blk.items():
        # k is the blocknum, and the first value in v(which is a list) would be the place we find the data we sotred
        datanode_num = v[0]
        datanode_url = datanodeMapping[datanode_num]
        resp1 = requests.get(datanode_url + '/' + k + jsonSuffix)
        get_data = get_data + resp1.text
    return get_data

def get(edfs_source_path, local_destiniation_path):
    data = cat(edfs_source_path)
    name = edfs_source_path.rsplit('/', 1)[1]
    with open(local_destiniation_path, 'w') as fh:
        fh.write(data)

def rmdir(path):
    # search for parent and location in directory section
    # generate name and parent name
    name = path.rsplit('/', 1)[1]
    name = '/' if not name else name
    parent = path.split('/')[-2] 
    parent = '/' if not parent else parent
    mes = ''
    resp = requests.get(url + '/' + namenodeField  + '/' + inumSection + jsonSuffix).json()
    # finding inumber for the directory and its parent
    for k, v in resp.items():
        if v['name'] == name:
            inum = k
        elif v['name'] == parent:
            p_inum = k
    resp1 = requests.get(url + '/' + namenodeField  + '/' + dirSection + '/' + inum + jsonSuffix).json()
    if not resp1: # if the dir is empty
        requests.delete(url + '/' + namenodeField  + '/' + dirSection + '/' + inum + jsonSuffix)
        requests.delete(url + '/' + namenodeField  + '/' + inumSection + '/' + inum + jsonSuffix)
    else:
        mes = "This directory is not empty!"
    # delete under parent in dir section
    resp2 = requests.delete(url + '/' + namenodeField  + '/' + dirSection + '/' + p_inum + '/' + inum + jsonSuffix)
    if resp2.status_code == 200:
        mes = 'Successfully remove the file!'
    return mes

async def handle_client(reader, writer):
    data = await reader.read(128000)
    data = data.decode()
    message = json.loads(data)

    if message[0] == '-ls':
        if ls(message[1]):
            writer.write(bytes(' '.join(ls(message[1])), 'utf-8'))
        else:
            writer.write(bytes(' ', 'utf-8'))
    elif message[0] == '-rm':
        rm(message[1])
        writer.write(b'Successfully remove the file!')
    elif message[0] == '-mkdir':
        mkdir(message[1])
        writer.write(b'Successfully make the directory!')
    elif message[0] == '-rmdir':
        ms = rmdir(message[1])
        writer.write(bytes(ms, 'utf-8'))
    elif message[0] == '-cat':
        ms = cat(message[1])
        writer.write(bytes(ms, 'utf-8'))
    elif message[0] == '-get':
        ms = cat(message[1])
        writer.write(bytes(ms, 'utf-8'))
    elif message[0] == '-put':
        local_file_name = message[1]
        file_name = message[2]
        put(local_file_name,file_name)
        writer.write(b'Successfully put file to the edfs!')




async def main():
    server = await asyncio.start_server(
        handle_client, '127.0.0.1', 5555)
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    url = 'https://fir-3d546-default-rtdb.firebaseio.com/'
    namenodeField = 'namenode'
    inumSection = 'inodeSection'
    blockField = 'blocks'
    dirSection = 'dirSection'
    jsonSuffix = ".json"
    perferredBlockSize = 128000

    datanode_1 = 'https://dsci-551-a9568-default-rtdb.firebaseio.com/datanode_1'
    datanode_2 = 'https://dsci551-project-83c7c-default-rtdb.firebaseio.com/datanode_2'
    datanode_3 = 'https://datanode3-cb3f4-default-rtdb.firebaseio.com/datanode_3'
    datanode_4 = 'https://datanode4-default-rtdb.firebaseio.com/datanode_4'

    datanodeMapping = {
        1: datanode_1,
        2: datanode_2,
        3: datanode_3,
        4: datanode_4
    }

    initNamenode()
    initDatanode()
    asyncio.run(main())



