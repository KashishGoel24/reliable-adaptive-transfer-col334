import socket
import re
import threading
import time
import hashlib
import math



server = "127.0.0.1"
port = 9802
serverAddressPort = (server, port)
bufferSize = 4096
UDPsocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
UDPsocket.settimeout(1)
rate = 0.01 #time b/w diff messages in seconds

def sendSizeReq():
    global UDPsocket, sizeflag
    while (sizeflag == False):
        try:
            print("Sending SendSize")
            UDPsocket.sendto(b"SendSize\nReset\n\n", serverAddressPort)
            time.sleep(0.1)
        except:
            continue

def recvSizeReq():
    global UDPsocket, sizeflag, totalsize, bufferSize
    while (sizeflag == False):
        try:
            reply = UDPsocket.recvfrom(bufferSize)
            print("Receieved SendSize")
            totalsize = int(re.search(r"Size:\s+(\d+)", reply[0].decode()).group(1))
            sizeflag = True
            break
        except:
            continue

def getTotalSize():
    global totalsize, UDPsocket, sizeflag
    sizeflag = False
    sendThread = threading.Thread(target=sendSizeReq)
    recvThread = threading.Thread(target=recvSizeReq)
    sendThread.start()
    recvThread.start()
    sendThread.join()
    recvThread.join()

def sendToSever():
    global remainingPartitions, UDPsocket, inTransit, maxbytes, rate, receivedPartitionSet, totalsize
    inTransit = set()
    while (len(remainingPartitions) > 0):
        for u in remainingPartitions:
            begin = time.time()
            message = f"Offset: {u*maxbytes}\nNumBytes: {min(maxbytes, abs(totalsize-maxbytes*u))}\n\n"
            # if (u == 6):
            #     print("Sending: ", message)
            UDPsocket.sendto(message.encode(), serverAddressPort)
            inTransit.add(u)
            end = time.time()
            # print("InTransit: ", inTransit)
            # print("Remaining Partitions: ", remainingPartitions)
            if (len(remainingPartitions) == 1):
                time.sleep(0.1)
            if (end-begin < rate):
                time.sleep(rate-(end-begin))
        remainingPartitions = []
        for u in inTransit:
            if u not in receivedPartitionSet:
                remainingPartitions.append(u)
    print("send exited")

def recvFromServer():
    global UDPsocket, receivedPartitionSet, inTransit, receivedPartitions, maxbytes, totalsize
    receivedPartitions = ["" for i in range(math.ceil(totalsize/maxbytes))]
    receivedPartitionSet = set()
    while (len(receivedPartitionSet) < math.ceil(totalsize/maxbytes)):
        try:
            reply = UDPsocket.recvfrom(bufferSize)
            reply = reply[0].decode()
            if not re.fullmatch(r"Size:\s+\d+\n\n", reply):
                offset = re.search(r"Offset:\s+(\d+)", reply).group(1)
                numBytes = re.search(r"NumBytes:\s+(\d+)", reply).group(1)
                data =  re.search(r"(?:NumBytes:\s+\d+|Squished)\n\n(.*)", reply, re.DOTALL).group(1)
                receivedPartitions[int(offset)//maxbytes] = data
                receivedPartitionSet.add(int(offset)//maxbytes)
                inTransit.remove(int(offset)//maxbytes)
                print("recv partition set: ", len(receivedPartitionSet))
            # print("InTransit: ", inTransit)
        except:
            continue
    print("recv exited")

def sendFinalHash(hash):
    global UDPsocket, finalflag
    while finalflag == False:
        try:
            print("Sending Final Hash")
            msg = f"Submit: 2021CS10069\nMD5: {hash}\n\n"
            UDPsocket.sendto(msg.encode(), serverAddressPort)
            time.sleep(0.1)
        except:
            continue

def recvFinalHash():
    global UDPsocket, finalflag
    while finalflag == False:
        try:
            reply = UDPsocket.recvfrom(bufferSize)
            print("Received Reply")
            print(reply)
            finalflag = True
            break
        except:
            continue

def submitFinal(hash):
    global UDPsocket, finalflag
    finalflag = False
    sendThread = threading.Thread(target=sendFinalHash, args=(hash,))
    recvThread = threading.Thread(target=recvFinalHash)
    sendThread.start()
    recvThread.start()
    sendThread.join()
    recvThread.join()

            

if __name__ == "__main__":
    global totalsize, remainingPartitions, maxbytes
    maxbytes = 1448 #given
    #Initialize a get total size
    getTotalSize() #implement reliable size transfer
    print(totalsize)
    # totalsize = 210175
    print("Total Size:" , totalsize) 
    remainingPartitions = []
    partitions = math.ceil(totalsize/maxbytes)
    print("Partitions: " , partitions)
    for i in range(partitions):
        remainingPartitions.append(i)

    #Initialize a send to server thread
    sendThread = threading.Thread(target=sendToSever)
    sendThread.start()
    #Initialize a recv from server thread
    recvThread = threading.Thread(target=recvFromServer)
    recvThread.start()

    sendThread.join()
    recvThread.join()
    
    finalString = "".join(receivedPartitions)
    print(len(receivedPartitions))
    # with open("finaloutput.txt", "w") as file:
    #     file.write(finalString)

    md5 = hashlib.md5()
    md5.update(finalString.encode())
    md5_hash = md5.hexdigest()
    print("MD5: ", md5_hash)
    submitFinal(md5_hash)
    UDPsocket.close()