import socket
import re
import threading
import time
import hashlib
import math
# import matplotlib.pyplot as plt

server = "10.17.7.134"
port = 9801
serverAddressPort = (server, port)
bufferSize = 4096
UDPsocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
UDPsocket.settimeout(1)

request_time = []
request_offset = []
reply_time = []
reply_offset = []
re_request_offset = []
re_request_time = []

inTransitSize = 15
rateSize = 50
rateUpperLimit = 10000
rate = 1/rateSize #time b/w diff messages in seconds

timeout = 0.05  # the time to wait to loop through the inTransit set again
lastReceivedPartiitonSetSize = 0

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

def rateIncrease():
    global start_time, inTransit, inTransitSize, rate, remainingPartitions, inTransitlock, receivedPartitionSet, lastReceivedPartiitonSetSize, rateSize
    # print("running this functionnnnnnnnnn")
    # lastCount = 0
    # while True:
    if (len(inTransit) <= inTransitSize and rateSize < rateUpperLimit ):
        rateSize += 1
        rate = 1/rateSize
        # print("length of received partition set ",len(receivedPartitionSet)," last count is ",lastCount)
        # print("length of the receied partiiton set is ",len(receivedPartitionSet)," last received partition set size is ",lastReceivedPartiitonSetSize)
        # if ((len(receivedPartitionSet) - lastReceivedPartiitonSetSize) % inTransitSize == 0) and (len(inTransit) <= inTransitSize) and (len(receivedPartitionSet) != 0): 
        #     lastReceivedPartiitonSetSize = len(receivedPartitionSet)
        #     # with inTransitlock:
        #     inTransitSize += 1
        #     print("rate increased new rate from ",rate, " to ", 1/inTransitSize)
        #     print("new intranit window size ",inTransitSize)
        #     rate = 1/inTransitSize
        # if len(remainingPartitions) == 0:
        #     break
        # time.sleep(0.1)

def sendToSever(): 
    global remainingPartitions, UDPsocket, inTransit, maxbytes, rate, receivedPartitionSet, totalsize, start_time, inTransitlock, inTransitSize, timeout, rateSize, rateUpperLimit
    inTransit = set()
    burn = []
    while (len(remainingPartitions) > 0):
        for u in remainingPartitions:
            # print("running rate of sending request rn is ",rate)
            print("running rateSize is ", rateSize)
            # print("inTransit window maximum size allowed ",inTransitSize)
            print("Curr intransit size: ", len(inTransit))
            # print("printing the length of the list of remaining partititions : ", len(remainingPartitions))
            
            if len(inTransit) > inTransitSize:
                burn.append(u)
                for j in range (2):
                    time.sleep(timeout)
                    with inTransitlock:
                        for i in inTransit:
                            message = f"Offset: {i*maxbytes}\nNumBytes: {min(maxbytes, abs(totalsize-maxbytes*i))}\n\n"
                            UDPsocket.sendto(message.encode(), serverAddressPort)
                if len(inTransit) > inTransitSize:
                    rateUpperLimit = min(rateUpperLimit, rateSize)
                    rateSize //= 2
                    if rateSize == 0:
                        rateSize = 1
                    rate = 1/rateSize
                    print("rate is being decreasedddddddddddddddd", rateSize, rate)
            
            else:
                begin = time.time()
                message = f"Offset: {u*maxbytes}\nNumBytes: {min(maxbytes, abs(totalsize-maxbytes*u))}\n\n"
                UDPsocket.sendto(message.encode(), serverAddressPort)
                with inTransitlock:
                    inTransit.add(u)
            
                end = time.time()
                # if (len(remainingPartitions) == 1):
                #     time.sleep(0.1)
                if (end-begin < rate):
                    time.sleep(rate-(end-begin))
        remainingPartitions = []
        for u in inTransit:
            if u not in receivedPartitionSet:
                re_request_offset.append(u*maxbytes)
                re_request_time.append(time.time()-start_time)
                remainingPartitions.append(u)
        for u in burn:
            if u not in receivedPartitionSet:
                remainingPartitions.append(u)
    print("send exited")

def recvFromServer():
    global UDPsocket, receivedPartitionSet, inTransit, receivedPartitions, maxbytes, totalsize, start_time, inTransitlock
    receivedPartitions = ["" for i in range(math.ceil(totalsize/maxbytes))]
    receivedPartitionSet = set()
    while (len(receivedPartitionSet) < math.ceil(totalsize/maxbytes)):
        try:
            reply = UDPsocket.recvfrom(bufferSize)
            reply = reply[0].decode()
            # print("the reply is ", reply)
            # print("received a reply, the repy is  ",reply)
            if not re.fullmatch(r"Size:\s+\d+\n\n", reply):

                offset = re.search(r"Offset:\s+(\d+)", reply).group(1)
                numBytes = re.search(r"NumBytes:\s+(\d+)", reply).group(1)
                data =  re.search(r"(?:NumBytes:\s+\d+|Squished)\n\n(.*)", reply, re.DOTALL).group(1)
                receivedPartitions[int(offset)//maxbytes] = data
                # if (time.time() - start_time <= 0.5):
                #     reply_offset.append(int(offset))
                #     reply_time.append(time.time() - start_time)
                # reply_offset.append(int(offset))
                # reply_time.append(time.time() - start_time)
                receivedPartitionSet.add(int(offset)//maxbytes)
                # if inTransitlock.locked():
                #     print("it is locked")
                with inTransitlock:
                    inTransit.remove(int(offset)//maxbytes)
                    # print("received a reply and removing the partition from intransit function")
                # print("recv partition set: ", len(receivedPartitionSet))
                if ("Squished" in data):
                    print(data)
                rateIncrease()
            # print("InTransit: ", inTransit)
        except:
            continue
    print("rate size final is ", rateSize)
    print("rate upper limit is ", rateUpperLimit)
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
            reply = reply[0].decode()
            if "Result" in reply:
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
    global totalsize, remainingPartitions, maxbytes, start_time, inTransitlock
    maxbytes = 1448 #given
    #Initialize a get total size
    getTotalSize() #implement reliable size transfer
    # print(totalsize)
    print("Total Size:" , totalsize) 
    remainingPartitions = []
    inTransitlock = threading.Lock()
    partitions = math.ceil(totalsize/maxbytes)
    print("Partitions: " , partitions)
    for i in range(partitions):
        remainingPartitions.append(i)

    #Initialize a send to server thread
    sendThread = threading.Thread(target=sendToSever)
    # rateIncreaseThread = threading.Thread(target=rateIncrease)
    start_time = time.time()
    # print("printing start time", start_time)
    sendThread.start()
    #Initialize a recv from server thread
    recvThread = threading.Thread(target=recvFromServer)
    recvThread.start()
    # rateIncreaseThread.start()

    sendThread.join()
    recvThread.join()
    # rateIncreaseThread.join()
    
    finalString = "".join(receivedPartitions)
    # print(len(receivedPartitions))
    # with open("finaloutput.txt", "w") as file:
    #     file.write(finalString)

    md5 = hashlib.md5()
    md5.update(finalString.encode())
    md5_hash = md5.hexdigest()
    # print("MD5: ", md5_hash)
    submitFinal(md5_hash)
    UDPsocket.close()

    # plt.scatter(request_time,request_offset, label =  "Request time against offset", color = 'blue')
    # plt.scatter(reply_time, reply_offset, label =  "Reply time against offset", color = 'orange')
    # plt.scatter(re_request_time,re_request_offset, label = "Offsets for which requests were sent again", color = 'green')
    # plt.xlabel('Time')
    # plt.ylabel('Offset')
    # plt.title('Sequence number trace')
    # plt.legend()
    # plt.show()
    # plt.savefig('my_plot.png')
