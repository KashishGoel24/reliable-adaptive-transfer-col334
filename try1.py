import socket
import re
import threading
import time
import hashlib
import math
import random
import matplotlib.pyplot as plt

# server = "127.0.0.1"
server = "10.17.51.115"
port = 9802
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
rttEstimates = []
rates = []
rate_sizes = []
rate_times = []
squishTime = []

inTransitSize = 4
rateSize = 100
rateUpperLimit = 10000
rate = 1/rateSize #time b/w diff messages in seconds
timescalled = 0
timesrandom = 0
squishState = False
squishFactor = 5
timeout = 0.05  # the time to wait to loop through the inTransit set again
lastReceivedPartitionSetSize = 0
estimatedRTT = 0
devRTT = 0
alpha = 0.125
beta = 0.125
lastreduce = time.time()
ssthresh = (64000 // 1448)

# 6398 , 107 - incrementing size after every ack
# 15349 , 0 - incrementing size after receiving n new acks where n = rate size
# 7772 , 0 - incrementing size after receiving n new acks where n = intransit size

def sendSizeReq():
    global UDPsocket, sizeflag, sendsizeStartTime
    while (sizeflag == False):
        try:
            sendsizeStartTime = time.time()
            print("Sending SendSize")
            UDPsocket.sendto(b"SendSize\nReset\n\n", serverAddressPort)
            time.sleep(0.1)
        except:
            continue

def recvSizeReq():
    global UDPsocket, sizeflag, totalsize, bufferSize, sendsizeStartTime, rttEstimates
    while (sizeflag == False):
        try:
            estimatedRTT = time.time() - sendsizeStartTime
            reply = UDPsocket.recvfrom(bufferSize)
            print("Receieved SendSize")
            rttEstimates.append(estimatedRTT)
            # print(sendsizeStartTime, time.time(),estimatedRTT)
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
    global start_time, inTransit, inTransitSize, rate, remainingPartitions, inTransitlock, receivedPartitionSet, lastReceivedPartitionSetSize, rateSize, timescalled, timesrandom, squishState, squishFactor, rates, rate_times, ssthresh
    # print("running this functionnnnnnnnnn")
    # while True:
    randomNum = random.random()
    if (squishState):
        rate = (1/rateSize)*squishFactor
        # time.sleep(0.04)
        # print("the squished rate has been used")
    elif (len(receivedPartitionSet) < ssthresh):
        print("in the ssthresh function")
        rateSize = rateSize*rateSize
        rate = 1/rateSize
    elif (len(inTransit) <= inTransitSize and (rateUpperLimit == 10000 or randomNum < 0.05)):
    # elif (len(inTransit) <= inTransitSize):
        if (rateSize < 0.9*rateUpperLimit):
            rateSize += 1
            rate = 1/rateSize 
        else:
            randomNum = random.random()
            if (0.9*rateUpperLimit < rateSize < 1.1*rateUpperLimit):
                probability = 0.2
            else:
                probability = 0.05
            if randomNum < probability:
                rateSize += 1
                rate = 1/rateSize
                timesrandom += 1
            timescalled += 1
    rate_sizes.append(rateSize)
    if (squishState):
        squishTime.append(1)
    else:
        squishTime.append(0)
    rates.append(rate)
    rate_times.append(time.time()-start_time)

def sendToSever(): 
    global remainingPartitions, UDPsocket, inTransit, maxbytes, rate, receivedPartitionSet, totalsize, start_time, inTransitlock, inTransitSize, timeout, rateSize, rateUpperLimit, squishFactor, squishState, requestsTime, rates, rate_times, lastreduce
    inTransit = set()
    requestsTime = {}
    # burn = []
    while (len(remainingPartitions) > 0 ):
        k = len(remainingPartitions) - 1
        while k >= 0:
            u = remainingPartitions[k]
            # print("running rate of sending request rn is ",rate)
            print("running rateSize is ", rateSize, "squish mode is ", squishState)
            # print("inTransit window maximum size allowed ",inTransitSize)
            print("Curr intransit size: ", len(inTransit))
            print("current size of remaining partitions set ",len(remainingPartitions))
            # print("printing the length of the list of remaining partititions : ", len(remainingPartitions))
            
            if len(inTransit) > inTransitSize:
                # burn.append(u)
                for j in range (2):
                    if (len(inTransit) > inTransitSize):
                        inTransitList = []
                        with inTransitlock:
                            for i in inTransit:
                                inTransitList.append(i)
                        for i in inTransitList:
                            if (len(inTransit) <= inTransitSize):
                                break
                            begin = time.time()
                            requestsTime[u] = begin
                            message = f"Offset: {i*maxbytes}\nNumBytes: {min(maxbytes, abs(totalsize-maxbytes*i))}\n\n"
                            UDPsocket.sendto(message.encode(), serverAddressPort)
                            end = time.time()
                            if (end-begin < rate):
                                time.sleep(rate-(end-begin))
                    else:
                        break
                if len(inTransit) > inTransitSize:
                    if (time.time() - lastreduce >= 0.5):
                        rateUpperLimit = rateSize
                        rateSize //= 1.5
                        lastreduce = time.time()
                        if rateSize == 0:
                            rateSize = 1
                        rate = 1/rateSize
                    if (squishState == True):
                        rate = (1/rateSize)*squishFactor
                        time.sleep(0.04)
                    # rates.append(rateSize)
                    # rate_times.append(time.time()-start_time)
                    print("rate is being decreasedddddddddddddddd", rateSize, rate)
            
                rates.append(rate)
                rate_sizes.append(rateSize)
                if (squishState):
                    squishTime.append(1)
                else:
                    squishTime.append(0)
                rate_times.append(time.time()-start_time)

            else:
                begin = time.time()
                requestsTime[u] = begin
                message = f"Offset: {u*maxbytes}\nNumBytes: {min(maxbytes, abs(totalsize-maxbytes*u))}\n\n"
                UDPsocket.sendto(message.encode(), serverAddressPort)
                with inTransitlock:
                    inTransit.add(u)
            
                end = time.time()
                k -= 1
                remainingPartitions.pop()
                # if (len(remainingPartitions) == 1):
                #     time.sleep(0.1)
                if (end-begin < rate):
                    time.sleep(rate-(end-begin))

        # remainingPartitions = []
        for u in inTransit:
            if u not in receivedPartitionSet:
                # re_request_offset.append(u*maxbytes)
                # re_request_time.append(time.time()-start_time)
                remainingPartitions.append(u)
        # for u in burn:
        #     if u not in receivedPartitionSet:
        #         remainingPartitions.append(u)
    print("send exited")

def recvFromServer():
    global UDPsocket, receivedPartitionSet, inTransit, receivedPartitions, maxbytes, totalsize, start_time, inTransitlock, timescalled, timesrandom, squishState, requestsTime, alpha, estimatedRTT, rttEstimates, remainingPartitions, devRTT, beta, lastreduce, rateSize
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

                sampleRTT = time.time() - requestsTime[int(offset)//maxbytes]
                estimatedRTT = (1-alpha)*estimatedRTT + alpha*sampleRTT
                devRTT = (1-beta)*devRTT + beta*(abs(estimatedRTT-sampleRTT))
                del requestsTime[int(offset)//maxbytes]
                # print("priting the length of requests time ",len(requestsTime))
                rttEstimates.append(estimatedRTT)
                # print("the new estimate of rtt is ",estimatedRTT)
                # if (time.time() - start_time <= 0.5):
                #     reply_offset.append(int(offset))
                #     reply_time.append(time.time() - start_time)
                reply_offset.append(int(offset))
                reply_time.append(time.time() - start_time)
                receivedPartitionSet.add(int(offset)//maxbytes)
                # if inTransitlock.locked():
                #     print("it is locked")
                with inTransitlock:
                    inTransit.remove(int(offset)//maxbytes)
                    # print("received a reply and removing the partition from intransit function")
                print("recv partition set: ", len(receivedPartitionSet))
                if ("Squished" in reply):
                    squishState = True
                    if squishfirst and (time.time() - lastreduce >= 1.5):
                        lastreduce = time.time()
                        rateSize //= 1.5
                        squishfirst = False
                    # print("i am in here")
                    # squishTime.append(time.time()-start_time)
                else:
                    squishfirst = True
                    squishState = False
                    # print(reply)
                rateIncrease()
            # print("InTransit: ", inTransit)
        except:
            continue
    print("rate size final is ", rateSize)
    print("rate upper limit is ", rateUpperLimit)
    print("Times rateSize > UpperLimit ", timescalled)
    print("Times random increase ", timesrandom)
    print("recv exited")
    # print("size of received partition set ",len(receivedPartitionSet), " the lenght of remaining partitions set ",len(remainingPartitions))
    # print("following are the elements in remaining partitions ")
    # for i in remainingPartitions:
    #     print(i)
    # print("following are the elements present in intransit")
    # for i in inTransit:
    #     print(i)
    # print("rtt estimates done are as follows: ",rttEstimates)
    print("average rtt is ",sum(rttEstimates)/len(rttEstimates))
    print("dev rtt is ", devRTT)
    print("hence the timeout must be ",4*devRTT + sum(rttEstimates)/len(rttEstimates))

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
    rate = estimatedRTT[0]
    rateSize = 1/rate
    rates.append(rate)
    rate_sizes.append(rateSize)
    rate_times.append(0)
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

    # print("the rates used to obtain the data in the server were ",rates)
    print("average rate used by the server ",sum(rates)/len(rates))


    # print("rate array ",rates)
    # plt.plot(rate_times, rates, label = "rates vs rate times", color = "green")
    # plt.plot(rate_times,squishTime, label = "squished satate - 0 or 1")
    # plt.plot(rate_times,rate_sizes, label = "rate size vs rate times", color = "orange")
    # plt.scatter(request_time,request_offset, label =  "Request time against offset", color = 'blue')
    # plt.scatter(reply_time, reply_offset, label =  "Reply time against offset", color = 'orange')
    # plt.scatter(re_request_time,re_request_offset, label = "Offsets for which requests were sent again", color = 'green')
    # plt.xlabel('Time')
    # plt.ylabel('Offset')
    # plt.title('Sequence number trace')
    # plt.legend()
    # plt.show()
    # plt.savefig('my_plot.png')

    # Create a figure and a set of subplots
    fig, ax1 = plt.subplots()

    ax1.plot(rate_times, rate_sizes, color='green', label='Rate Sizes vs time')
    ax1.set_xlabel('Time')
    ax1.set_ylabel('Rate Sizes', color='green')
    ax1.tick_params(axis='y', labelcolor='green')

    ax2 = ax1.twinx()

    ax2.plot(rate_times, squishTime, color='orange', label='Squish state vs time')
    ax2.set_ylabel('Squish State', color='orange')
    ax2.tick_params(axis='y', labelcolor='orange')
    ax2.set_ylim(-1, 10)

    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')

    plt.title("Rate size trace along with squish state for constant rate server")

    plt.show()
