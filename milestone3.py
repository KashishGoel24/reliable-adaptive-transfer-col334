import socket
import re
import threading
import time
import hashlib
import math
import random
# import matplotlib.pyplot as plt

# server = "127.0.0.1"
server = "10.17.51.115"
# server = "vayu.iitd.ac.in"
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
rttestimates_time = []
devRTT_array = []
rates = []
rate_sizes = []
rate_times = []
squishTime = []
currhigh_array = []
highCount_array = []
highhigh_time = []
inTransitsize_arr = []
inTransitsizetime = []
constantIntransit = []

inTransitSize = 4
rateSize = 100
rateUpperLimit = 10000
currhigh = 10000
currhighcount = 0
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
    global UDPsocket, sizeflag, totalsize, bufferSize, sendsizeStartTime, rttEstimates, start_time, rttestimates_time, devRTT_array
    while (sizeflag == False):
        try:
            estimatedRTT = time.time() - sendsizeStartTime
            reply = UDPsocket.recvfrom(bufferSize)
            print("Receieved SendSize")
            rttEstimates.append(estimatedRTT)
            rttestimates_time.append(time.time() - start_time)
            devRTT_array.append(0)
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
    global start_time, inTransit, inTransitSize, rate, remainingPartitions, inTransitlock, receivedPartitionSet, lastReceivedPartitionSetSize, rateSize, timescalled, timesrandom, squishState, squishFactor, rates, rate_times, currhighcount
    # print("running this functionnnnnnnnnn")
    # while True:
    randomNum = random.random()
    if (squishState):
        rate = (1/rateSize)*squishFactor
        # time.sleep(0.04)
        # print("the squished rate has been used")
    elif (len(inTransit) <= inTransitSize and ((rateUpperLimit == 10000 and randomNum < 0.2) or randomNum < 0.3)):
    # elif (len(inTransit) <= inTransitSize):
        if (currhighcount >= 1):
            randomNum = random.random()
            if (rateSize < currhigh*0.93):
                if (randomNum < 0.7):
                    rateSize += 1
                    rate = 1/rateSize
            else:
                if (randomNum < 0.03):
                    rateSize += 1
                    rate = 1/rateSize
        else:
            if (rateSize < 0.9*rateUpperLimit):
                rateSize += 1
                rate = 1/rateSize 
            else:
                randomNum = random.random()
                if (0.85*rateUpperLimit < rateSize < 0.95*rateUpperLimit):
                    probability = 0.15
                else:
                    probability = 0.03
                if randomNum < probability:
                    rateSize += 1
                    rate = 1/rateSize
                    timesrandom += 1
                timescalled += 1
    # rate_sizes.append(rateSize)
    # if (squishState):
    #     squishTime.append(1)
    # else:
    #     squishTime.append(0)
    # rates.append(rate)
    # rate_times.append(time.time()-start_time)

def sendToSever(): 
    global remainingPartitions, UDPsocket, inTransit, maxbytes, rate, receivedPartitionSet, totalsize, start_time, inTransitlock, inTransitSize, timeout, rateSize, rateUpperLimit, squishFactor, squishState, requestsTime, rates, rate_times, lastreduce, currhigh, currhighcount, currhigh_array, highCount_array, highhigh_time
    inTransit = set()
    requestsTime = {}
    # burn = []
    while (len(remainingPartitions) > 0 ):
        k = len(remainingPartitions) - 1
        while k >= 0:
            # inTransitsize_arr.append(len(inTransit))
            # inTransitsizetime.append(time.time() - start_time)
            # constantIntransit.append(inTransitSize)
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
                    if (len(inTransit) > 1):
                        inTransitList = []
                        with inTransitlock:
                            for i in inTransit:
                                inTransitList.append(i)
                        for i in inTransitList:
                            begin = time.time()
                            requestsTime[u] = begin
                            message = f"Offset: {i*maxbytes}\nNumBytes: {min(maxbytes, abs(totalsize-maxbytes*i))}\n\n"
                            UDPsocket.sendto(message.encode(), serverAddressPort)
                            # if time.time() - start_time <= 0.5:
                            # request_time.append(time.time() - start_time)
                            # request_offset.append(u*maxbytes)
                            end = time.time()
                            if (end-begin < rate):
                                time.sleep(rate-(end-begin))
                    else:
                        break

                    if len(inTransit) > 1:
                        if (time.time() - lastreduce >= 0.3):
                            if (rateUpperLimit == 10000):
                                rateUpperLimit = rateSize
                            if (0.9 <= currhigh/rateSize <= 1.1):
                                currhighcount += 1
                            else:
                                currhigh = rateSize
                                currhighcount = 0
                            if (currhighcount >= 1):
                                rateUpperLimit = currhigh
                                rateSize = min(rateSize, currhigh)*0.85
                            else:
                                rateSize //= 1.1
                            lastreduce = time.time()
                            if rateSize == 0:
                                rateSize = 1
                            rate = 1/rateSize
                        if (squishState == True):
                            rate = (1/rateSize)*squishFactor
                        # rates.append(rateSize)
                        # rate_times.append(time.time()-start_time)
                        print("rate is being decreasedddddddddddddddd", rateSize, rate)
            
                # rates.append(rate)
                # rate_sizes.append(rateSize)
                # if (squishState):
                #     squishTime.append(1)
                # else:
                #     squishTime.append(0)
                # rate_times.append(time.time()-start_time)

                # if (currhigh != 10000):
                #     highhigh_time.append(time.time() - start_time)
                #     currhigh_array.append(currhigh)
                #     highCount_array.append(currhighcount)
                # highhigh_time.append(time.time() - start_time)
                # currhigh_array.append(currhigh)
                # highCount_array.append(currhighcount)

            else:
                begin = time.time()
                requestsTime[u] = begin
                message = f"Offset: {u*maxbytes}\nNumBytes: {min(maxbytes, abs(totalsize-maxbytes*u))}\n\n"
                UDPsocket.sendto(message.encode(), serverAddressPort)
                # if time.time()- start_time <= 0.5:
                request_time.append(time.time() - start_time)
                request_offset.append(u*maxbytes)
                with inTransitlock:
                    inTransit.add(u)
            
                end = time.time()
                k -= 1
                remainingPartitions.pop()
                # if (len(remainingPartitions) == 1):
                #     time.sleep(0.1)
                if (end-begin < rate):
                    time.sleep(rate-(end-begin))

        for u in inTransit:
            if u not in receivedPartitionSet:
                remainingPartitions.append(u)
    print("send exited")

def recvFromServer():
    global UDPsocket, receivedPartitionSet, inTransit, receivedPartitions, maxbytes, totalsize, start_time, inTransitlock, timescalled, timesrandom, squishState, requestsTime, alpha, estimatedRTT, rttEstimates, remainingPartitions, devRTT, beta, lastreduce, rateSize, rateUpperLimit, devRTT_array, rttestimates_time
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
                rttEstimates.append(estimatedRTT)
                # devRTT_array.append(devRTT)
                # rttestimates_time.append(time.time() - start_time)
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
                        rateUpperLimit = rateSize
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
    start_time = time.time()
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
    sendThread.start()
    #Initialize a recv from server thread
    recvThread = threading.Thread(target=recvFromServer)
    recvThread.start()

    sendThread.join()
    recvThread.join()
    
    finalString = "".join(receivedPartitions)

    md5 = hashlib.md5()
    md5.update(finalString.encode())
    md5_hash = md5.hexdigest()
    # print("MD5: ", md5_hash)
    submitFinal(md5_hash)
    UDPsocket.close()

    # print("the rates used to obtain the data in the server were ",rates)
    # print("average rate used by the server ",sum(rates)/len(rates))

    # plt.scatter(request_time,request_offset, label =  "Request time against offset", color = 'blue')
    # plt.scatter(reply_time, reply_offset, label =  "Reply time against offset", color = 'orange')
    # plt.xlabel('Time')
    # plt.ylabel('Offset')
    # plt.title('Sequence number trace')
    # plt.legend()
    # plt.show()
    # plt.savefig('my_plot.png')

    # plt.plot(rttestimates_time,rttEstimates, label =  "Calculated value of RTT against time", color = 'green')
    # plt.xlabel('Time')
    # plt.ylabel('RTT Value estimated')
    # plt.title('RTT Estimation using EWMA')
    # plt.legend()
    # plt.show()
    # plt.savefig('my_plot.png')

    # plt.plot(rttestimates_time,devRTT_array, label =  "Calculated value of Deviation RTT against time", color = 'yellow')
    # plt.xlabel('Time')
    # plt.ylabel('Deviation RTT Value estimated')
    # plt.title('RTT and Deviation RTT Estimation using EWMA')
    # plt.legend()
    # plt.show()
    # plt.savefig('my_plot.png')

    # Create a figure and a set of subplots
    # fig, ax1 = plt.subplots()

    # ax1.plot(rate_times, rate_sizes, color='green', label='Rate sizes vs time')
    # ax1.set_xlabel('Time')
    # ax1.set_ylabel('Rate Size', color='green')
    # ax1.tick_params(axis='y', labelcolor='green')

    # ax2 = ax1.twinx()

    # ax2.plot(rate_times, squishTime, color='orange', label='Squish state vs time')
    # ax2.set_ylabel('Squish State', color='orange')
    # ax2.tick_params(axis='y', labelcolor='orange')
    # ax2.set_ylim(-1, 10)

    # ax1.legend(loc='upper left')
    # ax2.legend(loc='upper right')

    # plt.title("Rate Size trace along with squish state for variable rate server")

    # plt.show()


    # for current high and high count
    # fig, ax1 = plt.subplots()

    # ax1.plot(highhigh_time, currhigh_array, color='green', label='Current High vs time')
    # ax1.set_xlabel('Time')
    # ax1.set_ylabel('Current High', color='green')
    # ax1.tick_params(axis='y', labelcolor='green')

    # ax2 = ax1.twinx()

    # ax2.plot(highhigh_time, highCount_array, color='orange', label='Current High Count vs time')
    # ax2.set_ylabel('Current High Count', color='orange')
    # ax2.tick_params(axis='y', labelcolor='orange')
    # # ax2.set_ylim(-1, 10)

    # ax1.legend(loc='upper left')
    # ax2.legend(loc='upper right')

    # plt.title("Plot of current high and current high count against time")

    # plt.show()


    # for intransit size
    # fig, ax1 = plt.subplots()

    # ax1.plot(inTransitsizetime, inTransitsize_arr, color='green', label='Size of the set inTransit vs time')
    # ax1.set_xlabel('Time')
    # ax1.set_ylabel('Size of the set inTransit', color='green')
    # ax1.tick_params(axis='y', labelcolor='green')

    # ax2 = ax1.twinx()

    # ax2.plot(inTransitsizetime, constantIntransit , color='orange', label='inTransitSize vs time')
    # ax2.set_ylabel('inTransitSize', color='orange')
    # ax2.tick_params(axis='y', labelcolor='orange')
    # ax2.set_ylim(0, max(inTransitsize_arr))

    # ax1.legend(loc='upper left')
    # ax2.legend(loc='upper right')

    # plt.title("Plot of size of set inTransit against time")

    # plt.show()