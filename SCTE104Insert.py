import socket
import csv
import sys
import time
import datetime
from datetime import timedelta
import select
import threading
import warnings

# Ignore specific deprecation warning by category
warnings.filterwarnings("ignore", category=DeprecationWarning)

#Threading lock
lock = threading.Lock()

#Global variable for debug
debugMode = False

#Global value for message number
messageNumber = 1

#Global value for how far ahead computer is than encoder
encoderOffset = 0

#global value for currentEncoderTime
currentEncoderTime = 0

#global value for the current offset between the stream time (maybe just encoder time) and the computer time
offset = 0

#global Value for if timecode is included
timeCodeIncluded = False

#Global value for if the end of file is reached
endOfFile = False

#Global value for loop mode
loopMode = False



def incrementMessageNumber():
    """
    Function to increment message number
    Parameters: 
    None
    Returns:
    None
    """
    global messageNumber
    if (messageNumber < 255):
        messageNumber += 1
    else:
        messageNumber = 0



def connectTCP(ip, port):
    """
    Function to connect to the TCP port of the encoder
    Parameters:
    ip(String): The IP address of the encoder
    port(int): The port of the encoder
    Returns:
    encoderSocket (Socket): The socket
    """
    #Create the TCP socket
    encoderSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Get the current time
    timestamp_string = getTimestampString()
    

    
    #Connect to the socket
        
    try:
        encoderSocket.connect((ip, port))
        print(f"[{timestamp_string}] Connected to {ip}:{port}")


    except ConnectionRefusedError:
        print(f"[{timestamp_string}] Connection was refused. Ensure the server is running or check the IP and port.")
    except Exception as e:
        print(f"An error occurred: {e}")

    # Return the socket
    return encoderSocket
   


def sendMessageTCP(socket, message):
    """
    A function to send a message on a given socket
    Parameters:
    socket (Socket): The socket to send on
    messasge (String) The message to send
    Returns:
    code(int): The return code
    """

    binaryData = bytes.fromhex(message)
    socket.send(binaryData)
    # Receive data from the server
   
    data = socket.recv(1024)
    hex_string = ''.join([hex(byte)[2:].zfill(2) for byte in data])
    if debugMode:
        # Get the current time
        timestamp_string = getTimestampString()
        print(f"[{timestamp_string}] Received from server: {hex_string}")
    
    #increment message number
    incrementMessageNumber()
    #return the code 
    return(hex_string)
    

def waitForResponse(timeOut, socket, hexMessageNumber, index, eventID):
    """
    Function to wait for a Splice Inject Complete Response
    Parameters: 
    timeOut(int): The milliseconds before timeout
    socket(Socket): The socket to listen on
    hexMessageNumber(String): The message number to wait for
    index(int): The index of the message
    eventID(int): The eventID of the message
    """
    # Set the start time in milliseconds
    startTime = time.time() * 1000

    try:
        while True:
            # Calculate the elapsed time in milliseconds
            elapsedTime = (time.time() * 1000) - startTime
            
            # Check if the timeout duration has elapsed
            if elapsedTime >= timeOut:
                # Get the current time
                timestamp_string = getTimestampString()
                print(f"\n[{timestamp_string}] Timeout occurred for Splice Insert {index} (Event ID: {eventID}). Inject Complete Response Not Received.")
                break  # Exit loop on timeout
            
            # Wait for incoming data on the socket or until the remaining timeout expires
            remainingTimeout = timeOut - elapsedTime
            readyToRead, _, _ = select.select([socket], [], [], remainingTimeout / 1000)
            
            if socket in readyToRead:
                # Receive data from the socket
                data = socket.recv(1024)
                hex_string = ''.join([hex(byte)[2:].zfill(2) for byte in data])
                substring1 = hex_string[24]
                substring2 = hex_string[25]
                substring = f"{substring1}{substring2}"
                #print(hex_string)
                """
                print(f"Substring = {substring.upper()}")
                print(f"messageNo = {hexMessageNumber.upper()}")
                print(hexMessageNumber == substring)
                """
                #check if it starts with 0008 and has the message number correct
                if ((hex_string.startswith("0008")) and (substring.upper() == hexMessageNumber.upper())):
                    # Get the current time
                    timestamp_string = getTimestampString()
                    if debugMode:
                        print(f"\n[{timestamp_string}] Pattern found: {hex_string}")
                    print(f"[{timestamp_string}] Inject Complete Response Received - Splice Successfully Executed")
                    break  # Exit loop if the pattern is found
            
        else:
            print("TIMEOUT. Splice Insert not complete")
    except Exception as e:
        print(f"An error occurred: {e}")


def sendAliveRequest(socket, asIndex, pidIndex):
    """
    A function to send an alive message on a given socket
    Parameters:
    socket (Socket): The socket to send on
    asIndex(int): The AS index
    pidIndex(int): The PID index
    Returns:
    None
    """
    #convert AS Index to hex
    hexASIndex = hex(asIndex)[2:].zfill(2)
    #convert message number to hex
    hexMessageNumber = hex(messageNumber)[2:].zfill(2)
    #Convert PID index to hex
    hexPIDIndex = hex(pidIndex)[2:].zfill(4)
    #Create the message - REFER TO ANSI_SCTE1042017 Table 8-1
    #OP ID for Alive Request
    opId = "0003"
    #Message Size always 13 + 8 (for the time data)
    messageSize = "0015"
    #Result always FFFF
    result = "FFFF"
    #Result Extension always FFFF
    resultExt = "FFFF"
    #protocol version always 00
    protocol = "00"
    
    #time data, 4 bytes for seconds past Jan6 1980, 4 bytes for milliseconds
    # Define the start date and time (January 6, 1980, 12:00 AM UTC)
    startDate = datetime.datetime(1980, 1, 6, 0, 0, 0)
    # Get the current date and time
    currentDate = datetime.datetime.utcnow()
    # Calculate the elapsed time
    elapsedTime = currentDate - startDate
    # Get the total elapsed seconds
    elapsedSeconds = int(elapsedTime.total_seconds())
    # Calculate milliseconds separately
    milliseconds = int((elapsedTime.total_seconds() - elapsedSeconds) * 1000)
    #seconds 4 bytes 
    #I CHANGED THIS PLEASE
    secondsHex = hex(elapsedSeconds)[2:].zfill(8)
    #milliseconds 4 bytes
    millisecondsHex = hex(milliseconds)[2:].zfill(8)
    message = (f"{opId}{messageSize}{result}{resultExt}{protocol}{hexASIndex}{hexMessageNumber}{hexPIDIndex}{secondsHex}{millisecondsHex}")
    # Get the current time
    timestamp_string = getTimestampString()
    if debugMode:
        print(f"\n[{timestamp_string}] Sending ALIVE REQUEST Message: {message}")
    else:
        print(f"\n[{timestamp_string}] Sending ALIVE REQUEST Message")
    output = sendMessageTCP(socket, message)
    
    #Get the time data received (last 8 bytes / 16 chars of message)
    timeDataHex = output[-16:]
    secondsDataHex = int(timeDataHex[:8],16)
    #divide by 1000 to convert micro to milliDataHex
    microSeconds = int(timeDataHex[8:16], 16)
    milliSeconds = microSeconds // 1000
    #milliDataHex = int(timeDataHex[8:16],16)
    
    #calculate the total time since Jan 6 1980
    totalMilli = (secondsDataHex*1000)+milliSeconds
    #calculate how far ahead computer is than encoder
    computerAheadTime = (elapsedSeconds*1000 + milliseconds) - totalMilli
    # Get the current time
    timestamp_string = getTimestampString()
    #print encoder time
    encoderTime = convertMsToTime(totalMilli, "UTC")
    #set the current encoder time global variable
    global currentEncoderTime
    currentEncoderTime = encoderTime
    
    print(f"[{timestamp_string}] Encoder time is: {encoderTime}")
    if debugMode:
        computerAheadTimeAsTime = convertMsToTime(computerAheadTime, "UTC")
        print(f"[{timestamp_string}] Computer ahead of encoder by: {computerAheadTime}ms ({computerAheadTimeAsTime})")
    global encoderOffset
    encoderOffset = computerAheadTime
    
    
    #Get the Output Code
    subString = output[8:12]
    outputCode = int(subString, 16)
    #int code
    print(f"[{timestamp_string}] {getResultName(outputCode)}")
    return(outputCode)
    
    
    
def sendInitRequest(socket, asIndex, pidIndex):
    """
    A function to send an init request message on a given socket
    Parameters:
    socket (Socket): The socket to send on
    asIndex(int): The AS index
    pidIndex(int): The PID index
    Returns:
    None
    """
    #convert AS Index to hex
    hexASIndex = hex(asIndex)[2:].zfill(2)
    #convert message number to hex
    hexMessageNumber = hex(messageNumber)[2:].zfill(2)
    #Convert PID index to hex
    hexPIDIndex = hex(pidIndex)[2:].zfill(4)
    #Create the message - REFER TO ANSI_SCTE1042017 Table 8-1
    #OP ID for Init Request
    opId = "0001"
    #Message Size always 13 as no data
    messageSize = "000D"
    #Result always FFFF
    result = "FFFF"
    #Result Extension always FFFF
    resultExt = "FFFF"
    #protocol version always 00
    protocol = "00"
    
    message = (f"{opId}{messageSize}{result}{resultExt}{protocol}{hexASIndex}{hexMessageNumber}{hexPIDIndex}")
    # Get the current time
    timestamp_string = getTimestampString()
    if debugMode:
        print(f"\n[{timestamp_string}] Sending INIT REQUEST Message: {message}")
    else:
        print(f"\n[{timestamp_string}] Sending INIT REQUEST Message")
    output = sendMessageTCP(socket, message)
    
    #Get the Output Code
    subString = output[8:12]
    outputCode = int(subString, 16)
    #int code
    print(f"[{timestamp_string}] {getResultName(outputCode)}")
    return(outputCode)

    
def closeTCP(socket):
    """
    Function to close a given socket
    Parameters:
    socket (SOcket): The socket to close
    Returns:
    None
    """
    if not(socket == None):
        socket.close()
    


def processFile(startTimeHH, startTimeMM, startTimeSS, startTimeFF, inputFile, asIndex, pidIndex, programID, preRoll, socket, messageTime, timeOut):
    """
    Function to process a CSV file and make SCTE 104 packets baswed on the data
    Parameters:
   
    startTimeHH(int): The start hour
    startTimeMM(int): The start minute
    startTimeSS(int): The start second
    startTimeFF(int): The start frame
    inputFile(String): The input CSV file
    asIndex(int): The AS Index
    pidIndex(int): The PID Index
    programID(int): The ID of the program
    preRoll(int): The preRoll time
    socket(Socket): The socket
    messageTime(int): How soon before the event to send the message (ms)
    timeOut(int): The timeout on waiting for a response
    Returns:
    None
    """
    
    with open(inputFile, mode='r') as file:
        csv_reader = csv.reader(file)
        #skip header row, data row, header row
        next(csv_reader)
        next(csv_reader)
        next(csv_reader)
        index = 1
        for row in csv_reader:
            
            
            # get the offset
            global offset
            duration = int(row[0])
            eventID = int(row[1])
            timeVar = row[2]
            availID = int(row[3])
            autoReturn = int(row[4])
            #Split time to get HHMMSSFF
            splitTime = timeVar.split(':')
            hours = int(splitTime[0])
            mins = int(splitTime[1])
            secs = int(splitTime[2])
            frames = int(splitTime[3])
    
            # Get the current date
            current_date = datetime.datetime.now()

            # Extract year, month, and day
            startTimeYYYY = current_date.year
            startTimeMo = current_date.month
            startTimeDD = current_date.day

           
            
            originalDateTime = datetime.datetime(startTimeYYYY, startTimeMo, startTimeDD, hours, mins, secs)
            #If go past midnight, and <13 hours ahead of stream start, increment the days
            if(hours < startTimeHH and hours < ((startTimeHH+13)%24)): 
                oneDay = timedelta(days=1)
                originalDateTime = originalDateTime + oneDay
                
            
            #Adjust time by the offset
            """
            timeD = timedelta(milliseconds = offset)
            resultDateTime = originalDateTime+timeD
            #print(f"Result Date time is: {resultDateTime}")
            """
            resultDateTime = originalDateTime
            
            adjYear = int(resultDateTime.year)
            adjMon = int(resultDateTime.month)
            adjDay = int(resultDateTime.day)
            adjHour = int(resultDateTime.hour)
            adjMin = int(resultDateTime.minute)
            adjSec = int(resultDateTime.second)
            
            
            
            #Create the message - REFER TO ANSI_SCTE1042017 Table 8-2
            
            #Reserved is FFFF
            reserved = "FFFF"
            
            #set message size all components, will always be as timeType known, only one OP, data length known.
            messageSize = "0022"
            
            #Protocol Version is 00
            protocolVersion = "00"
            
            #AS Index as hex
            hexASIndex = hex(asIndex)[2:].zfill(2)
            
            #convert message number to hex
            hexMessageNumber = hex(messageNumber)[2:].zfill(2)
            
            #Convert PID index to hex
            hexPIDIndex = hex(pidIndex)[2:].zfill(4)
            
            #SCTE 35 Protocol Version always 00
            scte35Version = "00"
            
            #Timestamp will be of type 2 HH:MM:SS:FFFF
            #timetype = 2
            timeType = "02"
            #Timestamp as HH:MM:SS:FF each with their own 1 byte variable
            """
            hexHours = hex(hours)[2:].zfill(2)
            hexMins = hex(mins)[2:].zfill(2)
            hexSecs = hex(secs)[2:].zfill(2)
            hexFrames = hex(frames)[2:].zfill(2)
            """
            hexHours = hex(adjHour)[2:].zfill(2)
            hexMins = hex(adjMin)[2:].zfill(2)
            hexSecs = hex(adjSec)[2:].zfill(2)
            hexFrames = hex(frames)[2:].zfill(2)
            
            #NumOps will be 1
            numOps = "01"
            
            #OP ID = 0101 for splice request
            opId = "0101"
            
            #Data Length is the length of the data in bytes, for splice_request_data() this is 14 - REFER TO ANSI_SCTE1042017 Table 9-5
            dataLength = "000E"
            
            #DATA
            #Splice Insert Type is always 01
            spliceInsertType = "01"
            
            #Splice Event ID, convert to hex as 4 bytes
            spliceEventID = hex(eventID)[2:].zfill(8)
            
            #programID 
            uniqueProgramID = hex(programID)[2:].zfill(4)
            
            #preRoll time
            preRollTime = hex(preRoll)[2:].zfill(4)
            
            #Break duration
            breakDuration = hex(duration)[2:].zfill(4)
            
            #Avail num
            availNum = hex(availID)[2:].zfill(2)
            
            #Avails Expected = AvailNum
            availsExpected = availNum
            
            #Auto Return 
            autoReturnFlag = hex(autoReturn)[2:].zfill(2)
            
            
            #Create the message
            #timeStamp Message
            timeStampMessage = timeType+hexHours+hexMins+hexSecs+hexFrames
            #Data Message
            dataMessage = spliceInsertType+spliceEventID+uniqueProgramID+preRollTime+breakDuration+availNum+availsExpected+autoReturnFlag
            
            message = (reserved+messageSize+protocolVersion+hexASIndex+hexMessageNumber+hexPIDIndex+scte35Version+timeStampMessage+numOps+opId+dataLength+dataMessage)
            #LOGIC FOR ONLY SENDING MESSAGETIME BEFORE
            
            #Get the current EPOCH time
            currentTime = time.time()
            currentTimeMilliseconds = int(currentTime * 1000)
            
    
            #Get the required splice time in ms
            
            # Create a datetime object for the given date and time
            originalDateTime = originalDateTime
            
            # Calculate the difference between the given time and Unix epoch
            epoch = datetime.datetime(1970, 1, 1)
            timeDifference = originalDateTime - epoch
            # Convert the time difference to milliseconds
            insertTimeMilliseconds = int(timeDifference.total_seconds() * 1000)
            
            #calculate time from now until then adjusted by offset
            #calculate time between now and insert time
            timeToLive = insertTimeMilliseconds - currentTimeMilliseconds
            
            #adjust by offset (offset is how far ahead encoder is)
            timeToLive = timeToLive + offset
            
            #insert message at messageTime before message
            timeToWait = timeToLive - messageTime
            
            # Get the current time
            timestamp_string = getTimestampString()
            
            #Get the STRINGS of the splice Time
            adjHourString = str(adjHour).zfill(2)
            adjMinString = str(adjMin).zfill(2)
            adjSecString = str(adjSec).zfill(2)
            framesString = str(frames).zfill(2)
            
 
            
            
            #first check if message cannot be sent in time
            if timeToWait < 0:
                print(f"\n[{timestamp_string}] Splice Request: {index} EventID: {eventID} Splice Time: {adjHourString}:{adjMinString}:{adjSecString}:{framesString} in the past. Not sending Splice Request.")
            else:
            
                print(f"\n[{timestamp_string}] Waiting {timeToWait}ms until time {datetime.datetime.utcfromtimestamp((currentTimeMilliseconds+timeToWait)//1000)} to send Splice Request {index} ({adjHourString}:{adjMinString}:{adjSecString}:{framesString})")
                time.sleep(timeToWait/1000)
                
                #acquire lock
                lock.acquire()
                
                #send the splice insert message after the offset.
                # Get the current time
                timestamp_string = getTimestampString()
                if debugMode:
                    print(f"\n[{timestamp_string}] Sending SPLICE INSERT Message: {index} EventID: {eventID} Splice Time: {adjHourString}:{adjMinString}:{adjSecString}:{framesString}  : "+message)
                else:
                    print(f"\n[{timestamp_string}] Sending SPLICE INSERT Message: {index} EventID: {eventID} Splice Time: {adjHourString}:{adjMinString}:{adjSecString}:{framesString}")
                output = sendMessageTCP(socket, message)
                
                
                
                #Get the Output Code
                subString = output[8:12]
                outputCode = int(subString, 16)
                # Get the current time
                timestamp_string = getTimestampString()
                #int code
                print(f"[{timestamp_string}] {getResultName(outputCode)}")
                #wait for the response
                if outputCode == 100:
                    print(f"[{timestamp_string}] Waiting Timeout: {timeOut}ms for Splice Complete Response")
                    waitForResponse(timeOut, socket, hexMessageNumber, index, eventID)
                # Release the lock 
                lock.release()
            #increment index
            index += 1
            
        global endOfFile
        endOfFile = True
 
 
 
#Result code is index 8-11 on a single operation (bytes 5&6)
def getResultName(resultCode):
    """
    Function to return the result name based on the result code - REFER TO ANSI_SCTE1042017 Table 14-1
    """
    resultTable = {
        100: 'Successful Response',
        101: 'Access Denied-Injector not authorized for DPI service',
        102: 'CW index does not have Code Word',
        103: 'DPI has been de-provisioned',
        104: 'DPI not supported',
        105: 'Duplicate service name',
        106: 'Duplicate service name is OK',
        107: 'Encryption not supported',
        108: 'Illegal shared value of DPI PID index found',
        109: 'Inconsistent value of DPI PID index found',
        110: 'Injector is already in use',
        111: 'Injector is not provisioned to service this AS',
        112: 'Injector Not Provisioned For DPI',
        113: 'Injector will be replaced',
        114: 'Invalid Message Size',
        115: 'Invalid Message Syntax',
        116: 'Invalid Version',
        117: 'No fault found',
        118: 'Service name is missing',
        119: 'Shared value of DPI PID index not found',
        120: 'Splice Request Failed – Unknown Failure',
        121: 'Splice Request Is Rejected Bad splice_request parameter',
        122: 'Splice Request Was Too Late – pre-roll is too small',
        123: 'Time type unsupported',
        124: 'Unknown Failure',
        125: 'Unknown opID',
        126: 'Unknown value for DPI_PID_index',
        127: 'Version Mismatch',
        128: 'Proxy Response',
        132: 'Splice Request Response'
    }

    return resultTable.get(resultCode, 'Result name not found')



def calculateTimeOffset(startTimeHH, startTimeMM, startTimeSS, startTimeFF):
    """
    A function to calculate the offset of times between the stream start time and the encoder time in milliseconds
    Parameters:
    startTimeHH(int): The stream start hours
    startTimeMM(int): The stream start mins
    startTimeSS(int): The stream start secs
    startTimeFF(int): The stream start frames
    Returns:
    offset(int): The offset in ms
    """
    #Get the current EPOCH time
    currentTime = time.time()
    currentTimeMilliseconds = int(currentTime * 1000)
    #adjust by the encoder offset if stream time included
    """
    global timeCodeIncluded
    if (timeCodeIncluded):
        global encoderOffset
        currentTimeMilliseconds = currentTimeMilliseconds - encoderOffset
    """
    
    #Get the EPOCH time of the start stream
    #get the current year, month and day
    # Get the current date
    current_date = datetime.datetime.now()

    # Extract year, month, and day
    startTimeYYYY = int(current_date.year)
    startTimeMo = int(current_date.month)
    startTimeDD = int(current_date.day)
    """
    # Formatting month and day to have leading zeros if necessary
    startTimeMo = int(f"{startTimeMo:02d}")
    startTimeDD = int(f"{startTimeDD:02d}")
    """
    #convert frames into ms
    framesMS = int((startTimeFF/25)*1000)
    # Create a datetime object for the given date and time
    givenDatetime = datetime.datetime(startTimeYYYY, startTimeMo, startTimeDD, startTimeHH, startTimeMM, startTimeSS, framesMS)

    # Calculate the difference between the given time and Unix epoch
    epoch = datetime.datetime(1970, 1, 1)
    timeDifference = givenDatetime - epoch

    # Convert the time difference to milliseconds
    streamTimeMilliseconds = int(timeDifference.total_seconds() * 1000)
    
    #Calculate offset (how far AHEAD the computer is)
    global offset
    offset = currentTimeMilliseconds - streamTimeMilliseconds
    #print(f"calc offset {currentTimeMilliseconds} - {streamTimeMilliseconds}")
    if debugMode:
        # Get the current time
        
        timestamp_string = getTimestampString()
        offsetAsTime = convertMsToTime(offset, "EPOCH")
        if offset<0:
            operator = "-"
        else:
            operator = ""
        #global timeCodeIncluded
        if (timeCodeIncluded):
            print(f"[{timestamp_string}] Stream ahead of Computer by {offset}ms ({operator}{offsetAsTime})")
    return(offset)
    
    
    
def convertMsToTime(milliseconds, start):
    """
    Function to convert milliseconds to HH:MM:SS
    Parameters: 
    milliseconds(int): The amount of milliseconds to convert
    start(String): The start time, either EPOCH or UTC
    Returns:
    time(String): The time in HH:MM:SS
    """
   
    #Jan 1 1970
    if(start == "EPOCH"):
        
        dateSince = datetime.datetime(1970, 1, 1)
        secondsSince = milliseconds / 1000
        #if negative, take away from 24 hours
        if(milliseconds < 0):
            secondsSince = 86400 - secondsSince
        currentDate = dateSince+timedelta(seconds=secondsSince)
        
        time = currentDate.strftime('%H:%M:%S')
         
        
    else:
        #Jan 6 1980
        dateSince = datetime.datetime(1980, 1, 6)
        secondsSince = milliseconds / 1000
        #if negative, take away from 24
        if(milliseconds < 0):
            secondsSince = 86400 - secondsSince
        currentDate = dateSince+timedelta(seconds=secondsSince)
        time = currentDate.strftime('%H:%M:%S')
    return(time)
    


def gatherData(inputFile):
    """
    Function to gather all relevant data to run the program from the input file
    Parameters:
    None
    Returns:
    data : Carries (preRoll, programID, asIndex, messageTime, timeout)
    """
    #Gather data
    with open(inputFile, mode='r') as file:
        csv_reader = csv.reader(file)
        #skip header row
        next(csv_reader)
        line = next(csv_reader)
        preRoll = int(line[0])
        programID = int(line[1])
        asIndex = int(line[2])
        messageTime = int(line[3])
        timeOut = int(line[4])
        if (len(line) == 6):
            
   
            startTime = line[5]
            #Split time to get HHMMSSFF
            splitTime = startTime.split(':')
            startTimeHH = int(splitTime[0])
            startTimeMM = int(splitTime[1])
            startTimeSS = int(splitTime[2])
            startTimeFF = int(splitTime[3])
            #Return all the data
            data = {
            'preRoll':preRoll,
            'programID':programID,
            'asIndex':asIndex,
            'messageTime':messageTime,
            'timeOut':timeOut,
            'timeIncluded':True,
            'startTimeHH':startTimeHH,
            'startTimeMM':startTimeMM,
            'startTimeSS':startTimeSS,
            'startTimeFF':startTimeFF
            }
        else:
            
     
            #Return all the data
            data = {
            'preRoll':preRoll,
            'programID':programID,
            'asIndex':asIndex,
            'messageTime':messageTime,
            'timeOut':timeOut,
            'timeIncluded':False,
            }
    return data
 

    
 
 
 
 
def getTimestampString():
    """
    Function to get the timestamp string
    Parameters: 
    None
    Returns
    timeStampString(String)
    """
    # Get the current time
    current_time = datetime.datetime.now().time()

    # Format the time as HH:MM:SS string
    timeStampString = current_time.strftime('%H:%M:%S')
    return(timeStampString)
    
    
    
def poll(socket, asIndex, pidIndex, previousEncoderTime, inputFile, programID, preRoll, messageTime, timeOut, startTimeComputer, startTimeStream):
    """
    Function to poll every 5 seconds with an alive request to get the encoder time and update the offset, checking if file needs to be rerun.
    Parameters:
    socket (Socket): The socket to send on
    asIndex(int): The AS index
    pidIndex(int): The PID index
    inputFile(String): The input file
    programID(int): The program ID
    preRoll(int): The pre roll
    messageTime(int): How long before to send the message
    timeOut(int): The timeout
    startTime(int): The ms in the day that the stream started for the computer
    Returns:
    None
    """
    #Give the program time to start
    time.sleep(0.5)
    lock.acquire()
    global offset
    global currentEncoderTime
    sendAliveRequest(socket, asIndex, pidIndex)
    #if time code not included - use the current encoder time
    #if time code included - use the current stream time
    global timeCodeIncluded
    if not(timeCodeIncluded):
        encoderTimeStringSplit = currentEncoderTime.split(":")
        startTimeHH = int(encoderTimeStringSplit[0])
        startTimeMM = int(encoderTimeStringSplit[1])
        startTimeSS = int(encoderTimeStringSplit[2])
        startTimeFF = 00
        #get time offset
        offset = calculateTimeOffset(startTimeHH,startTimeMM,startTimeSS,startTimeFF)
    else:
        
        #offset is how far ahead encoder is of stream, just need the current stream time
        #get current stream time by adding time since start 
        #time since start is currentMs - startTimeComputer
        currentTime = datetime.datetime.now().strftime('%H%M%S%f')[:-3]  # Get time in HHMMSSMS format (milliseconds)
        hours = int(currentTime[0:2])
        minutes = int(currentTime[2:4])
        seconds = int(currentTime[4:6])
        milliseconds = int(currentTime[6:])

        # Calculating the total milliseconds
        currentTimeMs = (hours * 3600000) + (minutes * 60000) + (seconds * 1000) + milliseconds
        
        timeSinceStart = currentTimeMs - startTimeComputer
        #currentStreamTime is startTimeStream + timeSinceStart
        currentStreamTimeMs = startTimeStream + timeSinceStart
        #convert to HH,MM,SS,FF
        hours = currentStreamTimeMs // (1000 * 60 * 60)
        remaining_milliseconds = currentStreamTimeMs % (1000 * 60 * 60)

        minutes = remaining_milliseconds // (1000 * 60)
        remaining_milliseconds %= (1000 * 60)

        seconds = remaining_milliseconds // 1000
        milliseconds = remaining_milliseconds % 1000
        frames = int(milliseconds/1000 * 25)
        
        offset = calculateTimeOffset(hours, minutes, seconds, frames)
        
    lock.release()
    #If we are in loop mode, do the loop code
    
    global loopMode
    if loopMode:
        #print("checking loops")
        #if new encoder time is less than previous (loop has happened) && end of file reached, redo the file as there will be looped ones still to do
        global endOfFile
        if(currentEncoderTime < previousEncoderTime and endOfFile and (not(timeCodeIncluded))):
            timeStampString = getTimestampString()
            print(f"\n[{timeStampString}] Encoder Looped - Processing File Again")
            endOfFile = False
            processFile(startTimeHH, startTimeMM,startTimeSS,startTimeFF, inputFile, asIndex, pidIndex, programID, preRoll, socket, messageTime, timeOut)
    
    
    # Call the function again after x seconds
    threading.Timer(9.5, poll, args=(socket, asIndex, pidIndex, currentEncoderTime, inputFile, programID, preRoll, messageTime, timeOut, startTimeComputer, startTimeStream)).start()  # Change 5.0 to your desired interval



if __name__ == "__main__":
    #get data from args
    inputFile = sys.argv[1]
    ip = sys.argv[2]
    if not(inputFile.endswith(".csv")):
        inputFile = inputFile + ".csv"
    port = int(sys.argv[3])
    pidIndex = int(sys.argv[4])
    argFive = sys.argv[5]
    loopMode = argFive
    
    #debug mode
    if len(sys.argv) > 6:
        argSix = sys.argv[6]
    else:
        argSix = None
    if not(argSix == None):
        #global debugMode
        debugMode = True
    
    
    
    #Gather Data
    data = gatherData(inputFile)
    #set global variable
    if(data['timeIncluded']):
        timeCodeIncluded = True
    
    #Connect to TCP
    socket = connectTCP(ip, port)
    #send Init Request
    sendInitRequest(socket,data['asIndex'], pidIndex)
    
    #send heartbeat, if alive process the file.
    if(sendAliveRequest(socket, data['asIndex'], pidIndex) == 100):
        #get start time of the computer in ms
        current_time = time.time()  # Get current time in seconds since epoch
        startTime = int((current_time % 86400) * 1000)  # Calculate milliseconds since midnight
        
        
        
        if(data['timeIncluded'] == False):
            thread = threading.Thread(target=poll, args=(socket, data['asIndex'], pidIndex, currentEncoderTime, inputFile, data['programID'], data['preRoll'], data['messageTime'], data['timeOut'], startTime, 0))
            thread.daemon = True
            thread.start()
            #poll(socket, data['asIndex'], pidIndex, currentEncoderTime, inputFile, data['programID'], data['preRoll'], data['messageTime'], data['timeOut'], startTime, 0)
            
            #get the encoder time as the start time
            #global currentEncoderTime
            
            encoderTimeStringSplit = currentEncoderTime.split(":")
            startTimeHH = int(encoderTimeStringSplit[0])
            startTimeMM = int(encoderTimeStringSplit[1])
            startTimeSS = int(encoderTimeStringSplit[2])
            startTimeFF = 00
            #get time offset
            offset = calculateTimeOffset(startTimeHH, startTimeMM,startTimeSS,startTimeFF)
            processFile(startTimeHH, startTimeMM,startTimeSS,startTimeFF, inputFile, data['asIndex'], pidIndex, data['programID'], data['preRoll'], socket, data['messageTime'], data['timeOut'])
            #thread.join()
        else:
            #get the start time of the stream in ms.
            startTimeStream = int(data['startTimeHH']*60*60*1000 + data['startTimeMM']*60*1000 + data['startTimeSS']*1000 + (data['startTimeFF']/25)*1000)
            thread = threading.Thread(target=poll, args=(socket, data['asIndex'], pidIndex, currentEncoderTime, inputFile, data['programID'], data['preRoll'], data['messageTime'], data['timeOut'], startTime, startTimeStream))
            thread.daemon = True
            thread.start()
            #poll(socket, data['asIndex'], pidIndex, currentEncoderTime, inputFile, data['programID'], data['preRoll'], data['messageTime'], data['timeOut'], startTime, startTimeStream)
            #get time offset
            offset = calculateTimeOffset(data['startTimeHH'], data['startTimeMM'],data['startTimeSS'],data['startTimeFF'])
            processFile(data['startTimeHH'], data['startTimeMM'],data['startTimeSS'],data['startTimeFF'], inputFile, data['asIndex'], pidIndex, data['programID'], data['preRoll'], socket, data['messageTime'], data['timeOut'])
            #thread.join()
    
    #continue the running of the program if in loop mode
    if loopMode:
        while True:
            #do nothing
            pass
        
    #closeTCP(socket) 
    
    
    
    
    
    
    
    