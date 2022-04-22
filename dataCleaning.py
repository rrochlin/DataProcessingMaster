from msilib.schema import Directory
import pandas as pd
import numpy as np
from datetime import datetime as dt
import os
import glob
from time import perf_counter
import yaml
import logging
import re
import math
from fillDataFrame import fillDf
from cleanUpData import cleanUp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data-cleaning")
logger.propagate = True


# running list of faulty time stamps
badTimes = ['     0/0/0      0:0:0', '2165/165/165 165:165:85']


def main():

    # collect and organise all of the data then make it into nice things
    conditionDictionary = getConditions()
    # remove old logs from previous run
    if os.path.exists("../dataInfo/interpolation_Effect_Log.txt"):
        os.remove("../dataInfo/interpolation_Effect_Log.txt")
    if os.path.exists("../dataInfo/time_Frequency_Error_Log.txt"):
        os.remove("../dataInfo/time_Frequency_Error_Log.txt")

    columns = conditionDictionary["Columns"]
    particle = conditionDictionary["Particle"]
    sensorsWithNonPSTTime = conditionDictionary["sensorConditions"]
    dayStart = conditionDictionary["dayStart"]
    dayEnd = conditionDictionary["dayEnd"]
    processAll = conditionDictionary["processAll"]

    newFilesChecked = True

    for day, condition in conditionDictionary["Days"].items():

        if not processAll:
            if condition["processed"]:
                logger.info(f"skipping {day}, params file indicates data is already processed")
                continue


        filePattern = condition["filePattern"]
        confirmedFiles = condition["confirmedFiles"]
        date = condition["date"]
        start = f"{date.replace('-','/')} {dayStart}"
        end = f"{date.replace('-','/')} {dayEnd}"

        files = glob.glob(filePattern)
        logger.info(f"filenames for {condition}:{files}")
        
        
        data, filesChecked = cleanUp(start, sensorsWithNonPSTTime,
                       files, columns, badTimes, date, confirmedFiles)

        if filesChecked:
            newFilesChecked = True

        checkFileList = list(conditionDictionary["Days"][day]["confirmedFiles"])
        for specificFile, check in filesChecked.items():
            if check:
                checkFileList.append(specificFile)
        conditionDictionary["Days"][day]["confirmedFiles"] = checkFileList

        saveToCSV(f'../proccessedData/{date}', data)

        logger.debug(data)

        checkDataRecordingPerformance(
            data, date, particle, start, end)

        interpDF = interpolateMissingData(
            data, cutOffTime=start, endTime=end, date=date)
        saveToCSV(f'../interpolatedData/{date}', interpDF)

        mergedDataFrame = mergeDataFrames(
            interpDF, particle, start, end)

        saveToCSV(f"../mergedData/",
                  {f"mergedData_{date}": mergedDataFrame})

        # Set process flag to True so that time won't be wasted processing old data
        conditionDictionary["Days"][day]["processed"] = True

    if newFilesChecked:
        logger.info("overwriting yaml parameter file with new params")
        with open('dataCleaningParams.yaml', 'w') as outfile:
            yaml.dump(conditionDictionary, outfile, default_flow_style=False)
    return


def getConditions():
    '''
    yaml file should contain the specific variables for the data cleaning:
    filePattern, sensorConditions, columns, DataChecking, preCursorFactor, particle
    '''
    with open("dataCleaningParams.yaml", "r") as stream:
        try:
            allConditions = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.exception(exc)
    return allConditions


def saveToCSV(directory, data):
    # saving the pandas data frame passed into function as a csv file
    for x in data:
        temp = data[x]
        if not os.path.exists(directory):
            os.makedirs(directory)
        location = os.path.join(directory, x+'.csv')
        temp.to_csv(location, index=False)
    return


def checkDataRecordingPerformance(data, date, particle, start, end):
    '''
    This function scans through the data set and looks for irregularities in the timestamps
    This will give a general idea of the health of the device.
    Also will calculate the percentage of recorded data that is 0, if this count is too high
    an error is likely to have occoured.
    Will also tally the amount of lost time between startTime and endTime, i.e. how long the
    sensor was off during the window where we would like to collect.
    '''

    startTime = pd.Timestamp(start)
    endTime = pd.Timestamp(end)


    directory = f'../dataInfo/'
    if not os.path.exists(directory):
        os.makedirs(directory)

    # fout = open(f'{directory}/time_Frequency_Error_Log.txt', 'wt')
    fout = open(f'../dataInfo/time_Frequency_Error_Log.txt', 'a')
    fout.write(f"{'-'*60}\n{date}\n{'-'*60}\n")
    errors = {}
    errorCount = {}
    # Enter the expected interval here
    interval = 20
    for x in data:
        # errors keeps track of length of each time interval error that occurs
        errors[x] = set(())
        # errorCount keeps track of how many times each time interval error occured
        errorCount[x] = {}
        # counter keeps track of the total time interval errors per sensor
        counter = 0
        # filter the data frame to include only the values we care about
        temp = data[x][(data[x]["Date_Time"]>startTime) & (data[x]["Date_Time"]<endTime)]
        for idx, time in enumerate(temp['Date_Time']):
            try:
                test = (temp['Date_Time'][idx+1] - time) <= pd.Timedelta(seconds=interval)
                if not test:
                    timeErr = temp['Date_Time'][idx+1] - time
                    if str(timeErr.seconds) in errorCount[x]:
                        errorCount[x][str(timeErr.seconds)] += 1
                    else:
                        errorCount[x][str(timeErr.seconds)] = 1
                    errors[x].add(timeErr)
                    counter += 1
            except:
                continue

        timeDeltaStart = pd.Timedelta(seconds = 0)
        timeDeltaEnd = pd.Timedelta(seconds = 0)
        if temp["Date_Time"].iloc[0] > startTime:
            timeDeltaStart = temp["Date_Time"].iloc[0] - startTime
        if temp["Date_Time"].iloc[-1] < endTime:
            timeDeltaEnd = endTime - temp["Date_Time"].iloc[-1]
        timeDeltaDuring = sum([pd.Timedelta(seconds = int(time))*amount for time,amount in errorCount[x].items()], pd.Timedelta(seconds = 0))
        totalTimeLost = timeDeltaDuring+timeDeltaEnd+timeDeltaStart

        
        percentZero = round(
            sum([1 if i == 0 else 0 for i in temp[particle]])/len(temp[particle]), 2)
        # logger.exception(data)
        # display the different types of errors
        listOfErrors = [i.seconds for i in errors[x]]
        autoFrmt = [f"{{:>{round(math.log(error+1,10))+2}}}"if error>10**3 else "{:>4}" for error in listOfErrors]
        frmt = "".join(autoFrmt)

        # display the quantity of each type of error
        listOfErrorQuantity = [errorCount[x][str(i.seconds)] for i in errors[x]]

        logger.info(f"{x}: counter:{counter}, temp: {len(temp)}")
        logger.info(f" {str(round(counter/len(temp)*100,2))}% of data set is over {interval} second recording intervals")
        logger.info(frmt.format(*listOfErrors))
        logger.info(frmt.format(*listOfErrorQuantity))
        logString =f"{x}\n"
        logString+=f" {round(totalTimeLost/(endTime - startTime)*100,2)}%  Time lost -- Total Time Lost : {totalTimeLost}\n"
        logString+=f" {str(round(counter/len(temp)*100, 2))}% of data set is over {interval} second recording intervals\n"
        logString+=f' {percentZero*100}% of data set is 0 for paricle size : {particle}\n'
        logString+=f" during: {timeDeltaDuring}\n start: {timeDeltaStart}\n end: {timeDeltaEnd}\n"
        logString+=f" Time Errors {frmt.format(*listOfErrors)}\n"
        logString+=f" # Observed {frmt.format(*listOfErrorQuantity)}\n"
        fout.write(logString)
        fout.write('\n')

    fout.close()
    return


def interpolateMissingData(data, cutOffTime, endTime, date):
    # fout = open(f'../dataInfo/{date}/interpolation_Effect_Log{date}.txt', 'wt')
    fout = open(f'../dataInfo/interpolation_Effect_Log.txt', 'a')
    interpDF = {}
    fout.write(f"\n{date}\n\n")

    for x in data:
        df = data[x]
        cutoff = 40
        freq = '10S'
        try:
            interpDF[x], accuracy = fillDf(
                df, freq, cutOffTime, endTime, cutoff)
            logger.info(f"{x}     {accuracy}")
            fout.write(
                x+' ' + '\n' + accuracy[0] + '\n' + accuracy[1] + '\n' + accuracy[2] + '\n\n')
        except IndexError:
            logger.exception(f"{x} NO DATA")
            fout.write(x+'NO DATA'+'\n')
    fout.close()
    return interpDF


def mergeDataFrames(interpDF, particle, start, end):

    length_list = []
    for x in interpDF:
        length_list.append(len(interpDF[x]))
    lowValue = min(length_list)
    lowIDX = length_list.index(lowValue)
    columns = list(interpDF.keys())
    mergedData = pd.DataFrame(
        {'Date_Time': interpDF[columns[lowIDX]]['Date_Time']})

    for idx, column in enumerate(columns):
        mergedData[column] = interpDF[column][particle]
    mergedData['Average'] = np.mean(mergedData, axis=1)
    mergedData['Variance'] = np.var(mergedData, axis=1)
    for i in mergedData:
        tempFrame = mergedData.values
        tempList = []
        for idx, x in enumerate(tempFrame):
            try:
                increment = (tempFrame[idx+1] - x)/10
                for count in range(10):
                    tempList.append(x+increment*count)
            except IndexError:
                tempList.append(x)
                continue
        hiResMergedDF = pd.DataFrame(tempList, columns=mergedData.keys())
    return hiResMergedDF


if __name__ == "__main__":
    start = perf_counter()
    main()
    end = perf_counter()
    logger.info(end-start)