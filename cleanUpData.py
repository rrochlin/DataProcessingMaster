import logging
import pandas as pd
import re
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data-cleanup")
logger.propagate = True

def cleanUp(cutoff, timeRectifyingParams, filePaths, columns, badTimes, date, confirmedFiles):

    fData = {}
    mod = {}
    cleaningCutOffTime = pd.Timestamp(cutoff)
    filesChecked = {}
    for idx, file in enumerate(filePaths):
        logger.debug(f"filename: {file}")
        # This function will fix utc timestamp errors
        try:
            if file not in confirmedFiles:
                filesChecked[file] = fixUTCStamps(file, date)
        except IndexError as e:
            logging.exception(f"skipping {file} due to index error {e}")
            '''
            this index error will trigger due to a file being empty, i.e. not having
            a 3rd row. We will keep the file marked as not checked, so it is always skipped.
            Users should manually remove the file from database
            '''
            filesChecked[file] = False
            continue
        '''
        Here we are reading in the data from the sensors. 
        if 'all' was put into the columns variable we just
        take everything.
        '''
        if 'all' in columns:
            df = pd.read_csv(
                file,
                header=1,
                parse_dates=[[0, 1]]
            ).dropna(how='all')
        else:
            df = pd.read_csv(
                file,
                header=1,
                parse_dates=[[0, 1]],
                usecols=columns
            ).dropna(how='all')

        logger.debug(df)

        # columns have spaces in front and in between for the merged Data Time column
        df.columns = df.columns.str.replace(" ","")

        # This assumes file start with ./Data\\{sensorName} followed by either "-" or "_"
        sensorNamePattern = r"Data\\[a-zA-Z]+\d+"
        nameMatch = re.search(sensorNamePattern, file)
        name = nameMatch[0].replace("Data\\","")

        logger.debug(df)

        '''
        Some of the time stamps will error when pandas is parsing them to datetime, which causes
        the entire column to be left as a string. This will check to see if the Date_Time column is
        still strings, if so it will manually scan for the known bad stamps and remove them with their
        associatted data.
        '''

        if type(df['Date_Time'][0]) == type('string'):
            for time in badTimes:
                df.drop(df[df['Date_Time'] == time].index, inplace=True)
            # df.drop(df[df['Date_Time'] == '     0/0/0      0:0:0'].index, inplace = True)
            # df.drop(df[df['Date_Time'] == '2165/165/165 165:165:85'].index, inplace = True)
            try:
                df['Date_Time'] = pd.to_datetime(df['Date_Time'])
            except:
                df = autoFix(file, df)
                df['Date_Time'] = pd.to_datetime(df['Date_Time'])

            '''
            Here we need to set up our time changing parameters
            For this instance we need to roll back all sensors by 1 hour
            except the two BU sensors which needed to be rolled back by
            8 hours.
            '''
        try:
            offset = timeRectifyingParams[name]
            mod[name] = 'yes'
            df['Date_Time'] = df['Date_Time']-pd.Timedelta(hours=offset)
        except KeyError:
            mod[name] = 'no'

        try:
            df.drop(df[df['Date_Time'] < cleaningCutOffTime].index, inplace=True)

        except TypeError:
            logger.exception('TypeError: ')
            return file, df
            '''
            In the instance of a TypeError occuring, we are bascically dealing with
            the 0 timestamps causing an error in the read_csv parser and not
            converting the Date_Time column to timestamp data type.
            '''

        fData[name] = df.reset_index(drop=True)

        # ends by logger the new start and stop times of the data sets
    for label in fData:
        try:
            logger.info(
                f"{label}:   {fData[label]['Date_Time'].iloc[0]}    {fData[label]['Date_Time'].iloc[-1]}     mod:{mod[label]}")
        except:
            logger.exception(f"{label}: NO DATA PRESENT    NO DATA PRESENT")
    return fData, filesChecked


def fixUTCStamps(filePath, date):
    '''
    This function will parse the text array and convert any timestamps in YY/MM/dd format
    to YYYY/MM/dd format while setting back the time by 7 hours for UTC -> PST timezone
    if not in PST change the offset
    '''
    offset = 7
    incorrectString = r"   \d{2}/\d+/\d+"
    fin = open(filePath, 'rt')
    content = fin.readlines()
    fin.close()
    '''
    parse the data array to get position of the time data in the text file
    content[3] is the 3rd row of the data frame, which should be the first nonheader row
    text file is expected to be ordered as follows:

    Breakout-05
          Date,      Time,   Battery,  Fix,       Latitude,     etc....
     2022/4/13,   11:58:8,  3.988750,    0,       0.000000,     etc....
     2022/4/13,  11:58:18,  3.988750,    0,       0.000000,     etc....
     2022/4/13,  11:58:28,  3.987500,    0,       0.000000,     etc....
     2022/4/13,  11:58:38,  3.983750,    0,       0.000000,     etc....
    '''
    charTimeStart, charTimeEnd = re.search(r",\s+\d+:\d+:\d+",content[3]).span()
    fout = open(filePath, 'wt')
    for idx, i in enumerate(content):
        match = re.match(incorrectString, i)
        if match:
            # date should be in format MM-dd-YY, this method will work until 2100
            year = date.split('-')[-1]
            dateStamp = match[0].replace(year,f"20{year}")
            line = (pd.Timestamp(dateStamp+i[charTimeStart+1:charTimeEnd])-pd.Timedelta(
                hours=offset)).strftime(' %Y/%m/%d, %H:%M:%S') + i[charTimeEnd:]
        else:
            line = i
        fout.write(line)
    fout.close()
    return True


def autoFix(file, df, start=0):
    indexErrors = {}
    '''
    recursively loops through the dataframe dropping values that cause errors keeping track
    of its current index.
    In retrospec I am curious if just dropping the value and then resetting the index after
    the loop finishes would have the same result as this.
    '''

    for idx, i in enumerate(df['Date_Time'][start:]):
        try:
            pd.Timestamp(i)
        except:
            logger.exception('Error encountered when parsing: ', file)
            logger.exception('first index', idx, '  ', 'time value \''+i+'\'')
            logger.debug(len(df['Date_Time']))
            indexErrors[idx] = i
            df.drop(df[df['Date_Time'] == i].index, inplace=True)
            df.reset_index(drop=True)
            df = autoFix(file, df, idx)
            break
    return df