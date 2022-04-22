import pandas as pd
import numpy as np


def fillDf(df, freq, start, end, cutoff):

    # if a start time is specified then use it, otherwise use the dataframe values
    if start:
        startTime = pd.Timestamp(start)
    else:
        startTime = df.values[0][0]

    if end:
        endTime = pd.Timestamp(end)
    else:
        endTime = df.values[-1][0] + pd.Timedelta(seconds=freq)

    # variables to keep track of the properties of the data set
    volatility = 0
    padding = 0
    nochange = 0

    # how long between data points we will allow to be interpolated
    threshold = pd.Timedelta(seconds=cutoff)

    # creates a pandas dataframe of time timestamps
    index = pd.date_range(startTime, endTime, freq=freq)

    # keeps track of the index in the date range set we are in
    count = 0

    overall = []

    for idx, row in enumerate(df.values):
        oldCount = count

        try:
            '''
            keep increasing the count until we reach a timestamp in our data frame
            that is greater than or equal to the test frame we created
            row[0] is the timestamp of that row in the data frame, should look like:

            Date_Time  Dp>0.3  Dp>0.5  Dp>1.0  Dp>2.5  Dp>5.0  Dp>10.0
            0  2022-04-13 12:04:14      27       9       3       3       3        0
            1  2022-04-13 12:04:34       9       3       0       0       0        0
            2  2022-04-13 12:04:54      18       6       0       0       0        0
            3  2022-04-13 12:05:14      39      10       0       0       0        0
            4  2022-04-13 12:05:34      39      13       0       0       0        0
            5  2022-04-13 12:05:54      27       9       0       0       0        0
            6  2022-04-13 12:06:14      48      13       0       0       0        0

            index[count] will go through the dataframe we created with the timestamp
            '''
            while row[0] >= index[count]:
                count += 1
        except IndexError:
            continue

        # val is the time since the last entry we recorded divided by the frequency
        val = count - oldCount

        # if sensor measurements are more frequent than the sampling rate we just skip them
        if not val:
            continue

        # Threshold exceeded Case
        # check if the time passed exceeds our threshold
        if threshold < (index[count] - index[oldCount]):
            # take the first row of the df to set to 0
            temp = df.values[0][1:]

            # since the time gap is over the threshold entries are 0 padded instead of interpolated
            for step, ovrwrt in enumerate(range(oldCount, count)):
                padding += 1
                tempdata = np.concatenate(
                    (np.array([index[ovrwrt]]),
                     np.floor(np.array(temp * 0))), 0
                )
                overall.append(tempdata)
            continue

        # Interpolate linearly Case
        # if there have been more than 1 missed value then we need to interpolate
        if val > 1:

            # time gaps < threshold will be linearly interpolated
            if not idx:
                temp = df.values[0][1:]
            else:
                temp = df.values[idx - 1][1:]

            # calculate the slope between the two points for linear interpolation
            inc = (row[1:] - temp) / val

            # create n rows of linearly interpolated data and then add them to overall
            for step, ovrwrt in enumerate(range(oldCount, count)):
                volatility += 1
                tempdata = np.concatenate(
                    (np.array([index[ovrwrt]]), np.floor(
                        np.array(temp + inc * step))),
                    0
                )
                overall.append(tempdata)

        # pass on point to new time stamp case.
        else:
            nochange += 1
            temp = row[1:]
            tempdata = np.concatenate(
                (np.array([index[oldCount]]), np.floor(np.array(temp))), 0
            )
            overall.append(tempdata)

    if overall[-1][0] < index[-1]:
        # take the first row of the df to set to 0
        temp = df.values[0][1:]

        # since the time gap is over the threshold entries are 0 padded instead of interpolated
        for step, ovrwrt in enumerate(range(count, len(index))):
            padding += 1
            tempdata = np.concatenate(
                (np.array([index[ovrwrt]]),
                    np.floor(np.array(temp * 0))), 0
            )
            overall.append(tempdata)

    total = len(overall)

    if total:
        accuracy = ["% of values from interpolation : " + str(np.round(volatility/total*100, 3)),
                    "% of values from 0-padding : " +
                    str(np.round(padding/total*100, 3)),
                    "% of values not changed : " + str(np.round(nochange/total*100, 3))]
    else:
        accuracy = 'NO DATA'

    newDF = pd.DataFrame(overall, columns=df.columns)

    return newDF, accuracy

