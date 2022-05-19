import os
import glob
from time import perf_counter
import yaml
import logging
import re
from datetime import datetime, timedelta

dirname = os.path.dirname(__file__)
if not os.path.exists(os.path.join(dirname,"..","..","dataInfo")):
    os.mkdir(os.path.join(dirname,"..","..","dataInfo"))
logging.basicConfig(
    filename=os.path.join(dirname,"..","..","dataInfo","yamlGenLog.log"),
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    force = True)
logger = logging.getLogger("genYamlParams")
logger.propagate = True


def main():
    conditionDictionary = getConditions()
    allFiles = ' '.join(glob.glob(os.path.join(dirname,'..','Data',"*.txt")))
    logger.info(allFiles)
    datePattern = r"(?<=\w\d[-,_])\d{1,2}\S\d{1,2}\S\d{1,2}(?=\.)"
    multiple_date_pattern = r"(?<=\w\d[-,_])\d{1,2}\S\d{1,2}\S\d{1,2}\S\d{1,2}(?=\.)"
    datesInData = {match.replace("_","-") for match in re.findall(datePattern,allFiles)}
    multiple_dates_in_data = []
    file_paths_for_multi_dates = {}
    for date in re.findall(multiple_date_pattern, allFiles):
        [start, end] = date.split('_')
        [start_month, start_day] = start.split('-')
        [end_month, end_day] = end.split('-')
        start_date = datetime(datetime.today().year, int(start_month), int(start_day))
        end_date = datetime(datetime.today().year, int(end_month), int(end_day))
        multiple_dates_in_data.extend([day.strftime("%m-%d-%y") for day in date_range(start_date, end_date)])
        file_paths_for_multi_dates.update({day.strftime("%m-%d-%y"):date for day in date_range(start_date, end_date)})
    multiple_dates_in_data = set(multiple_dates_in_data)
    datesInYaml = set(conditionDictionary["Days"].keys())
    missingDates = [dataDate for dataDate in datesInData if dataDate not in datesInYaml]
    logger.info(missingDates)
    if missingDates:
        missingObjects = [genSampleObj(date) for date in missingDates]
        logger.info(f"dates added to yaml: {missingDates}")
        for obj in missingObjects:
            conditionDictionary["Days"].update(obj)
        logger.info("overwriting yaml parameter file with new params")
        with open(os.path.join(dirname,"dataCleaningParams.yaml"), 'w') as outfile:
            yaml.dump(conditionDictionary, outfile, default_flow_style=False)
    
    missing_multiple_dates = [dataDate for dataDate in multiple_dates_in_data if dataDate not in datesInYaml]
    if missing_multiple_dates:
        missingObjects = [gen_multi_date_obj(key,file_paths_for_multi_dates[key]) for key in file_paths_for_multi_dates]
        logger.info(f"dates added to yaml: {missing_multiple_dates}")
        for obj in missingObjects:
            conditionDictionary["Days"].update(obj)
        logger.info("overwriting yaml parameter file with new params")
        with open(os.path.join(dirname,"dataCleaningParams.yaml"), 'w') as outfile:
            yaml.dump(conditionDictionary, outfile, default_flow_style=False)
    return


def date_range(start, end):
    delta = end - start  # as timedelta
    days = [start + timedelta(days=i) for i in range(delta.days + 1)]
    return days


def genSampleObj(date):
    return {date: {"confirmedFiles" : [], "filePattern": ["..","Data",f"*{date}.txt"], "processed": {}}}


def gen_multi_date_obj(date, date_range):
    return {date: {"confirmedFiles" : [], "filePattern": ["..","Data",f"*{date_range}.txt"], "processed": {}}}


def getConditions():
    '''
    yaml file should contain the specific variables for the data cleaning:
    filePattern, sensorConditions, columns, DataChecking, preCursorFactor, particle
    '''
    with open(os.path.join(dirname,"dataCleaningParams.yaml"), "r") as stream:
        try:
            allConditions = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logger.exception(exc)
    return allConditions


if __name__ == "__main__":
    start = perf_counter()
    main()
    end = perf_counter()
    logger.info(end-start)