import os
import glob
from time import perf_counter
import yaml
import logging
import re

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
    datePattern = r"(?<=-)\d{1,2}\S\d{1,2}\S\d{1,2}|(?<=_)\d{1,2}\S\d{1,2}\S\d{1,2}"
    datesInData = {match.replace("_","-") for match in re.findall(datePattern,allFiles)}
    datesInYaml = set(conditionDictionary["Days"].keys())
    missingDates = [dataDate for dataDate in datesInData if dataDate not in datesInYaml]
    if missingDates:
        missingObjects = [genSampleObj(date) for date in missingDates]
        logger.info(f"dates added to yaml: {missingDates}")
        for obj in missingObjects:
            conditionDictionary["Days"].update(obj)
        logger.info("overwriting yaml parameter file with new params")
        with open(os.path.join(dirname,"dataCleaningParams.yaml"), 'w') as outfile:
            yaml.dump(conditionDictionary, outfile, default_flow_style=False)

    return


def genSampleObj(date):
    return {date: {"confirmedFiles" : [], "filePattern": ["..","Data",f"*{date}.txt"], "processed": {}}}


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