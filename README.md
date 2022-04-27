# DataProcessingMaster
Template for data processing, so we can use the same process on multiple data sets

# Using this Repo
In order to set up this repo on your local directory create a new branch to work on and clone this into the directory you are working in.

# Expected file structure
|-ProjectFolder                 <---- this is an overarching file to contain the files this project will produce\
|--DataProcessingMaster         <---- this is the repo you will clone\
|----Data                       <---- folder containing all raw data to access\
|-------data1.txt\
|-------data2.txt etc.\
|----project
|-------__init__.py             <---- left blank
|-------Analysis.ipynb          <---- starter template for jupyter notebook analysis
|-------cleanUpData.py          <---- modules for dataCleaning.py. handles ingensting and fixing raw data
|-------dataCleaning.py         <---- base script to run. handles calling cleanUp from cleanUpData.py and parsing yaml params
|-------dataCleaningParams.yaml <---- parameter file for adjusting settings of the script
|-------fillDataFrame.py        <---- handles linearly interpolating or 0 padding data to get us high resolution data
|----README.md                  <---- this is where you are now
|----setup.py                   <---- package info, don't touch

# Version Control
Branches in this project are used to keep mutliple project in line with data processing standards for scalibility
Regularly pull updates from main to keep projects in line with the latest methods
  
# Setting up the project
create a new branch from master to save your specific configuration for a new project, or clone an existing branch to collaborate on an existing project
start a terminal session in the project directory.
install pip globally if you do not have it (run 'pip --version' to see if you have it)
install git globally if you do not have it (run 'git --version' to see if you have it)
run 'git clone {branch_clone_link}'
run 'pip install virtualenv'
create a virtual environemnt with the command 'pythonvirtualenv {environment name}'
activate the virtual environment with './{environment name}/Scripts/activate' on windows or 'source {environment name}/bin/activate' on unix
change directory to dataProcessingMaster with 'cd dataProcessingMaster'
run the command 'pip install -e .' to install all dependencies from setup.py
you're all set up now!

# Using the data processing script
the script expects the user to edit dataCleaningParams.yaml in order to control its behavior
## dataCleaningParams.yaml
Columns:
- 0
- 1
- 6
- 13
Days:
  MM-dd-YY:
    confirmedFiles: []
    date: MM-dd-YY
    filePattern: ['..','Data','*4_13_22.txt']
    processed:
        Dp>0.3: true
        PM2.5_Std: true
Particles:
- Dp>0.3
- PM2.5_Std
dayEnd: '17:00'
dayStart: '10:00'
processAll: true
sensorConditions: {}




