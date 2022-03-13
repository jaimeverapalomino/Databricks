# Databricks notebook source
def get_cloud():
  with open("/databricks/common/conf/deploy.conf") as f:
    for line in f:
      if "databricks.instance.metadata.cloudProvider" in line and "\"GCP\"" in line: return "GCP"
      elif "databricks.instance.metadata.cloudProvider" in line and "\"AWS\"" in line: return "AWS"
      elif "databricks.instance.metadata.cloudProvider" in line and "\"Azure\"" in line: return "MSA"

#############################################
# TAG API FUNCTIONS
#############################################

# Get all tags
def getTags() -> dict: 
  return sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
    dbutils.entry_point.getDbutils().notebook().getContext().tags()
  )

# Get a single tag's value
def getTag(tagName: str, defaultValue: str = None) -> str:
  values = getTags()[tagName]
  try:
    if len(values) > 0:
      return values
  except:
    return defaultValue

#############################################
# Get Databricks runtime major and minor versions
#############################################

def getDbrMajorAndMinorVersions() -> (int, int):
  import os
  dbrVersion = os.environ["DATABRICKS_RUNTIME_VERSION"]
  dbrVersion = dbrVersion.split(".")
  return (int(dbrVersion[0]), int(dbrVersion[1]))

# Get Python version
def getPythonVersion() -> str:
  import sys
  pythonVersion = sys.version[0:sys.version.index(" ")]
  spark.conf.set("com.databricks.training.python-version", pythonVersion)
  return pythonVersion

#############################################
# USER, USERNAME, AND USERHOME FUNCTIONS
#############################################

def get_cloud():
  with open("/databricks/common/conf/deploy.conf") as f:
    for line in f:
      if "databricks.instance.metadata.cloudProvider" in line and "\"GCP\"" in line: return "GCP"
      elif "databricks.instance.metadata.cloudProvider" in line and "\"AWS\"" in line: return "AWS"
      elif "databricks.instance.metadata.cloudProvider" in line and "\"Azure\"" in line: return "MSA"
              
# Get the user's username
def getUsername() -> str:
  return spark.sql("SELECT current_user()").first()[0]

# Get the user's userhome
def getUserhome() -> str:
  username = getUsername()
  return f"dbfs:/user/{username}/dbacademy"

def getModuleName() -> str: 
  # This will/should fail if module-name is not defined in the Classroom-Setup notebook
  return spark.conf.get("com.databricks.training.module-name")

def getLessonName() -> str:
  # If not specified, use the notebook's name.
  return dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None).split("/")[-1]

def getCourseDir() -> str:
  import re
  moduleName = re.sub(r"[^a-zA-Z0-9]", "_", getModuleName()).lower()
  courseDir = f"{getUserhome()}/{moduleName}"
  return courseDir.replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_")
  
def getWorkingDir() -> str:
  import re
  course_dir = getCourseDir()
  lessonName = re.sub(r"[^a-zA-Z0-9]", "_", getLessonName()).lower()
  workingDir = f"{getCourseDir()}/{lessonName}"
  return workingDir.replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_")


############################################
# USER DATABASE FUNCTIONS
############################################

def getDatabaseName(courseType:str, username:str, moduleName:str, lessonName:str) -> str:
  import re
  langType = "p" # for python
  databaseName = username + "_" + moduleName + "_" + lessonName + "_" + langType + courseType
  databaseName = databaseName.lower()
  databaseName = re.sub("[^a-zA-Z0-9]", "_", databaseName)
  return databaseName.replace("__", "_").replace("__", "_").replace("__", "_").replace("__", "_")


# Create a user-specific database
def createUserDatabase(courseType:str, username:str, moduleName:str, lessonName:str) -> str:
  databaseName = getDatabaseName(courseType, username, moduleName, lessonName)

  spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(databaseName))
  spark.sql("USE {}".format(databaseName))

  return databaseName

    
#############################################
# Legacy testing functions
#############################################

# Test results dict to store results
testResults = dict()

# Hash a string value
def toHash(value):
  from pyspark.sql.functions import hash
  from pyspark.sql.functions import abs
  values = [(value,)]
  return spark.createDataFrame(values, ["value"]).select(abs(hash("value")).cast("int")).first()[0]

# Clear the testResults map
def clearYourResults(passedOnly = True):
  whats = list(testResults.keys())
  for what in whats:
    passed = testResults[what][0]
    if passed or passedOnly == False : del testResults[what]

# Validate DataFrame schema
def validateYourSchema(what, df, expColumnName, expColumnType = None):
  label = "{}:{}".format(expColumnName, expColumnType)
  key = "{} contains {}".format(what, label)

  try:
    actualType = df.schema[expColumnName].dataType.typeName()
    
    if expColumnType == None: 
      testResults[key] = (True, "validated")
      print("""{}: validated""".format(key))
    elif actualType == expColumnType:
      testResults[key] = (True, "validated")
      print("""{}: validated""".format(key))
    else:
      answerStr = "{}:{}".format(expColumnName, actualType)
      testResults[key] = (False, answerStr)
      print("""{}: NOT matching ({})""".format(key, answerStr))
  except:
      testResults[what] = (False, "-not found-")
      print("{}: NOT found".format(key))

# Validate an answer
def validateYourAnswer(what, expectedHash, answer):
  # Convert the value to string, remove new lines and carriage returns and then escape quotes
  if (answer == None): answerStr = "null"
  elif (answer is True): answerStr = "true"
  elif (answer is False): answerStr = "false"
  else: answerStr = str(answer)

  hashValue = toHash(answerStr)
  
  if (hashValue == expectedHash):
    testResults[what] = (True, answerStr)
    print("""{} was correct, your answer: {}""".format(what, answerStr))
  else:
    testResults[what] = (False, answerStr)
    print("""{} was NOT correct, your answer: {}""".format(what, answerStr))

# Summarize results in the testResults dict
def summarizeYourResults():
  html = """<html><body><div style="font-weight:bold; font-size:larger; border-bottom: 1px solid #f0f0f0">Your Answers</div><table style='margin:0'>"""

  whats = list(testResults.keys())
  whats.sort()
  for what in whats:
    passed = testResults[what][0]
    answer = testResults[what][1]
    color = "green" if (passed) else "red" 
    passFail = "passed" if (passed) else "FAILED" 
    html += """<tr style='font-size:larger; white-space:pre'>
                  <td>{}:&nbsp;&nbsp;</td>
                  <td style="color:{}; text-align:center; font-weight:bold">{}</td>
                  <td style="white-space:pre; font-family: monospace">&nbsp;&nbsp;{}</td>
                </tr>""".format(what, color, passFail, answer)
  html += "</table></body></html>"
  displayHTML(html)

# Log test results to a file
def logYourTest(path, name, value):
  value = float(value)
  if "\"" in path: raise AssertionError("The name cannot contain quotes.")
  
  dbutils.fs.mkdirs(path)

  csv = """ "{}","{}" """.format(name, value).strip()
  file = "{}/{}.csv".format(path, name).replace(" ", "-").lower()
  dbutils.fs.put(file, csv, True)

# Load test results from log file
def loadYourTestResults(path):
  from pyspark.sql.functions import col
  return spark.read.schema("name string, value double").csv(path)

# ****************************************************************************
# Utility method to determine whether a path exists
# ****************************************************************************

def pathExists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except:
    return False
  
# ****************************************************************************
# Utility method for recursive deletes
# Note: dbutils.fs.rm() does not appear to be truely recursive
# ****************************************************************************

def deletePath(path):
  files = dbutils.fs.ls(path)

  for file in files:
    deleted = dbutils.fs.rm(file.path, True)
    
    if deleted == False:
      if file.is_dir:
        deletePath(file.path)
      else:
        raise IOError("Unable to delete file: " + file.path)
  
  if dbutils.fs.rm(path, True) == False:
    raise IOError("Unable to delete directory: " + path)

# ****************************************************************************
# Facility for advertising functions, variables and databases to the student
# ****************************************************************************
def allDone(advertisements):
  
  functions = dict()
  variables = dict()
  databases = dict()
  
  for key in advertisements:
    if advertisements[key][0] == "f" and spark.conf.get(f"com.databricks.training.suppress.{key}", None) != "true":
      functions[key] = advertisements[key]
  
  for key in advertisements:
    if advertisements[key][0] == "v" and spark.conf.get(f"com.databricks.training.suppress.{key}", None) != "true":
      variables[key] = advertisements[key]
  
  for key in advertisements:
    if advertisements[key][0] == "d" and spark.conf.get(f"com.databricks.training.suppress.{key}", None) != "true":
      databases[key] = advertisements[key]
  
  html = ""
  if len(functions) > 0:
    html += "The following functions were defined for you:<ul style='margin-top:0'>"
    for key in functions:
      value = functions[key]
      html += f"""<li style="cursor:help" onclick="document.getElementById('{key}').style.display='block'">
        <span style="color: green; font-weight:bold">{key}</span>
        <span style="font-weight:bold">(</span>
        <span style="color: green; font-weight:bold; font-style:italic">{value[1]}</span>
        <span style="font-weight:bold">)</span>
        <div id="{key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">{value[2]}</div>
        </li>"""
    html += "</ul>"

  if len(variables) > 0:
    html += "The following variables were defined for you:<ul style='margin-top:0'>"
    for key in variables:
      value = variables[key]
      html += f"""<li style="cursor:help" onclick="document.getElementById('{key}').style.display='block'">
        <span style="color: green; font-weight:bold">{key}</span>: <span style="font-style:italic; font-weight:bold">{value[1]} </span>
        <div id="{key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">{value[2]}</div>
        </li>"""
    html += "</ul>"

  if len(databases) > 0:
    html += "The following database were created for you:<ul style='margin-top:0'>"
    for key in databases:
      value = databases[key]
      html += f"""<li style="cursor:help" onclick="document.getElementById('{key}').style.display='block'">
        Now using the database identified by <span style="color: green; font-weight:bold">{key}</span>: 
        <div style="font-style:italic; font-weight:bold">{value[1]}</div>
        <div id="{key}" style="display:none; margin:0.5em 0; border-left: 3px solid grey; padding-left: 0.5em">{value[2]}</div>
        </li>"""
    html += "</ul>"

  html += "All done!"
  displayHTML(html)

# ****************************************************************************
# Placeholder variables for coding challenge type specification
# ****************************************************************************
class FILL_IN:
  from pyspark.sql.types import Row, StructType
  VALUE = None
  LIST = []
  SCHEMA = StructType([])
  ROW = Row()
  INT = 0
  DATAFRAME = sqlContext.createDataFrame(sc.emptyRDD(), StructType([]))

# ****************************************************************************
# All done
# ****************************************************************************

displayHTML("Defining courseware-specific utility methods...")

