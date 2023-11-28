import sqlite3
from datetime import datetime
from os import listdir #### to get a lsit of filname and directory
import os
import re
import json
import shutil  ### performing file operations
import pandas as pd
from application_logging.logger import App_logger


class Raw_data_Validation:



    def __init__(self,path):

        self.batch_directory=path
        self.schema_path='schema_training.json'
        self.logger=App_logger()


        

    def values_from_schema(self):
        try:
            with open(self.schema_path,'r') as f:
                dic=json.load(f)
                f.close()

            pattern=dic["SampleFileName"]
            LengthOfDataStampInFile=dic["LengthOfDateStampInFile"]
            LenghtOfTimeStampInFile=dic["LengthOfTimeStampInFile"]
            column_names=dic["ColName"]
            NumberOFColumns=dic["NumberofColumns"]


            file=open("Training_Logs/ValuesfromSchemaValidationLog.txt",'a+')
            message="LengthOfDataStampInFile:: %s" %LengthOfDataStampInFile +"\t" + "LengthOfTimeStampInFile:: %s" %LenghtOfTimeStampInFile + "\t" + "NumberOfColumns:: %s" %NumberOFColumns +"\n"
            self.logger.log(file,message)

            file.close()

        except ValueError:
            file=open("Training_Logs/ValuesfromSchemaValidationLog.txt",'a+')
            message="ValueError:Value not found inside schema_training.json"
            self.logger.log(file,message)
            file.close()
            raise ValueError
        
        except KeyError:
            file=open("Training_Logs/ValuesfromSchemaValidationLog.txt",'a+')
            message="KeyError:Key value Error incorrect key passed"
            self.logger.log(file,message)
            file.close()
            raise KeyError
        
        except Exception as e:
            file=open("Training_Logs/ValuesfromSchemaValidationLog.txt",'a+')
            message=str(e)
            self.logger.log(file,message)
            file.close()
            raise e
        
        return LengthOfDataStampInFile,LenghtOfTimeStampInFile,column_names,NumberOFColumns
    

    def manualRegexCreation(self):
        regex="['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex
    
    def createDirectoryForGoodBadRawData(self):
        try:
            path=os.path.join("Training_raw_files_validated/","Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path=os.path.join("Training_raw_files_validated/","Bad_Raw")
            if not os.path.isdir(path):
                os.makedirs(path)
 

        except OSError as ex:
            file=open("Training_Logs/General_Log.txt","a+")
            message="OSError: Error while creating Dictionary :  %s" %ex
            self.logger.log(file,message)
            file.close()
            raise ex
        

    def DeleteExistingGoodDataTrainingFolder(self):
        try:
            path='Training_raw_files_validated/'
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file=open("Training_Logs/General_Log.txt","a+")
                message="Goodraw directory deleted successfully"
                self.logger.log(file,message)
                file.close()

        except OSError as S:
            file=open("Training_Logs/General_Log.txt","a+")
            message="Error while deleting directory : %s" %S
            self.logger.log(file,message)
            file.close()
            raise S
    def DeleteExistingBadDataTrainingFolder(self):
        try:
            path='Training_raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file=open("Training_Logs/General_Log.txt","a+")
                message="Badraw directory deleted successfully"
                self.logger.log(file,message)
                file.close()


        except OSError as S:
            file=open("Training_Logs/General_Log.txt","a+")
            message="Error while deleting directory : %s" %S
            self.logger.log(file,message)
            file.close()
            raise S    
        
    def moveBadFilesToArchiveBad(self):

        now=datetime.now()
        date=now.date()
        time=now.strftime("%H%M%S")
        try:

            source="Training_raw_files_validated/Bad_Raw"
            if os.path.isdir(source):
                path="TrainingArchivedBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                    dest='TrainingArchivedBadData/BadData_' + str(date) +"_"+str(time)
                    if not os.path.isdir(dest):
                        os.makedirs(dest)
                    files=os.listdir(source)
                    for f in files:
                        if f not in os.listdir(dest):
                            shutil.move(source+'/'+f,dest)
                    file=open("Training_Logs/General_Log.txt","a+")
                    self.logger.log(file,"Bad files moved to archive")
                    path='Training_raw_files_validated/'
                    if os.path.isdir(path + 'Bad_Raw/'):
                      
                      shutil.rmtree(path + 'Bad_Raw/')
                    file=open("Training_Logs/General_Log.txt","a+")
                    message="Badraw directory deleted successfully"
                    self.logger.log(file,message)
                    file.close()
        except Exception as e:
            file=open("Training_Logs/General_Log.txt","a+")
            message="Error while moving bad raw data to archived directory : %s" %e
            self.logger.log(file,message)
            file.close()
            raise e
    def ValidationFileName(self,regex,LengthOfDataStampInFile,LenghtOfTimeStampInFile):

        self.DeleteExistingBadDataTrainingFolder()
        self.DeleteExistingGoodDataTrainingFolder()


        self.createDirectoryForGoodBadRawData()
        datafiles=[f for f in listdir(self.batch_directory)]
        #print(datafiles)
        try:
            file=open("Training_Logs/nameValidationLog.txt","a+")
            for filename in datafiles:
                if (re.match(regex,filename)):
                    splitadot=re.split('.csv',filename)
                    splitadot=re.split("_",splitadot[0])
                    if len(splitadot[1])==LengthOfDataStampInFile:
                        if len(splitadot[2])==LenghtOfTimeStampInFile:
                            shutil.copy("Training_files/" +filename,"Training_raw_files_validated/Good_Raw")
                            self.logger.log(file,"Valid File Name !! file moved to goodraw folder:: %s" %filename)
                        else:
                            shutil.copy("Training_files/" +filename,"Training_raw_files_validated/Bad_Raw")
                            self.logger.log(file,"InValid File Name !! file moved to Badraw folder:: %s" %filename)
                    else:
                        shutil.copy("Training_files/" +filename,"Training_raw_files_validated/Bad_Raw")
                        self.logger.log(file,"InValid File Name !! file moved to Badraw folder:: %s" %filename)
                else:
                    shutil.copy("Training_files/" +filename,"Training_raw_files_validated/Bad_Raw")
                    self.logger.log(file,"InValid File Name !! file moved to Badraw folder:: %s" %filename)
            file.close()

        except Exception as e:
            file=open("Training_Logs/nameValidationLog.txt","a+")
            self.logger.log(file,"Error occured while validating filename %s" %e)
            file.close()
            raise e
    

    def ValidateColLength(self,NumberOFColumns):
        try:
            f=open("Training_Logs/colLengthValidationLog.txt","a+")
            self.logger.log(f,"col length validation started")
            for file in listdir('Training_raw_files_validated/Good_Raw/'):
                csv=pd.read_csv('Training_raw_files_validated/Good_Raw/'+file)
                if csv.shape[1]==NumberOFColumns:
                    pass
                else:
                    shutil.move("Training_raw_files_validated/Good_Raw/"+file,"Training_raw_files_validated/Bad_Raw")
                    self.logger.log(f,"Invalid Column Length for the file !! file moved to Bad raw Folder:: %s" %file)
            self.logger.log(f,"Column length Validation completed")
        except OSError:
            f=open("Training_Logs/colLengthValidationLog.txt","a+")
            self.logger.log(f,"Errror Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f=open("Training_Logs/colLengthValidationLog.txt","a+")
            self.logger.log(f,"Error Occured :: %s" %e)
            f.close()
            raise e
        
    def ValidateMisiingValuesInWholeColumn(self):
        try:
            f=open("Training_Logs/missingValueCol.txt","a+")
            self.logger.log(f,"Misssing Values Validation Started")
            for file in listdir("Training_raw_files_validated/Good_Raw/"):
                csv=pd.read_csv("Training_raw_files_validated/Good_Raw/"+file)
                count=0
                for col in csv:


                
                    if (len(csv[col])-csv[col].count() == len(csv[col])):
                        
                        
                    

                        count+=1
                        shutil.move("Training_raw_files_validated/Good_Raw/"+file,"Training_raw_files_validated/Bad_Raw")
                        self.logger.log(f,"Zero column spotted !! file moved to Bad_raw Data")
                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("Training_raw_files_validated/Good_Raw/"+file,index=None,header=True)
        except OSError:
            f=open("Training_Logs/missingValueCol.txt","a+")
            self.logger.log(f,"Error Occured while moving the file :: %s" %OSError)
            f.close()
            raise OSError
        except Exception as e:
            f=open("Training_Logs/missingValueCol.txt","a+")
            self.logger.log(f,"Error Occured while moving the file :: %s" %e)
            f.close()
            raise e
        f.close()

                   

                        





    
        

    
    














        










