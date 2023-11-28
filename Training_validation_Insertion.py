from datetime import datetime
from Training_raw_data_validation.rawValidation import Raw_data_Validation
from Training_DataBase.Data_base_operations import DBoperations
from Training_Data_Transformation.Data_Transformation import Data_Transformation
from application_logging.logger import App_logger

class TrainValidation:

    def __init__(self,path):
        self.raw_data=Raw_data_Validation(path)
        self.data_transform=Data_Transformation()
        self.dboperation=DBoperations()
        self.file_log=open("Training_Logs/Training_Main_Log.txt",'a+')
        self.logger=App_logger()


    def train_validation(self):

        try:

            self.logger.log(self.file_log,'start of validation on files')

            ##extracting value training schema
            LengthOfDataStampInFile,LenghtOfTimeStampInFile,column_names,NumberOFColumns=self.raw_data.values_from_schema()


            ##getting the regex
            regex=self.raw_data.manualRegexCreation()

            ##validating file name
            self.raw_data.ValidationFileName(regex=regex,LengthOfDataStampInFile=LengthOfDataStampInFile,LenghtOfTimeStampInFile=LenghtOfTimeStampInFile)
            
            ##validating column length
            self.raw_data.ValidateColLength(NumberOFColumns=NumberOFColumns)

            ###validating if any zero column 
            self.raw_data.ValidateMisiingValuesInWholeColumn()
            
            self.logger.log(self.file_log,'Raw Data Validation completed')


            self.logger.log(self.file_log,"Data transformation started")

            ##replacing blanks in the csv file with 'null values to insert in table
            self.data_transform.replaceMiisingWithNull()

            self.logger.log(self.file_log,"Data transformation completed")


            self.logger.log(self.file_log,"creating training Database and tables on the basis given schema")

            #create database with given name .if present open the connection ! create table with columns given in schema

            self.dboperation.createTableinDB('Training',column_names=column_names)
            self.logger.log(self.file_log,"Table cration completed!!!")

            self.logger.log(self.file_log,"Inserting Data Into table started!!!")
            ##inserting data into table from good data

            self.dboperation.InsertIntoTableGoodData('Training')
            self.logger.log(self.file_log,"Data insertion in database completed")

            self.logger.log(self.file_log,"Deleting Good Data folder")
            ###deleting good data folder after inserting it into database
            self.raw_data.DeleteExistingGoodDataTrainingFolder()
            self.logger.log(self.file_log," Deleting Good data folder")


            self.logger.log(self.file_log,"Moving bad files to archive and deleting bad_data folder")
            ## Moving bad files to archive and deleting bad data folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.logger.log(self.file_log,"deleted Bad raw data after moving it into Archives")
            self.logger.log(self.file_log,"Data avlidation Operation completed")

            self.logger.log(self.file_log,"Extracting csv from database table")
            ##Exporting data from database into csv
            self.dboperation.selectingdatafromtableintocsv('Training')

            self.file_log.close()

        except Exception as e:
            raise e
















    