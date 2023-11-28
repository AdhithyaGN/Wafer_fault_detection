import pandas as pd
from application_logging import logger
import os
from os import listdir




class Data_Transformation:
    def __init__(self):
        self.goodDataPath="Training_raw_files_validated/Good_Raw"
        self.logger=logger.App_logger()



    def replaceMiisingWithNull(self):

        log_file=open("Training_Logs/DataTransformLog.txt","a+")
        try:
            files=[f for f in listdir(self.goodDataPath)]
            for file in files:
                csv=pd.read_csv(self.goodDataPath+"/"+file)
                csv.fillna("NULL",inplace=True)
                csv['Wafer']=csv["Wafer"].str[6:]
                csv.to_csv(self.goodDataPath+"/"+file,index=None)
                self.logger.log(log_file,"File Transformed Successfully!! %s" % file)
        except Exception as e:
            self.logger.log(log_file,"Data Transformation failed Because :: %s" %e)
            log_file.close()
            raise e
        
        log_file.close()
        
