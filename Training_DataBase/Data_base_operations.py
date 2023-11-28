import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
from application_logging import logger


class DBoperations:


    def __init__(self):
        self.path='Training_Database/'
        self.badFilePath="Training_raw_files_validated/Bad_Raw"
        self.goodFilePath="Training_raw_files_validated/Good_Raw"
        self.logger=logger.App_logger()


    def dataBaseConnection(self,databasename):


        try:
            conn=sqlite3.connect(self.path+databasename+'.db')
            file=open("Training_Logs/DatabaseConnectionLog.txt",'a+')
            self.logger.log(file,"Opened The Database Successfully : %s" %databasename)
            file.close()
        except ConnectionError:
            file=open("Training_Logs/DatabaseConnectionLog.txt",'a+')
            self.logger.log(file,"Error while connecting to database : %s" %ConnectionError)
            file.close()
            raise ConnectionError
        return conn
    

    def createTableinDB(self,databasename,column_names):

        try:
            conn=self.dataBaseConnection(databasename)
            c=conn.cursor()
            c.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='Good_RawData'")
            if c.fetchone()[0]==1:
                conn.close()
                file=open('Training_Logs/createtTableFromDBLog.txt','a+')
                self.logger.log(file,"Tables created successfully :%s" % databasename)
                file.close()

                file=open('Training_Logs/DatabaseConnectionLog.txt','a+')
                self.logger.log(file,"database closed successfully :%s" % databasename)
                file.close()


            else:
                for key in column_names.keys():

                    dtype=column_names[key]
                    try:
                        
                        conn.execute(f'ALTER TABLE Good_RawData ADD COLUMN "{key}" {dtype}')
                    except:
                        conn.execute(f'CREATE TABLE  Good_RawData ("{key}" {dtype})')

                conn.close()


                file=open('Training_Logs/createtTableFromDBLog.txt','a+')
                self.logger.log(file,"Table Created Successfully")
                file.close()

                file=open('Training_Logs/DatabaseConnectionLog.txt','a+')
                self.logger.log(file,"database closed successfully :%s" % databasename)
                file.close()


        except Exception as e:
            file=open('Training_Logs/createtTableFromDBLog.txt','a+')
            self.logger.log(file,"Error while creating table : %s" %e)
            file.close()
            conn.close()
            file=open('Training_Logs/DatabaseConnectionLog.txt','a+')
            self.logger.log(file,"database closed successfully :%s" % databasename)
            file.close()
            raise e
        

    def InsertIntoTableGoodData(self,databasename):
        conn=self.dataBaseConnection(databasename)
        goodfilepath=self.goodFilePath
        badfilepath=self.badFilePath

        filename=[f for f in listdir(goodfilepath)]
        file=open("Training_Logs/DBInsertion.txt","+a")
        for fl in filename:
            try:
                with open(goodfilepath+'/'+fl,'r') as f:
                    next(f)

                    
                
                
                    reader=csv.reader(f,delimiter='\n')
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute(f'INSERT INTO Good_RawData values ({list_})')

                                self.logger.log(file,f"{f} loaded succesfully")
                                conn.commit()

                            except Exception as e:
                                raise e
            except Exception as e:
                conn.rollback()
                self.logger.log(file,"Error While Creating table: %s" %e)
                shutil.move(goodfilepath+"/"+file,badfilepath)
                self.logger.log(file,"file moved Successfully %s" %file)
                file.close()
                conn.close()
        conn.close()
        file.close()

    def selectingdatafromtableintocsv(self,databasename):
        self.filefromDB='Training_filefromDB/'
        self.filename='inputfile.csv'
        file=open("Training_Logs/Exporttocsv.txt",'a+')

        try:
            conn=self.dataBaseConnection(databasename)
            c=conn.cursor()
            c.execute("SELECT * FROM Good_RawData")
            results=c.fetchall()
            headers=[i[0] for i in c.description]

            ##making output directory

            if not os.path.isdir(self.filefromDB):
                os.makedirs(self.filefromDB)
            

            ##opening csv file for writing

            csvFile = csv.writer(open(self.filefromDB + self.filename, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')
            
            ##adding headings and rows to csv file

            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(file,"file exported successfully")
            file.close()

        except Exception as e:
            self.logger.log(file,"file exporting failed.Error : %s" %e)
            file.close()







            


        
        





            
        
        











