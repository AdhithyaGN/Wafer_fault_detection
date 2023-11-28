import pandas as pd


class data_getter:

    def __init__(self,log_file,logger_object):
        self.training_file='Training_filefromDB/inputfile.csv'
        self.log_file=log_file
        self.logger_object=logger_object


    def get_gata(self):


        self.logger_object.log(self.log_file,"Entered the get_data method of Data_getter class")

        try:
            self.data=pd.read_csv(self.training_file)
            self.logger_object.log(self.log_file,"Data Loaded Successful.Exited the get_data method from data_getter class ")
            return self.data
            




        except Exception as e:
            self.logger_object.log(self.log_file,"Error happening while reading csv check the error :%s" %e)
            self.logger_object.log(self.log_file,"Data Load Unsuccessful.Exited the get_data method from Data_Getter class")
            raise e

            


            