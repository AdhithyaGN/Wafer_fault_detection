import numpy as np
import pandas as pd
from sklearn.impute import KNNImputer


class preprocessor:


    def __init__(self,log_file,logger_object):
        self.log_file=log_file
        self.logger_object=logger_object



    def remove_columns(self,data,column_names):


        self.logger_object.log(self.log_file,"Entered the remove columns method of the preprocessor class ")
        self.data=data
        self.columns=column_names

        try:
            self.useful_data=self.data.drop(labels=self.columns,axis=1)
            self.logger_object.log(self.log_file,"Column Removed Successful.Exited the remove_columns method from preprocessor class")
            return self.useful_data
        
        except Exception as e:
            self.logger_object.log(self.log_file,"Exception occured in remove_columns method of the preprocessor class .Exception message: "+str(e))
            self.logger_object.log(self.log_file,"Column Removal Unsucessfull.Exited the remove_columns method from preprocessor class")
            
            raise e
        


    def Seperate_label_feature(self,data,label_column_name):
        

        self.logger_object.log(self.log_file,"Entered Seperate_label_feature method in preprocessor class")

        try:
            self.X=data.drop(labels=label_column_name,axis=1)
            self.Y=data[label_column_name]

            self.logger_object.log(self.log_file,"Seperated features and labels,Exited Seperate_label_feature method from preprocessor class")
            return self.X,self.Y
        
        except Exception as e:
            self.logger_object.log(self.log_file,"Exception occured in Seperate_label_feature method of the preprocessor class .Exception message: "+str(e))
            self.logger_object.log(self.log_file,"Seperating Labels and features  Unsucessfull.Exited the remove_columns method from preprocessor class")
            raise e
        

    def is_null_present(self,data):

        self.logger_object.log(self.log_file,"Entered the is_null_present method of preprocessing class")

        self.is_null=False
        try:
            self.null_counts=data.isna().sum()
            for i in self.null_counts:
                if i>0:
                    self.is_null=True
                    break
            if (self.is_null):
                dataframe_with_null=pd.DataFrame()
                dataframe_with_null['columns']=data.columns
                dataframe_with_null['missing_value_count']=np.asarray(data.isna().sum())
                dataframe_with_null.to_csv('preprocessing_data/null_values.csv')

            self.logger_object.log(self.log_file,'Finding Missing values is a sucess,Data Written to null_values file.Exited is_null_present method from preprocessing class')
            return self.is_null
        except Exception as e:
            self.logger_object.log(self.log_file,"Exception occured in is_null_present method of the preprocessor class .Exception message: "+str(e))
            self.logger_object.log(self.log_file,"Checking is_null_present  Unsucessfull.Exited the is_null_present method from preprocessor class")
            raise e
    

    def impute_missing_values(self,data):

        

        self.logger_object.log(self.log_file,"Entered the impute_missing_values method of preprocessing class")
        self.data=data
        try:
            imputer=KNNImputer(n_neighbors=3,weights='uniform',missing_values=np.nan)
            self.new_array=imputer.fit_transform(self.data)
            self.new_data=pd.DataFrame(data=self.new_array,columns=self.data.columns)
            self.logger_object.log(self.log_file,"Missing Values Imputed .Exited the impute_missing_values method from preprocessor class")
            return self.new_data
        except Exception as e:
            self.logger_object.log(self.log_file,"Exception occured in impute_missing_values method of the preprocessor class .Exception message: "+str(e))
            self.logger_object.log(self.log_file,"Imputing Missing Values is failed.Exited the is_null_present method from preprocessor class")
            raise e
        
    def get_columns_with_zero_std(self,data):
        try:




            self.logger_object.log(self.log_file,"Entered get_columns_with_zero_std method of preprocessor class")
            self.data_col=data.columns
            self.data_describe=data.describe()
            self.to_drop=[]
            for i in self.data_col:
                if (self.data_describe[i]['std']==0):

                    self.to_drop.append(i)
                    self.logger_object.log(self.log_file,"Find columns having zero std deviation and columns list .Exited the get_columns_with_zero_std method from preprocessor class")    
            return self.to_drop
        
        except Exception as e:
            self.logger_object.log(self.log_file,"Exception occured in get_columns_with_zero_std :%s" %e)
            self.logger_object.log(self.log_file," get_columns_with_zero_std is failed.Exited the get_columns_with_zero_std method from preprocessor class")
            raise e

        





