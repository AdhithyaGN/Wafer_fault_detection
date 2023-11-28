import os
import pickle
import shutil


class file_operations:


    def __init__(self,file_object,logger_object):
        self.log_file=file_object
        self.logger_object=logger_object
        self.model_directory='models/'



    def save_model(self,model,file_name):
        self.logger_object.log(self.log_file,"Entered the save model method inside file_operations class")
        try:
            path=os.path.join(self.model_directory,file_name)
            if os.path.isdir(path):
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path+'/'+file_name+'.sav','wb') as f:
                pickle.dump(model,f)
            self.logger_object.log(self.log_file,"Model saved sucessfully in %s" %path)
            

            return 'success'
        except Exception as e:
            self.logger_object.log(self.log_file,"Model saving met some error ,kindly look forward in %s " %e)
            self.logger_object.log(self.log_file,"Exiting save model function in file operations class without saving the model")
            raise e




