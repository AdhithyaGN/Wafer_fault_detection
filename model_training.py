from Data_Ingestion import data_loader
from Data_preprocessing import preprocessing,clustering
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger
from sklearn.model_selection import train_test_split



class TrainModel():


    def __init__(self):
        self.log_file=open("Training_Logs/ModelTrainingLog.txt","a+")
        self.log_obj=logger.App_logger()


    def trainingModel(self):
        self.log_obj.log(self.log_file,"Start of Training")
        try:
            data_getter=data_loader.data_getter(self.log_file,self.log_obj)
            data=data_getter.get_gata()



            preprocessor=preprocessing.preprocessor(self.log_file,self.log_obj)
            data=preprocessor.remove_columns(data,['Wafer'])

            X,Y=preprocessor.Seperate_label_feature(data,label_column_name="Output")

            is_null_present=preprocessor.is_null_present(X)

            if (is_null_present):
                X=preprocessor.impute_missing_values(X)


            cols_to_drop=preprocessor.get_columns_with_zero_std(X)
            X=preprocessor.remove_columns(X,cols_to_drop)

            kmeans=clustering.Kmeansclustering(self.log_file,self.log_obj)
            number_of_clusters=kmeans.elbow_plot(X)

            X=kmeans.create_clusters(X,number_of_clusters)

            X['labels']=Y

            list_of_Clusters=X['Cluster'].unique()

            for i in list_of_Clusters:
                cluster_data=X[X['Cluster']==i]

                cluster_features=cluster_data.drop(['Cluster','labels'],axis=1)
                cluster_label=cluster_data['labels']


                X_train,X_test,Y_train,Y_test=train_test_split(cluster_features,cluster_label,test_size=0.33,random_state=42)

                model_finder=tuner.Model_finder(self.log_file,self.log_obj)

                best_model_name,best_model=model_finder.get_best_model(X_train,Y_train,X_test,Y_test)

                file_op=file_methods.file_operations(self.log_file,self.log_obj)

                save_model=file_op.save_model(best_model,best_model_name+str(i))

            self.log_obj.log(self.log_file,"Succesfull End Of Training")
            self.log_file.close()

        except Exception as e:
            self.log_obj.log(self.log_file,"Unsuccesfull End of Training")
            self.log_file.close()
            raise e
        




