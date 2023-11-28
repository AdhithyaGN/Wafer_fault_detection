import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations import file_methods



class Kmeansclustering:
    


    def __init__(self,log_file,logger_object):
        self.log_file=log_file
        self.logger_object=logger_object


    def elbow_plot(self,data):


        self.logger_object.log(self.log_file,"Entered the elbow plt method of class kmeansclustering")
        inertialist=[]
        try:
            for i in range(1,11):
                kmeans=KMeans(n_clusters=i,init='k-means++',random_state=42)
                kmeans.fit(data)
                inertialist.append(kmeans.inertia_)


            plt.plot(range(1,11),inertialist)
            plt.title("The Elbow Method")
            plt.xlabel('Number of Clusters')
            plt.ylabel('Within_Cluster SUM of Squared')
            plt.savefig('preprocessing_data/K-Means_Elbow.PNG')


            self.kn=KneeLocator(range(1,11),inertialist,curve='convex',direction='decreasing')
            self.logger_object.log(self.log_file,'The optimum number of cluster is '+str(self.kn.knee)+'.Exited Elbow Method')
            return self.kn.knee
        except Exception as e:
            self.logger_object.log(self.log_file,"Exception occured in elbow plot method of KMeans Clustering.Exception message is  "+str(e))
            self.logger_object.log(self.log_file,"Elbow plot method was unsuccesful.Exited Elbow method in KMeans Clustering class")
            raise e
        


    def create_clusters(self,data,no_of_clusters):


        self.logger_object.log(self.log_file,"Entered Number of clusters method of KmeansClustering")
        self.data=data

        try:

            self.kmeans=KMeans(n_clusters=no_of_clusters,init='k-means++',random_state=42)
            self.clusters=self.kmeans.fit_predict(self.data)

            self.file_op=file_methods.file_operations(self.log_file,self.logger_object)
            self.save_model=self.file_op.save_model(self.kmeans,'Kmeans')
            

            self.data['Cluster']=self.clusters

            self.logger_object.log(self.log_file,"Succesfully created "+str(self.clusters)+" clusters.Exited create clusters method from KMeans clustering method.")
            return self.data
        except Exception as e:
            self.logger_object.log(self.logger_object,"Create Clusters method met an error .Here is the Exception "+ str(e))
            self.logger_object.log(self.logger_object,"Create Cluster method was unsuccessfull.Exited The method from KMeans Clustering.")
            raise e



            


    

        


