from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score,roc_auc_score



class Model_finder:

    def __init__(self,log_file,logger_object):
        self.log_file=log_file
        self.logger_object=logger_object
        self.clf=RandomForestClassifier()
        self.GBR=GradientBoostingClassifier()





    def best_params_for_RandomForest(self,X_train,Y_train):

        self.logger_object.log(self.log_file,"Entered best_params_for_RandomForest Method of Model_finder")

        try:
            self.param_grid={"n_estimators":[10,50],
                             "criterion":['gini','entropy'],
                             'max_depth':range(2,4,1),'max_features':['sqrt']}
            
            self.grid=GridSearchCV(self.clf,param_grid=self.param_grid,cv=5,verbose=3)
            self.grid.fit(X_train,Y_train)

            self.criterion=self.grid.best_params_['criterion']
            self.n_estimators=self.grid.best_params_['n_estimators']
            self.max_depth=self.grid.best_params_['max_depth']
            self.max_features=self.grid.best_params_['max_features']

            self.clf=RandomForestClassifier(n_estimators=self.n_estimators,criterion=self.criterion,
                                            max_depth=self.max_depth,max_features=self.max_features)
            
            self.clf.fit(X_train,Y_train)

            self.logger_object.log(self.log_file,"Random Forest best params"+str(self.grid.best_params_)+".Exited Best_params_for_randomForest method of Model_finder")

            return self.clf
        
        except Exception as e:
            self.logger_object.log(self.log_file,"Exception occured at best_params_for_RandomForest.Exception is as follows."+str(e))
            self.logger_object.log(self.log_file,"best_params_for_RandomForest method was unsuccesful.Exiting the method")
            raise e
        


    def best_params_for_GradientBoost(self,X_train,Y_train):

        self.logger_object.log(self.log_file,"Entering the best_params_method for GBoost Classifer")

        try:

            self.param_grid={'learning_rate':[0.1,0.01],
                             'max_depth':[3,5],
                             'n_estimators':[10,50]}
            self.grid=GridSearchCV(self.GBR,param_grid=self.param_grid,cv=5,verbose=3)
            self.grid.fit(X_train,Y_train)


            self.learning_rate=self.grid.best_params_['learning_rate']
            self.max_depth=self.grid.best_params_['max_depth']
            self.n_estimators=self.grid.best_params_['n_estimators']



            self.xgb=GradientBoostingClassifier(learning_rate=self.learning_rate,max_depth=self.max_depth,n_estimators=self.n_estimators)
            self.xgb.fit(X_train,Y_train)

            self.logger_object.log(self.log_file,"best params for GBoost is "+str(self.grid.best_params_)+"Exited the get params from GradientBoost Method in Model_finder")

            return self.xgb
        
        except Exception as e:
            self.logger_object.log(self.log_file,'Exception occured at Best params for GBoost occcured .This is the error '+str(e))
            self.logger_object.log(self.log_file,"Gboost parameter tuning is failed.Exited Best Params for GBost method from Model finder")
            raise e
        


    def  get_best_model(self,X_train,Y_train,X_test,Y_test):

        self.logger_object.log(self.log_file,"Entered the get_best_model in Model_finder class")

        try:
            self.gboost=self.best_params_for_GradientBoost(X_train,Y_train)
            self.gboost_prediction=self.gboost.predict(X_test)

            if len(Y_test.unique())==1:
                self.gboost_score=accuracy_score(Y_test,self.gboost_prediction)
                self.logger_object.log(self.log_file,"Accuracy score for GBoost "+str(self.gboost_score))

            else:
                self.gboost_score=roc_auc_score(Y_test,self.gboost_prediction)
                self.logger_object.log(self.log_file,"Roc Auc Score for Gboost "+str(self.gboost_score))
            

            self.RandomForest=self.best_params_for_RandomForest(X_train,Y_train)

            self.RandomForest_predictions=self.RandomForest.predict(X_test)

            if len(Y_test.unique())==1:
                self.RandomForest_score=accuracy_score(Y_test,self.RandomForest_predictions)
                self.logger_object.log(self.log_file,"Accuracy Score for RandomForest "+str(self.RandomForest_score))

            else:
                self.RandomForest_score=roc_auc_score(Y_test,self.RandomForest_predictions)
                self.logger_object.log(self.log_file,"Roc AuC Score is "+str(self.RandomForest_score))


            if (self.RandomForest_score<self.gboost_score):
                return "GBoost",self.gboost
            else:
                return 'RandomForest',self.RandomForest
            
        except Exception as e:
            self.logger_object.log(self.log_file,"Exception occured in get_best_model.Exception message :"+str(e))
            self.logger_object.log(self.log_file,"Model selection Failed.Exited the get_best_model method from Model_Finder class")
            raise e
         



