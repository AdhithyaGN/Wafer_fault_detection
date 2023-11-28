import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import roc_auc_score,accuracy_score
import mlflow
import mlflow.sklearn
from Data_preprocessing.preprocessing import preprocessor
from application_logging.logger import App_logger


data_path="Training_filefromDB/inputfile.csv"
df=pd.read_csv(data_path)

log_obj=App_logger()
log_file=open("Training_Logs/mlflow_appLog.txt",'a+')


preprocessor_obj=preprocessor(log_file=log_file,log_obj)

df=preprocessor_obj.remove_columns(df,['wafer'])
X,Y=preprocessor_obj.Seperate_label_feature(df,'Output')
isnull=preprocessor_obj.is_null_present(X)

if (isnull):
    X=preprocessor_obj.impute_missing_values(X)


droplist=preprocessor_obj.get_columns_with_zero_std(X)

X=preprocessor_obj.remove_columns(droplist)

X_train,X_test,Y_train,Y_test=train_test_split(X,Y,test_size=0.33,random_state=42)

Random_model=RandomForestClassifier()
param_grid={"n_estimators":[10,50],
                             "criterion":['gini','entropy'],
                             'max_depth':range(2,4,1),'max_features':['sqrt']}
grid_cv=GridSearchCV(Random_model,param_grid=param_grid,scoring='accuracy',cv=5)



grid_cv.fit(X_train,Y_train)


best_model=grid_cv.best_estimator_
y_pred=best_model.predict(X_test)

accuracy=accuracy_score(Y_test,y_pred)
roc_auc=roc_auc_score(Y_test,y_pred)




with mlflow.start_run():
    mlflow.log_param("n_estimators",best_model.get_params(['n_estimators']))
    mlflow.log_param("criterion",best_model.get_params()['criterion'])
    mlflow.log_param("max_depth",best_model.get_params()['max_depth'])

    mlflow.log_metric("accuracy",accuracy)
    mlflow.log_metric("Roc-auc-score",roc_auc)


    mlflow.sklearn.log_model(best_model)


