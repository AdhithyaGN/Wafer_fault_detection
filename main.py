from Training_validation_Insertion import TrainValidation
from model_training import TrainModel

from os import listdir


if __name__=="__main__":
    path='Training_files'
    validation_obj=TrainValidation(path)
    validation_obj.train_validation()

    training_obj=TrainModel()
    training_obj.trainingModel()

    
