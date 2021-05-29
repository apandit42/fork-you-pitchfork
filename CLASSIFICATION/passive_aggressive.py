import numpy as np 
import sklearn as sk
from sklearn import datasets
from sklearn import metrics
from sklearn import svm
from sklearn import linear_model
from sklearn import cluster
from sklearn import model_selection
from sklearn.datasets import load_iris
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.linear_model import PassiveAggressiveClassifier
from sklearn.linear_model import Perceptron
from sklearn.linear_model import RidgeClassifier
import pandas as pd
from pathlib import Path
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, precision_recall_curve, roc_curve, accuracy_score

def classification_scores(y_preds, y_true, target_names, plot = False) :
    class_rep = classification_report(y_true, y_preds, target_names=target_names)
    print(class_rep)
    #if plot:
    #   print('hereher')
    return class_rep

if __name__ == '__main__':
    X_train = pd.read_csv('../DATA_MATRICES/THE_REAL_ONE_X_TRAIN.csv')
    X_test = pd.read_csv('../DATA_MATRICES/THE_REAL_ONE_X_TEST.csv')
    y_train = pd.read_csv('../DATA_MATRICES/THE_REAL_ONE_Y_TRAIN_CLASSIFICATION.csv')
    y_test = pd.read_csv('../DATA_MATRICES/THE_REAL_ONE_Y_TEST_CLASSIFICATION.csv')
    clf = PassiveAggressiveClassifier(max_iter=10000, n_jobs=4, random_state=0)
    clf.fit(X_train, y_train)
    predictions = clf.predict(X_test)
    print('SCORE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    num_score = clf.score(X_test, y_test)
    print(num_score)
    target_names = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    class_rep = classification_scores(predictions, y_test, target_names, True)
    save_file = Path('class_rep_passagg.txt')
    save_file.write_text(class_rep)