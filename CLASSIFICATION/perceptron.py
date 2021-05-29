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
import pandas as pd
from pathlib import Path
from sklearn.linear_model import Perceptron
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, precision_recall_curve, roc_curve, accuracy_score


def classification_scores(y_preds, y_true, target_names, plot = False) :
    class_rep = classification_report(y_true, y_preds, target_names=target_names)
    print(class_rep)
    #if plot:
    #   print('hereher')
    return class_rep

def multiclassROC(Y_test, y_score, n_classes, plot = False) :
    precision = dict()
    recall = dict()
    average_precision = dict()
    for i in range(n_classes):
        precision[i], recall[i], _ = precision_recall_curve(Y_test[i], y_score[i])
        average_precision[i] = average_precision_score(Y_test[i], y_score[i])

  # A "micro-average": quantifying score on all classes jointly
    precision["micro"], recall["micro"], _ = precision_recall_curve(Y_test.ravel(), y_score.ravel())
    average_precision["micro"] = average_precision_score(Y_test, y_score, average="micro")
    print('Average precision score, micro-averaged over all classes: {0:0.2f}'.format(average_precision["micro"]))
  
    if plot :
        colors = cycle(['navy', 'turquoise', 'darkorange', 'cornflowerblue', 'teal'])
        plt.figure(figsize=(7, 8))
        f_scores = np.linspace(0.2, 0.8, num=4)
        lines = []
        labels = []
        for f_score in f_scores:
            x = np.linspace(0.01, 1)
            y = f_score * x / (2 * x - f_score)
            l, = plt.plot(x[y >= 0], y[y >= 0], color='gray', alpha=0.2)
            plt.annotate('f1={0:0.1f}'.format(f_score), xy=(0.9, y[45] + 0.02))

        lines.append(l)
        labels.append('iso-f1 curves')
        l, = plt.plot(recall["micro"], precision["micro"], color='gold', lw=2)
        lines.append(l)
        labels.append('micro-average Precision-recall (area = {0:0.2f})'''.format(average_precision["micro"]))

        for i, color in zip(range(n_classes), colors):
            l, = plt.plot(recall[i], precision[i], color=color, lw=2)
            lines.append(l)
            labels.append('Precision-recall for class {0} (area = {1:0.2f})'''.format(i, average_precision[i]))

        fig = plt.gcf()
        fig.subplots_adjust(bottom=0.25)
        plt.xlim([0.0, 1.0])
        plt.ylim([0.0, 1.05])
        plt.xlabel('Recall')
        plt.ylabel('Precision')
        plt.title('Extension of Precision-Recall curve to multi-class')
        plt.legend(lines, labels, loc=(0, -.38), prop=dict(size=14))


        plt.show()

        plt.figure(2)
        plt.step(recall['micro'], precision['micro'], where='post')

        plt.xlabel('Recall')
        plt.ylabel('Precision')
        plt.ylim([0.0, 1.05])
        plt.xlim([0.0, 1.0])
        plt.title('Average precision score, micro-averaged over all classes: AP={0:0.2f}'.format(average_precision["micro"]))
        plt.savefig('multiclassroc_logreg.png')
    
def confusion_matrix_metric(y_true, y_pred, targetnames, plot = False) :
    matrix = confusion_matrix(y_true, y_pred)
    print(matrix)
    disp = ConfusionMatrixDisplay(confusion_matrix=matrix, display_labels=targetnames)
    disp.plot() 
    disp.savefig('confuion_matrix_logreg.png')

if __name__ == '__main__':
    X_train = pd.read_csv('../DATA_MATRICES/THE_REAL_ONE_X_TRAIN.csv')
    X_test = pd.read_csv('../DATA_MATRICES/THE_REAL_ONE_X_TEST.csv')
    y_train = pd.read_csv('../DATA_MATRICES/THE_REAL_ONE_Y_TRAIN_CLASSIFICATION.csv')
    y_test = pd.read_csv('../DATA_MATRICES/THE_REAL_ONE_Y_TEST_CLASSIFICATION.csv')
    clf = Perceptron(random_state=0, max_iter=10000, n_jobs=4)
    clf.fit(X_train, y_train)
    predictions = clf.predict(X_test)
    score_num = clf.score(X_test, y_test)
    print('PREDICTIONS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print(predictions)
    print('SCORE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print(score_num)

    target_names = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']

    class_rep = classification_scores(predictions, y_test, target_names, True)
    save_file = Path('class_rep_perceptron.txt')
    save_file.write_text(class_rep)
    # write class_rep to a file... 
    #multiclassROC(predictions, y_test, 10, True)
    # want to save plot somewhere
    #confusion_matrix(y_test, predictions, target_names, True)
    # want to save plot somewhere 