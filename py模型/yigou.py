# -*- coding: utf-8 -*-
"""
Created on Mon Jul 23 20:44:35 2018

@author: 16040443
"""


import numpy as np
import pandas as pd
from datetime import datetime

train_set_path = 'E:\\ZB\\svm\\yigou_train_15000.csv'
print(datetime.now().strftime('%Y-%m-%D %H:%M:%S') + ' Program Strart...')
print(datetime.now().strftime('%Y-%m-%D %H:%M:%S') + ' loading dataset...')
df = pd.read_csv(train_set_path,sep='\t')
df = df.drop(['member_churn_yigou_train_w.child_age','member_churn_yigou_train_w.reliability_score_age','member_churn_yigou_train_w.reliability_grade_age','member_churn_yigou_train_w.accur_grade_age'],axis=1)

p = 0.7
trainDF = df.iloc[:int(len(df)*p)]
testDF = df.iloc[int(len(df)*p):]

cols_train = list(trainDF.columns)
cols_test = list(testDF.columns)
featStart_Idx = cols_train.index('member_churn_yigou_train_w.visit_amnt_1y')
featEnd_Idx = cols_train.index('member_churn_yigou_train_w.gds_cmnt_1w')
label = 'member_churn_yigou_train_w.whether_churn'

train_feat_cols = cols_train[featStart_Idx:featEnd_Idx+1]
test_feat_cols = cols_train[featStart_Idx:featEnd_Idx+1]
print(datetime.now().strftime('%Y-%m-%D %H:%M:%S') + ' Data TransFormed...')

from sklearn import preprocessing
label_encoder = preprocessing.LabelEncoder()
gender_arr_train = np.array(trainDF.loc[:,'member_churn_yigou_train_w.gender'])
gender_arr_test = np.array(testDF.loc[:,'member_churn_yigou_train_w.gender'])
label_encoder.fit(gender_arr_train)
gender_label_coder_train = np.reshape(label_encoder.transform(gender_arr_train),[len(gender_arr_train),1])
gender_label_coder_test = np.reshape(label_encoder.transform(gender_arr_test),[len(gender_arr_test),1])

onehot_encoder = preprocessing.OneHotEncoder(sparse=False)
onehot_encoder.fit(gender_label_coder_train)
gender_onehot_coder_train = onehot_encoder.transform(gender_label_coder_train)
gender_onehot_coder_test = onehot_encoder.transform(gender_label_coder_test)

label_encoder = preprocessing.LabelEncoder()
level_arr_train = np.array(trainDF.loc[:,'member_churn_yigou_train_w.cust_level_num'])
level_arr_test = np.array(testDF.loc[:,'member_churn_yigou_train_w.cust_level_num'])
label_encoder.fit(level_arr_train)
level_label_coder_train = np.reshape(label_encoder.transform(level_arr_train),[len(level_arr_train),1])
level_label_coder_test = np.reshape(label_encoder.transform(level_arr_test),[len(level_arr_test),1])

onehot_encoder = preprocessing.OneHotEncoder(sparse=False)
onehot_encoder.fit(level_label_coder_train)
level_onehot_coder_train = onehot_encoder.transform(level_label_coder_train)
level_onehot_coder_test = onehot_encoder.transform(level_label_coder_test)

label_encoder = preprocessing.LabelEncoder()
purchase_arr_train = np.array(trainDF.loc[:,'member_churn_yigou_train_w.purchase_power'])
purchase_arr_test = np.array(testDF.loc[:,'member_churn_yigou_train_w.purchase_power'])
label_encoder.fit(purchase_arr_train)
purchase_label_coder_train = np.reshape(label_encoder.transform(purchase_arr_train),[len(purchase_arr_train),1])
purchase_label_coder_test = np.reshape(label_encoder.transform(purchase_arr_test),[len(purchase_arr_test),1])

onehot_encoder = preprocessing.OneHotEncoder(sparse=False)
onehot_encoder.fit(purchase_label_coder_train)
purchase_onehot_coder_train = onehot_encoder.transform(purchase_label_coder_train)
purchase_onehot_coder_test = onehot_encoder.transform(purchase_label_coder_test)

label_encoder = preprocessing.LabelEncoder()
loyalty_arr_train = np.array(trainDF.loc[:,'member_churn_yigou_train_w.loyalty_level'])
loyalty_arr_test = np.array(testDF.loc[:,'member_churn_yigou_train_w.loyalty_level'])
label_encoder.fit(loyalty_arr_train)
loyalty_label_coder_train = np.reshape(label_encoder.transform(loyalty_arr_train),[len(loyalty_arr_train),1])
loyalty_label_coder_test = np.reshape(label_encoder.transform(loyalty_arr_test),[len(loyalty_arr_test),1])

onehot_encoder = preprocessing.OneHotEncoder(sparse=False)
onehot_encoder.fit(loyalty_label_coder_train)
loyalty_onehot_coder_train = onehot_encoder.transform(loyalty_label_coder_train)
loyalty_onehot_coder_test = onehot_encoder.transform(loyalty_label_coder_test)

label_encoder = preprocessing.LabelEncoder()
scor_arr_train = [['AA'],
['BB'],
['CC'],
['other'],
['A'],
['AAA'],
['B'],
['BBB'],
['C']]
scor_arr_test = np.array(testDF.loc[:,'member_churn_yigou_train_w.scor_label'])
label_encoder.fit(scor_arr_train)
scor_label_coder_train = np.reshape(label_encoder.transform(np.array(trainDF.loc[:,'member_churn_yigou_train_w.scor_label'])),[len(np.array(trainDF.loc[:,'member_churn_yigou_train_w.scor_label'])),1])
scor_label_coder_test = np.reshape(label_encoder.transform(scor_arr_test),[len(scor_arr_test),1])

onehot_encoder = preprocessing.OneHotEncoder(sparse=False)
onehot_encoder.fit(scor_label_coder_train)
scor_onehot_coder_train = onehot_encoder.transform(scor_label_coder_train)
scor_onehot_coder_test = onehot_encoder.transform(scor_label_coder_test)
print(datetime.now().strftime('%Y-%m-%D %H:%M:%S') + ' onehotEncode End...')

trainX = trainDF.loc[:,train_feat_cols]
trainY = trainDF.loc[:,label]
testX = testDF.loc[:,test_feat_cols]
trueY = testDF.loc[:,label]

scaler = preprocessing.StandardScaler()
scaler.fit(trainX)
trainX = scaler.transform(trainX)
testX = scaler.transform(testX)
print(datetime.now().strftime('%Y-%m-%D %H:%M:%S') + ' Standard End...')

trainX = np.hstack((gender_label_coder_train,level_onehot_coder_train,purchase_onehot_coder_train,loyalty_onehot_coder_train,scor_onehot_coder_train,trainX))
testX = np.hstack((gender_label_coder_test,level_onehot_coder_test,purchase_onehot_coder_test,loyalty_onehot_coder_test,scor_onehot_coder_test,testX))
'''
time_algorithm_start = datetime.now()
print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' My algorithm Start...')
from sklearn import svm
from sklearn.grid_search import GridSearchCV
svc = svm.SVC(decision_function_shape='ovo',verbose=1)
param_grid = dict(C=[0.001,0.1,1,10],gamma=[0.001,0.01,0.1,1,10])
clf = GridSearchCV(svc,param_grid=param_grid,scoring ='f1_micro',cv=3)
clf.fit(trainX,trainY)
print('grid_scores:',clf.grid_scores_)
print('best_score:',clf.best_score_)
print('best_params_:',clf.best_params_)
clf = clf.best_estimator_
predictY = clf.predict(testX)
time_algorithm_end = datetime.now()
print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' My algorithm End...')
print(' My algorithm Cost %s s' % (time_algorithm_end - time_algorithm_start).seconds)
'''
time_algorithm_start = datetime.now()
print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' My algorithm Start...')
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
clf = GradientBoostingClassifier(max_depth=5,max_features='sqrt',verbose=1)
clf.fit(trainX,trainY)
predictY =clf.predict(testX)
time_algorithm_end = datetime.now()
print(datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' My algorithm End...')
print(' My algorithm Cost %s s' % (time_algorithm_end - time_algorithm_start).seconds)

def acc(predictY,trueY):
    return 1 - sum(abs(predictY - trueY)) / len(predictY)

def precision_score(predictY,trueY):
    return ((predictY==1) * (trueY==1)).sum() / (predictY==1).sum()

def recall_score(predictY,trueY):
    return ((predictY==1) * (trueY==1)).sum() / (trueY==1).sum()

def f1_score(predictY,trueY):
    a = precision_score(predictY,trueY) * recall_score(predictY,trueY) * 2
    b = precision_score(predictY,trueY) + recall_score(predictY,trueY)
    return a/b

from sklearn import metrics
evaluation = {'acc':acc(predictY,trueY),'precision':precision_score(predictY,trueY),'recall':recall_score(predictY,trueY),'f1_score':f1_score(predictY,trueY),'roc_auc':metrics.roc_auc_score(trueY,predictY)}

