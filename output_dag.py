from datetime import datetime, timedelta
from airflow.decorators import dag, task


import csv, sqlite3
import pandas as pd
import numpy as np
import os

con = sqlite3.connect("churn_data.db")
telco_data = pd.read_sql_query("SELECT * from telco_data_labeled", con)

con.close()
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

@dag(default_args=default_args, schedule_interval=None, start_date=datetime(2022, 1, 24), catchup=False)
def n2d_dag():
    """
    Notebook to DAG Demo
    """
    
    
    @task
    def data_ingest_task():
        
        telco_data_csv = pd.read_csv('https://raw.githubusercontent.com/fletchjeff/COPML-1_telco_churn/main/data/WA_Fn-UseC_-Telco-Customer-Churn-.csv')
            
        telco_data_csv["TotalCharges"] = pd.to_numeric(telco_data_csv["TotalCharges"],errors='coerce')
            
        telco_data_csv = telco_data_csv.replace({'SeniorCitizen': {1: 'Yes', 0: 'No'}})
            
        telco_data_csv = telco_data_csv.replace(r'^\s$', np.nan, regex=True).dropna().reset_index().drop(columns=['index'])
            
        con = sqlite3.connect("churn_data.db")
            
        telco_data_csv.to_sql("telco_data_labeled",con=con,if_exists='replace',index=False)
            
    @task        
    def model_training_task(data_ingest_task):
        
        import mlflow
        from sklearn.model_selection import GridSearchCV
            
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import classification_report
        from sklearn.preprocessing import OneHotEncoder, StandardScaler
        from sklearn.pipeline import Pipeline
        from sklearn.linear_model import LogisticRegressionCV
        from sklearn.compose import ColumnTransformer
            
        categorical_columns = [
            "gender",
            "SeniorCitizen",
            "Partner",
            "Dependents",
            "PhoneService",
            "MultipleLines",
            "InternetService",
            "OnlineBackup",
            "DeviceProtection",
            "TechSupport",
            "StreamingTV",
            "StreamingMovies",
            "Contract",
            "PaperlessBilling",
            "PaymentMethod"
        ]
        
        numeric_columns = [
            "tenure",
            "TotalCharges",
            "MonthlyCharges"
        ]
            
        transformation_ct = ColumnTransformer(
            [('ohe', OneHotEncoder(), categorical_columns)],#col_index)],
            remainder='passthrough'
        )
            
        transformation_pipeline = Pipeline(
            [
                ('transformation_ct', transformation_ct),
                ('scaler', StandardScaler())
            ]
        )
            
        import sqlite3
        import pandas as pd
        con = sqlite3.connect("churn_data.db")
            
        telco_data = pd.read_sql_query("SELECT * from telco_data_labeled", con)
            
        X_data = transformation_pipeline.fit_transform(telco_data[categorical_columns+numeric_columns])
        y_label = telco_data["Churn"]
        X_data_train, X_data_test, y_label_train, y_label_test = train_test_split(
            X_data, y_label, random_state=42
        )
            
        from sklearn.model_selection import GridSearchCV
        grid_values = {'Cs':[10, 1000], 'cv': [5,10], 'max_iter':[100,150]}
        
        churn_model = LogisticRegressionCV()
        churn_model_gridsearch = GridSearchCV(churn_model, param_grid=grid_values,n_jobs=-1)
        
            
        #mlflow.set_tracking_uri("http://192.168.1.100:5000")
        mlflow.set_experiment("test_notebook")
        mlflow.sklearn.autolog() 
            
        mlflow.start_run()
        churn_model_gridsearch.fit(X_data_train,y_label_train)
        latest_run = mlflow.active_run().info.run_id
        mlflow.end_run()
            
    @task        
    def model_deployment_task(model_training_task):
        
        import mlflow
        logged_model = 'runs:/{}/best_estimator'.format(latest_run)
        
        # Load model as a PyFuncModel.
        loaded_model = mlflow.pyfunc.load_model(logged_model)
            
        import sqlite3
        import pandas as pd
        con = sqlite3.connect("churn_data.db")
            
        telco_data_unlabeled = pd.read_sql_query("SELECT * from telco_data_unlabeled", con)
            
        from joblib import dump, load
        transformation_pipeline = load("transformer.joblib")
            
        predictions = loaded_model.predict(transformation_pipeline.transform(telco_data_unlabeled).reshape(-1,43))
            
        telco_data_unlabeled['Churn_Prediction'] = predictions.tolist()
            
        telco_data_unlabeled.to_sql("telco_data_predicted",con=con,if_exists='replace',index=False)
        con.close()
            
    
    data_ingest_task_return = data_ingest_task()
        
    model_training_task_return = model_training_task(data_ingest_task_return)
        
    model_deployment_task_return = model_deployment_task(model_training_task_return)
        
    return model_deployment_task_return

n2d_dag_run = n2d_dag()