#import oracledb
from sqlalchemy import create_engine, Integer, String, Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT
import pandas as pd

df = pd.read_csv("/Users/ramazanyildirim/Desktop/Loan Prediction Dataset.csv")

# Oracle uyumlu dtype eşleştirmesi
dtype_mapping = {
    "Loan_ID": String(20),
    "Gender": String(20),
    "Married": String(20),
    "Dependents": String(20),
    "Education": String(20),
    "Self_Employed": String(20),
    "ApplicantIncome": Integer(),
    "CoapplicantIncome": Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), "oracle"),
    "LoanAmount": Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), "oracle"),
    "Loan_Amount_Term": Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), "oracle"),
    "Credit_History": Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), "oracle"),
    "Property_Area": String(20),
    "Loan_Status": String(20)
}

# Oracle bağlantısı için engine (thin mode ile çalışır)
engine = create_engine("oracle+oracledb://SYSTEM:Oracle123@localhost:1521/?service_name=XE")


# DataFrame'i Oracle'a yaz
df.to_sql('loanprediction', engine, if_exists="replace", index=False, dtype=dtype_mapping)

