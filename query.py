from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient('localhost', 27017)
db = client.DataBase
DataScience = db.DataScience
FinMastersInvesting = db.FinMastersInvesting
FinMastersCredit = db.FinMastersCredit

twomonthsago = datetime.today()-timedelta(days = 60)
twoagomonth = str(twomonthsago.month)
twomonthago2 = f"{twomonthsago.year}-{twoagomonth}-{twomonthsago.day}"

threemonthsago = datetime.today()-timedelta(days = 90)
threeagomonth = str(threemonthsago.month)
threemonthago2 = f"{threemonthsago.year}-{threeagomonth}-{threemonthsago.day}"


print("Number of articles about technology during the last 2 months: ",DataScience.count_documents({"date": {"$gt": twomonthago2}}), "\n")

print("Number of articles about finance during the last 2 months: ",FinMastersInvesting.count_documents({"date": {"$gt": twomonthago2}})+FinMastersCredit.count_documents({"date": {"$gt": twomonthago2}}), "\n\n")

print("Number of articles about technology : ",DataScience.count_documents({}), "\n")

print("Number of articles about finance : ",FinMastersInvesting.count_documents({})+FinMastersCredit.count_documents({}), "\n\n")

print("Total number of articles that are less than 3 months old : ",DataScience.count_documents({"date": {"$gt": threemonthago2}})+FinMastersInvesting.count_documents({"date": {"$gt": threemonthago2}})+FinMastersCredit.count_documents({"date": {"$gt": threemonthago2}}), "\n\n")

print("Total number of articles where the type is 'Réseau' or the author is 'Carlos Vilhena': ",DataScience.count_documents({"$or": [{"metaData.type": "Réseau"}, {"metaData.author":"Carlos Vilhena"}]})+FinMastersInvesting.count_documents({"$or": [{"metaData.type": "Réseau"}, {"metaData.author":"Carlos Vilhena"}]})+FinMastersCredit.count_documents({"$or": [{"metaData.type": "Réseau"}, {"metaData.author":"Carlos Vilhena"}]}))


















