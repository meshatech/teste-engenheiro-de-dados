import pyodbc 
server = 'localhost' 
database = 'master' 
username = 'sa' 
password = 'Your_password123' 
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='
    +database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()