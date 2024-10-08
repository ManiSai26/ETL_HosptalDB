import mysql.connector
from datetime import datetime
conn = mysql.connector.connect(
    host='localhost',
    user='root',  # Change this to your MySQL username
    password='Password',  # Change this to your MySQL password
    database='hospital_db'
)

cursor = conn.cursor()

# creating staging table if not exists
cursor.execute("""CREATE TABLE IF NOT EXISTS Staging_Customers (
    Name VARCHAR(100),
    Cust_I INT,
    Open_Dt DATE,
    Consul_Dt DATE,
    VAC_ID VARCHAR(100),
    DR_Name VARCHAR(100),
    State VARCHAR(100),
    County VARCHAR(100),
    DOB DATE,
    FLAG CHAR(1),
    age INT,
     days_since_last_consulted CHAR(1)
);""")
def calculate_age(dob):
    # dob = datetime.strptime(dob, '%Y%m%d')
    today = datetime.today()
    return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

def calculate_days_since_last_consult(consult_date):
    # consult_date = datetime.strptime(consult_date, '%Y%m%d')
    today = datetime.today().date()
    delta = today - consult_date
    if delta.days>30:
        return 'Y'
    return 'N'

with open('data.txt') as f:
    for row in f:
        row = row.strip().split('|')
        if(row[1]!='D'):
            continue
        row[4] = datetime.strptime(row[4], "%Y%m%d")
        row[5] = datetime.strptime(row[5], "%Y%m%d")
        row[10] = datetime.strptime(row[10], "%m%d%Y")
        row = tuple(row[2:])
        print(row)
        cursor.execute("""INSERT INTO Staging_Customers (Name, Cust_I, Open_Dt, Consul_Dt, VAC_ID, DR_Name, State, County, DOB, FLAG) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",row)
cursor.execute("SELECT Cust_I, DOB, Consul_Dt FROM Staging_Customers;")
records = cursor.fetchall()

for record in records:
    cust_id = record[0]
    dob = record[1]
    consul_dt = record[2]
    
    age = calculate_age(dob)
    days_since_consult = calculate_days_since_last_consult(consul_dt)
    
    cursor.execute("""
        UPDATE Staging_Customers
        SET age = %s, days_since_last_consulted = %s
        WHERE Cust_I = %s
    """, (age, days_since_consult, cust_id))

cursor.execute("SELECT DISTINCT County FROM Staging_Customers;")
countries = cursor.fetchall()

for country in countries:
    table_name = f"Table_{country[0]}"
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            Name VARCHAR(100),
            Cust_I INT PRIMARY KEY,
            Open_Dt DATE,
            Consul_Dt DATE,
            VAC_ID VARCHAR(100),
            DR_Name VARCHAR(100),
            State VARCHAR(100),
            County VARCHAR(100),
            DOB DATE,
            FLAG CHAR(1),
            age INT,
            days_since_last_consulted CHAR(1)
        );
    """)

for country in countries:
    table_name = f"Table_{country[0]}"
    cursor.execute(f"""
        INSERT INTO {table_name}
        SELECT * FROM Staging_Customers
        WHERE County = %s;
    """, (country[0],))

conn.commit()


cursor.execute("""
    DELETE FROM Staging_Customers
    WHERE Cust_I IN (
        SELECT Cust_I FROM (
            SELECT Cust_I, MAX(Consul_Dt) as Latest_Consul_Dt
            FROM Staging_Customers
            GROUP BY Cust_I
        ) AS LatestRecords
        WHERE Staging_Customers.Consul_Dt != LatestRecords.Latest_Consul_Dt
    );
""")



conn.commit()

cursor.close()
conn.close()