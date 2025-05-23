import csv
import sqlite3

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users('../../resources/users.csv')
    load_and_clean_call_logs('../../resources/callLogs.csv')
    write_user_analytics('../../resources/userAnalytics.csv')
    write_ordered_calls('../../resources/orderedCalls.csv')

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    # select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):
    with open(file_path, newline='') as file:
        reader = csv.reader(file)
        next(reader)
        user_id = 1
        for row in reader:
            if len(row) != 2:
                continue
            first, last = row[0].strip(), row[1].strip()
            if first and last:
                cursor.execute(
                    "insert into users (userId, firstName, lastName) values (?, ?, ?)",
                    (user_id, first, last)
                )
                print(user_id, first, last)
                user_id += 1
    conn.commit()

# This function will load the callLogs.csv ee into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):
    with open(file_path, newline='') as file:
        reader = csv.reader(file)
        headers = next(reader)

        for row in reader:
            if (len(row) != 5) or not (row[4]) or not (row[0]) or not (row[1]) or not (row[2]) or not (row[3]):
                continue
            try:
                phone_number = row[0].strip()
                start_time = int(row[1])
                end_time = int(row[2])
                direction = row[3].strip().lower()
                user_id = row[4].strip() 
                
                if not user_id or direction not in ("inbound", "outbound"):
                    continue
                
                cursor.execute("""
                    insert into callLogs (phoneNumber, startTime, endTime, direction, userId)
                    values (?, ?, ?, ?, ?)
                """, (phone_number, start_time, end_time, direction, user_id))
            
            except Exception as e:
                pass


# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):
    with open(csv_file_path, 'w', newline= '') as file:
        writer = csv.writer(file)
        writer.writerow(['userId', 'avgDuration', 'numCalls'])
        cursor.execute('''select userId, round(avg(endTime - startTime), 1) as avgDuration, count(*) as numCalls
                       from callLogs
                       group by userId''')
        
        user_data = cursor.fetchall()

        if not user_data:
            print('NO DATA FOUND FOR USER ANALYTICS, PLEASE CHECH THE callLogs Table.')
            return
        
        for row in user_data:
            try:
                user_id = int(row[0])
                avg_duration= row[1]
                num_calls = row[2]
                writer.writerow([user_id, avg_duration, num_calls])
            except Exception as e:
                continue        


# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['callId', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])
        cursor.execute(''' select callId, phonenumber,starttime, endtime, direction, userid 
                       from callLogs
                       order by userId, startTime
                       ''')
        for row in cursor.fetchall():
            writer.writerow(row)

    print("TODO: write_ordered_calls")



# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
