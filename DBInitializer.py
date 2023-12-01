import csv
import pymysql

# Connect to your MySQL database
connection = pymysql.connect(host='localhost', user='root', password='password123')
cursor = connection.cursor()

cursor.execute(f"DROP DATABASE IF EXISTS Library;")
cursor.execute(f"CREATE DATABASE IF NOT EXISTS Library;")
cursor.execute(f"USE Library;")

create_book = """
CREATE TABLE IF NOT EXISTS BOOK (
  isbn   CHARACTER(10) NOT NULL PRIMARY KEY,
  Title  VARCHAR(250) NOT NULL
);
"""
create_authors = """
CREATE TABLE IF NOT EXISTS AUTHORS (
    Author_id INT AUTO_INCREMENT PRIMARY KEY,
    Name VARCHAR(255) UNIQUE NOT NULL
);
"""
create_book_authors = """
CREATE TABLE IF NOT EXISTS BOOK_AUTHORS (
    isbn CHAR(10),
    author_id INT,
    PRIMARY KEY (isbn, author_id),
    FOREIGN KEY (author_id) REFERENCES AUTHORS(author_id),
    FOREIGN KEY (isbn) REFERENCES BOOK(isbn) 
);
"""
create_borrower = """
CREATE TABLE IF NOT EXISTS BORROWER (
    card_id  CHAR(8) NOT NULL PRIMARY KEY,
    ssn      CHAR(11) NOT NULL,
    Bname    VARCHAR(25) NOT NULL,
    address  VARCHAR(100) NOT NULL,  
    phone    CHAR(14) NOT NULL, 
    email    VARCHAR(50) NOT NULL,
    city 	 VARCHAR(25) NOT NULL, 
    state	 VARCHAR(25) NOT NULL
);
"""
create_book_loan = """
CREATE TABLE IF NOT EXISTS BOOK_LOANS (
    loan_id INT NOT NULL,
    isbn CHAR(10) NOT NULL,
    card_id CHAR(8) NOT NULL,
    date_out DATE NOT NULL,
    due_date DATE NOT NULL,
    date_in DATE,
    PRIMARY KEY (loan_id),
    FOREIGN KEY (isbn) REFERENCES BOOK(isbn),
    FOREIGN KEY (card_id) REFERENCES BORROWER(card_id)
);
"""
create_fines = """
CREATE TABLE IF NOT EXISTS FINES (
    loan_id INT NOT NULL,
    fine_amt DECIMAL(10,2) NOT NULL,
    paid INT NOT NULL,
    PRIMARY KEY (loan_id),
    FOREIGN KEY (loan_id) REFERENCES BOOK_LOANS(loan_id)
);
"""

cursor.execute(create_book)
cursor.execute(create_authors)
cursor.execute(create_book_authors)
cursor.execute(create_borrower)
cursor.execute(create_book_loan)
cursor.execute(create_fines)
connection.commit()

def insert_into_book(cursor, isbn, title):
    sql = "INSERT INTO BOOK (isbn, Title) VALUES (%s, %s)"
    cursor.execute(sql, (isbn, title))

def insert_into_authors(cursor, author_name):
    sql = "INSERT INTO AUTHORS (Name) VALUES (%s) ON DUPLICATE KEY UPDATE Name=Name"
    cursor.execute(sql, (author_name,))
    cursor.execute("SELECT Author_id FROM AUTHORS WHERE Name=%s", (author_name,))
    result = cursor.fetchone()
    return result[0]

def insert_into_book_authors(cursor, isbn, author_id):
    sql = "INSERT INTO BOOK_AUTHORS (isbn, author_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE isbn=isbn"
    cursor.execute(sql, (isbn, author_id))

def insert_into_borrower(cursor, card_id, ssn, first_name, last_name, email, address, city, state, phone):
    numeric_id = int(card_id[2:])
    sql = "INSERT INTO BORROWER (card_id, ssn, Bname, address, phone, email, city, state) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(sql, (numeric_id, ssn, f'{first_name} {last_name}', address, phone, email, city, state))

# Path to books.csv
books_csv_file_path = 'books.csv'
#'C:/Users/huynh/Downloads/books.csv'
# Open and read the BOOKS CSV file
with open(books_csv_file_path, 'r', encoding='utf-8') as file:
    # Create a CSV reader
    csv_reader = csv.reader(file, delimiter='\t')  # Assuming tab-separated values
    next(csv_reader) #skip first line
    # Iterate through each row in the CSV file
    for row in csv_reader:
        isbn, _, title, authors, *_ = row

        # Split authors into a list
        author_list = authors.split(',')

        # Insert data into the BOOK table
        insert_into_book(cursor, isbn, title)

        # Insert data into the AUTHORS table and get author_id
        for author_name in author_list:
            author_id = insert_into_authors(cursor, author_name.strip())

            # Insert data into the BOOK_AUTHORS table
            insert_into_book_authors(cursor, isbn, author_id)

borrowers_csv_file_path = 'borrowers.csv'
# Open and read the BORROWERS CSV file
with open(borrowers_csv_file_path, 'r') as file:
    csv_reader = csv.reader(file)
    
    # Skip the header row
    next(csv_reader)

    # Iterate through each row in the CSV file
    for row in csv_reader:
        # Extract values from the CSV row
        card_id, ssn, first_name, last_name, email, address, city, state, phone = row

        # Insert data into the BORROWER table
        insert_into_borrower(cursor, card_id, ssn, first_name, last_name, email, address, city, state, phone)

# Commit the changes to the database
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()