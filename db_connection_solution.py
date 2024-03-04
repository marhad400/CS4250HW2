#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: db_connection_solution.py
# SPECIFICATION: backend for inverted index simulation
# FOR: CS 4250- Assignment #1
# TIME SPENT: 1 day
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import psycopg2
from psycopg2.extras import RealDictCursor

def connectDataBase():

    # Create a database connection object using psycopg2
    # --> add your Python code here
    DB_NAME = "assignment2"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        return conn

    except:
        print("Database not connected successfully")

def createCategory(cur, id_cat, name):

    # Insert a category in the database
    # --> add your Python code here
    sql = "Insert into categories (id_cat, name) Values (%s, %s)"

    recset = [id_cat, name]
    cur.execute(sql, recset)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    # --> add your Python code here
    cur.execute("select id_cat from categories where name = %s;", (docCat,))
    id_cat = cur.fetchone()['id_cat']

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # --> add your Python code here
    num_chars = len(''.join(char for char in docText if char.isalpha()))

    sql = "Insert into documents (doc, title, text, num_chars, date, id_cat)" \
          "Values (%s, %s, %s, %s, %s, %s)"

    recset = [docId, docTitle, docText, num_chars, docDate, id_cat]
    cur.execute(sql, recset)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    cur.execute(r"SELECT DISTINCT regexp_split_to_table(lower(regexp_replace(text, '[^\w\s]', '', 'g')), ' ') AS term FROM documents WHERE doc = %s", (docId))

    # 3.2 For each term identified, check if the term already exists in the database
    rows = cur.fetchall()
    new_terms = [row['term'] for row in rows]

    # 3.3 In case the term does not exist, insert it into the database
    # --> add your Python code here
    cur.execute("Select term from terms order by term asc")
    terms_rows = cur.fetchall()
    terms = [row['term'] for row in terms_rows]
    print(terms)

    for new_term in new_terms:
        if new_term not in terms:
            num_chars = len(new_term)
            sql = "Insert into terms (term, num_chars) Values (%s, %s)"
            recset = [new_term, num_chars]
            cur.execute(sql, recset)

            terms.append(new_term)

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    # 4.3 Insert the term and its corresponding count into the database
    # --> add your Python code here
    term_counts = {}
    for new_term in new_terms:
        term_counts[new_term] = 0

    for new_term in new_terms:
        if new_term in docText.lower():
            term_counts[new_term] += 1

    for term, term_count in term_counts.items():
        sql = "Insert into doc_term_index (doc, term, term_count) Values (%s, %s, %s)"

        recset = [docId, term, term_count]
        cur.execute(sql, recset)

def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # --> add your Python code here
    cur.execute("DELETE FROM doc_term_index WHERE doc = %s;", (docId,))

    # 2 Delete the document from the database
    # --> add your Python code here
    sql = "Delete from documents where doc = %s"

    cur.execute(sql, (docId))
    

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    # --> add your Python code here
    #deleteDocument(cur, docId)
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    # --> add your Python code here
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

    '''
    num_chars = len(''.join(char for char in docText if char.isalpha()))
    sql = "Update documents set text = %(docText)s, title = %(docTitle)s, num_chars = %(num_chars)s, date = %(docDate)s, id_cat = %(docCat)s where doc = %(docId)s"
    cur.execute(sql, {'text': docText,
                      'title': docTitle,
                      'num_chars': num_chars,
                      'date': docDate if docDate != '' else None,
                      'doc': docId})
    '''
    

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    index = ""

    cur.execute("SELECT doc_term_index.term, documents.title, doc_term_index.term_count FROM documents JOIN categories ON documents.id_cat = categories.id_cat JOIN doc_term_index ON documents.doc = doc_term_index.doc")
    recset = cur.fetchall()

    for rec in recset:
        index += rec['term'] + " | " + rec['title'] + " | " + str(rec['term_count']) + "\n"

    return index

def createTables(cur, conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
                id_cat INTEGER NOT NULL PRIMARY KEY, 
                name TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            doc INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL,
            title TEXT NOT NULL,
            num_chars INTEGER NOT NULL,
            date TEXT NOT NULL,
            id_cat INTEGER NOT NULL,
            FOREIGN KEY (id_cat) REFERENCES Categories (id_cat)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS terms (
            term TEXT NOT NULL PRIMARY KEY,
            num_chars INTEGER NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS doc_term_index (
            doc INTEGER NOT NULL,
            term TEXT NOT NULL,
            term_count INTEGER,
            PRIMARY KEY (doc, term),
            FOREIGN KEY (doc) REFERENCES Documents (doc),
            FOREIGN KEY (term) REFERENCES Terms (term)
        );
    """)

    # Commit the changes
    conn.commit()

    # Closing the cursor
    cur.close()

def main():
    # Connecting to the database
    conn = connectDataBase()
    cur = conn.cursor()

    # Creating the tables (The database should be created manually)
    createTables(cur, conn)

if __name__ == "__main__":
    main()