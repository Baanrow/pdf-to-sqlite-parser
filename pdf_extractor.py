"""
Iterates through PDF files in a directory. For each PDF file, iterates through
each page to extract metadata and table data. Writes that data to an SQL DB
once it has been extracted.

Import libraries.
Define logging.
Define SQL and PDF paths.

main() - calls the setup SQL function, defines the list of files, then calls
the iterate function.

setup_sql() - drops the SQL table and recreates it along with columns.

iterate_files() - iterates through each of the files calling the data
extraction and then write to SQL functions.

extract_data() - for a single PDF file, calls the metadata then the 
table data extraction functions.

extract_metadata() - for a single PDF file, iterates through each page
to extract metadata.

extract tabledata() - for a single PDF file, iterates through each page
to extract table data.

write_sql() - write data to SQL for a single PDF file.
"""
import pymupdf
import logging
import sqlite3
from pathlib import Path
import re

PDF_DIR = Path("state_db/analyse")
PDF_GLOB = "*.pdf"
SQL_PATH = Path("state_db/school_reports.db")
logging.basicConfig(level=logging.DEBUG)

def main():
    """
    Set up the SQL DB, then call the iterate files function using the file
    list.
    """
    setup_sql()
    file_list = sorted(list(PDF_DIR.glob(PDF_GLOB)), reverse=True)
    # logging.debug("file_list: %s", file_list)
    iterate_files(file_list)


def iterate_files(files):
    """
    Iterates through each file in the list, calling the data extraction
    then writing the result to SQL.
    """
    for pdf_file in files:
        try:
            extracted_data = extract_data(pdf_file)
            if extracted_data is None:
                continue
            write_sql(extracted_data)
        except Exception as e:
            logging.exception(" iterate_files() error: %s", e)
            continue


def extract_data(current_file):
    """
    For a single file, calls the extract metadata and table data functions
    sequentially.
    """
    try:
        with pymupdf.open(current_file) as current_document:
            extracted_metadata = extract_metadata(current_document)
            if extracted_metadata is None:
                return None
            extracted_tabledata = extract_tabledata(current_document)
            if extracted_tabledata is None:
                return None
            return {**extracted_metadata, "all_rows": extracted_tabledata}
    except Exception as e:
        logging.exception()
        return None


def extract_metadata(document):
    """
    For a single PDF, iterates through each page to extract metadata.
    """
    nm_search = re.compile(r"^([A-Z][a-z]+) ([A-Z]+)", re.MULTILINE)
    se_search = re.compile(r"^Semester (\d), (\d{4}) - Progress Report (\d)", re.MULTILINE)
    
    for page in document:
        try:
            page_text = page.get_text()
            name_match = nm_search.search(page_text)
            if name_match is None:
                continue
            semester_match = se_search.search(page_text)
            if semester_match is None:
                continue
            firstname, surname = name_match.group(1), name_match.group(2)
            semester, year, report = map(int, semester_match.groups())
            return {
                "firstname": firstname,
                "surname": surname,
                "year": year,
                "semester": semester,
                "report": report
            }
                
        except Exception as e:
            logging.exception(" extract_metadata() error: %s", e)
            continue
    return None
        


def extract_tabledata(document):
    """
    For a single PDF, iterates through each page to extract table data.
    """
    for page in document:
        try:
            table_search = page.find_tables()
            if table_search.tables:
                for table in table_search.tables:
                    table_rows = table.extract()
                    if table_rows and "Areas Of Assessment" in table_rows[0]:
                        return table_rows[2:]
        except Exception as e:
            logging.exception(" extract_tabledata() error: %s", e)
            continue
    return None


def write_sql(extracted_data):
    """
    Writes data to the SQL DB for a single PDF file.
    """
    try:
        with sqlite3.connect(SQL_PATH) as conn:
            cur = conn.cursor()
            for row in extracted_data["all_rows"]:
                try:
                    cur.execute(
                        """
                        INSERT INTO mesc_reports (
                            "firstname",
                            "surname",
                            "year",
                            "semester",
                            "report",
                            "subject",
                            "Evidence of Learning",
                            "Personal Learning",
                            "Working with Others",
                            "Orderly Behaviour",
                            "Learning Outside the Classroom"
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            extracted_data["firstname"],
                            extracted_data["surname"],
                            extracted_data["year"],
                            extracted_data["semester"],
                            extracted_data["report"],
                            row[0], row[1], row[2], row[3], row[4], row[5]
                        )
                    )
                except sqlite3.Error as e:
                    logging.exception(" write_sql() row error: %s", e)
                    continue
    except sqlite3.Error as e:
        logging.exception(" write_sql() config error: %s", e)



def setup_sql():
    """
    Drops the current SQL table then recreates it.
    """
    try:
        with sqlite3.connect(SQL_PATH) as conn:
            cur = conn.cursor()
            cur.executescript(
                """
                DROP TABLE IF EXISTS mesc_reports; CREATE TABLE mesc_reports (
                    "id" INTEGER PRIMARY KEY,
                    "firstname" TEXT,
                    "surname" TEXT,
                    "year" INTEGER,
                    "semester" INTEGER,
                    "report" INTEGER,
                    "subject" TEXT,
                    "Evidence of Learning" TEXT,
                    "Personal Learning" TEXT,
                    "Working with Others" TEXT,
                    "Orderly Behaviour" TEXT,
                    "Learning Outside the Classroom" TEXT
                )
                """
            )
    except sqlite3.Error as e:
        logging.exception(" setup_sql() error: %s", e)

if __name__ == "__main__":
    main()
