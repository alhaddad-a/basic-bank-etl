
# ETL script to extract bank market cap data from Wikipedia, transform it, and store it in a CSV and SQLite DB

import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# ---------- Configuration ----------
log_file = 'code_log.txt'  # Path for log file
table_attributes = ('NAME', 'MC_USD_Billion')  # Column names for extracted table
csv_path = 'Largest_banks_data.csv'  # Output CSV path
url = r'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'  # Target page (web archive snapshot)
table_name = 'Largest_banks'  # Table name for database
sql_cnn = sqlite3.connect('Banks.db')  # SQLite database connection

# ---------- Logging ----------
def log_progress(message, level="INFO"):
    """Write timestamped log message to file."""
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now().strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(f'{now}, {level}, {message}\n')

# ---------- Extraction ----------
def extract(url, table_attributes):
    """Extract table data from the given Wikipedia page."""
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    df = pd.DataFrame(columns=table_attributes)

    # Find all table bodies
    tables = soup.find_all('tbody')
    if not tables:
        return df  # Return empty DataFrame if no table found

    rows = tables[0].find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 3:
            bank = cols[1].get_text(strip=True)
            MC = cols[2].get_text(strip=True).replace(',', '').replace('\n', '').replace('$', '')
            try:
                MC = float(MC)
                df1 = pd.DataFrame([{'NAME': bank, 'MC_USD_Billion': MC}])
                df = pd.concat([df, df1], ignore_index=True)
            except ValueError:
                continue  # Skip rows with non-numeric market cap

    return df

# ---------- Transformation ----------
def transform(df):
    """Add currency conversion columns and round to 2 decimals."""
    df['MC_GBP_Billion'] = (df['MC_USD_Billion'] * 0.8).round(2)
    df['MC_EUR_Billion'] = (df['MC_USD_Billion'] * 0.93).round(2)
    df['MC_INR_Billion'] = (df['MC_USD_Billion'] * 82.95).round(2)
    return df

# ---------- Loaders ----------
def load_to_csv(df, csv_path):
    """Save DataFrame to CSV."""
    df.to_csv(csv_path, index=False)

def load_to_db(df, sql_cnn, table_name):
    """Load DataFrame to SQLite database."""
    df.to_sql(table_name, sql_cnn, if_exists='replace', index=False)

# ---------- Query Executor ----------
def run_queries(query_statement, sql_cnn):
    """Run a SQL query and print the result."""
    print("\nRunning Query:")
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_cnn)
    print(query_output)

# ---------- ETL Pipeline Execution ----------
if __name__ == "__main__":
    log_progress('Preliminaries complete. Initiating ETL process')

    df = extract(url, table_attributes)
    log_progress('Data extraction complete. Initiating Transformation process')

    df = transform(df)
    log_progress('Data transformation complete. Initiating Loading process')

    load_to_csv(df, csv_path)
    log_progress('Data saved to CSV file')

    log_progress('SQL Connection initiated')
    load_to_db(df, sql_cnn, table_name)
    log_progress('Data loaded to Database as a table, Executing queries')

    # Print top 3 banks by USD Market Cap
    print("\nTop 3 Banks by Market Cap (USD):")
    print(df.sort_values(by='MC_USD_Billion', ascending=False).head(3)[['NAME', 'MC_USD_Billion']])

    log_progress('Process Complete')

    sql_cnn.close()
    log_progress('Server Connection closed')
