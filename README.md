#Bank ETL Pipeline

This project is a simple ETL (Extract, Transform, Load) pipeline built in Python. It scrapes data from a Wikipedia page listing the largest banks in the world by market capitalization, transforms the data into multiple currencies, and stores it in both a CSV file and a SQLite database.

---

## Features

- Web scraping using `requests` and `BeautifulSoup`
- Data transformation into GBP, EUR, and INR
- Output to both CSV and SQLite database
- Logging of each ETL step
- Prints top 3 banks by market cap in USD

---

## roject Structure

```
bank-etl-pipeline/
│
├── banks_project.py         # Main ETL script
├── exchange_rate.csv        # (Optional) Currency rates input
├── code_log.txt             # Log file (generated after run)
├── Largest_banks_data.csv   # Output CSV
├── Banks.db                 # Output SQLite database
├── requirements.txt         # Python dependencies
└── README.md                # Project documentation
```

---

## How to Run

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/bank-etl-pipeline.git
cd bank-etl-pipeline
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the ETL Script

```bash
python banks_project.py
```

---

## Example Output

After running the script, you'll see the top 3 largest banks printed in the terminal:

```
Top 3 Banks by Market Cap (USD):
                NAME               MC_USD_Billion
0               ICBC               488.59
1  China Construction Bank         231.52
2     Agricultural Bank of China   194.56
```

---

## Dependencies

- pandas==2.2.3  
- requests==2.32.3  
- beautifulsoup4==4.13.4  




