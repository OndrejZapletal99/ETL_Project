# 📌 Project Documentation

## 📂 Project Structure
```
/project_root
│── main.ipynb         # Main Jupyter Notebook (not analyzed)
│── classes.py         # Contains WebScraper and SqlManager
│── key_words.py       # Contains keywords and key persons
│── functions.py       # Contains helper functions for data handling
```

---

## 🏗️ Used Technologies
- **Python**
- **Pandas** (data processing)
- **NumPy** (numerical computations)
- **Requests** (HTTP requests)
- **BeautifulSoup** (HTML parsing)
- **SQLAlchemy** (database management)

---

## 📝 Modules and Descriptions

### 🔍 [`classes.py`](https://github.com/OndrejZapletal99/ETL_Project/blob/main/module_folder/classes.py)
#### **WebScraper** – Web Article Scraping
The `WebScraper` class allows downloading and extracting articles from a given URL.

**Main Methods:**
- `web_reader()` – Downloads and parses HTML using `BeautifulSoup`.
- `article_find()` – Finds articles based on CSS selectors and stores their titles + links.
- `articles_to_df()` – Saves the found articles into a `DataFrame`.

#### **SqlManager** – Database Management
The `SqlManager` class allows connecting to an SQL server, executing queries, and managing tables.

**Main Methods:**
- `connect()` – Establishes a connection to the database.
- `disconnect()` – Closes the connection.
- `execute_query_to_df(query)` – Runs a `SELECT` query and returns a `DataFrame`.
- `execute_query(query)` – Executes any SQL query (`INSERT`, `UPDATE`, `DELETE`).
- `create_table(name, db, df)` – Creates a table based on a `DataFrame`.
- `append_existing_table(name, db, df)` – Appends data to an existing table.

---

### 🔑 [`key_words.py`](https://github.com/OndrejZapletal99/ETL_Project/blob/main/module_folder/key_words.py)
Contains a dictionary of keywords `key_words` that maps categories to corresponding terms.

**Example:**
```python
'Volby': ['Volby', 'Volební', 'Voleb']
```
Additionally, it includes a list `key_person`, which contains key person names.

**Example:**
```python
['Babiš', 'Fiala', 'Havlíček', 'Pekarová']
```

---

### 🛠 [`functions.py`](https://github.com/OndrejZapletal99/ETL_Project/blob/main/module_folder/functions.py)
Contains helper functions for handling and transforming `DataFrame` columns.

**Main Functions:**
- `object_cols(df)` – Returns columns of type `object`.
- `int_cols(df)` – Returns columns of type `int`.
- `float_cols(df)` – Returns columns of type `float`.
- `numeric_cols(df)` – Returns columns of type `int` or `float`.
- `bool_cols(df)` – Returns columns of type `bool`.
- `datetime_cols(df)` – Returns columns of type `datetime`.
- `cols_to_string(df, list_of_columns)` – Converts specified columns to `string`.
- `cols_to_float(df, list_of_columns)` – Converts specified columns to `float`.
- `cols_to_int(df, list_of_columns)` – Converts specified columns to `int`.
- `cols_to_bool(df, list_of_columns)` – Converts specified columns to `bool`.
- `cols_to_datetime(df, list_of_columns)` – Converts specified columns to `datetime`.

---

## 🚀 How to Run the Project
1. **Install Dependencies:**
   ```sh
   pip install pandas numpy requests beautifulsoup4 sqlalchemy pyodbc
   ```
2. **Using the WebScraper:**
   ```python
   scraper = WebScraper("https://example.com")
   scraper.web_reader()
   scraper.article_find()
   df = scraper.articles_to_df()
   print(df.head())
   ```
3. **Connecting to the Database:**
   ```python
   sql_manager = SqlManager("server_name", "database_name", "ODBC Driver 17 for SQL Server")
   sql_manager.connect()
   ```

---


