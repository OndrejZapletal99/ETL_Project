import bs4
import requests
import pandas as pd
import datetime as dt
from sqlalchemy import create_engine, text


class WebScraper:

    """
    A web scraper class to extract articles from a given URL.

    Attributes:
    ----------
    url : str
        The URL of the website to scrape.
    res : requests.Response or None
        The HTTP response object after requesting the URL.
    soap : bs4.BeautifulSoup or None
        Parsed HTML content of the webpage.
    articles : set
        A set of extracted articles, each represented as a tuple (title, link, download_date).
    download_date : str
        The date when the scraping was performed (YYYY-MM-DD).

    Methods:
    -------
    web_reader():
        Fetches the webpage content and parses it into BeautifulSoup.
    article_find():
        Extracts article titles and links from the parsed HTML.
    articles_to_df():
        Converts the extracted articles into a Pandas DataFrame.
    """
    
    def __init__(self, url):
        self.url = url
        self.res = None
        self.soap = None
        self.articles = set()
        self.download_date = dt.datetime.now().strftime("%Y-%m-%d")

    def web_reader(self):
        """Fetches the webpage content and parses it into BeautifulSoup."""
        try:
            self.res = requests.get(self.url,timeout = 10)
            self.res.raise_for_status()
            self.soap = bs4.BeautifulSoup(self.res.text, "lxml")
        except requests.exceptions.RequestException as e:
            print(f'Something went wrong {e}')
            self.soap = None

    def article_find(self):
        """Extracts article titles and links from the parsed HTML."""
        if self.soap is None:
            print('Error: No parsed HTML, call web_reader()')

        for article in self.soap.select(".c_aM.c_aP"):
            title = article.get_text(strip=True)
            a_tag = article.find("a")
            if a_tag and "href" in a_tag.attrs and title:
                link = a_tag["href"]
                self.articles.add((title, link, self.download_date))

    def articles_to_df(self):
        """Converts the extracted articles into a Pandas DataFrame."""
        df_articles = pd.DataFrame(
            self.articles, columns=["Title", "Link", "Download_Date"]
        )
        return df_articles


class SqlManager:
    """
    A class to manage SQL Server database connections and execute queries.

    Attributes:
    ----------
    server : str
        The SQL Server name or address.
    database : str
        The database name to connect to.
    odbc_driver : str
        The ODBC driver name for the connection.
    engine : sqlalchemy.engine.Engine or None
        The SQLAlchemy engine instance for database interaction.
    connection_string : str
        The connection string used to establish the database connection.
    ssms_mapping_types : dict
        A dictionary mapping Pandas data types to SQL Server data types.

    Methods:
    -------
    connect():
        Establishes a connection to the database.
    disconnect():
        Closes the database connection.
    execute_query_to_df(after_select_text: str):
        Executes a SQL SELECT query and returns the result as a Pandas DataFrame.
    execute_query(sql_statement: str):
        Executes a SQL query that does not return a result (INSERT, UPDATE, DELETE).
    create_table(table_name: str, database: str, data_frame: pd.DataFrame):
        Creates a table in the specified database based on a given DataFrame.
    append_existing_table(table_name: str, database: str, data_frame: pd.DataFrame):
        Appends data from a DataFrame to an existing table in the database.
    """
    def __init__(self, server: str, database: str, odbc_driver: str):
        self.server = server
        self.database = database
        self.odbc_driver = odbc_driver
        self.engine = None
        self.connection_string = (
            f"mssql+pyodbc://@{self.server}/{self.database}?trusted_connection=yes&driver={self.odbc_driver}"
        )
        self.ssms_mapping_types = {
            "int64": "BIGINT",
            "int32": "INT",
            "float64": "FLOAT",
            "float32": "REAL",
            "bool": "BIT",
            "object": "NVARCHAR(MAX)",
            "string": "NVARCHAR(MAX)",
            "datetime64[ns]": "DATETIME",
            "timedelta[ns]": "TIME",
            "str": "NVARCHAR(MAX)",
        }

    def connect(self):
        """Establishes a connection to the database."""
        try:
            self.engine = create_engine(self.connection_string)
            print("Successfully connected")
            return self.engine
        except Exception as e:
            print(f"Something went wrong: {e}")
            raise

    def disconnect(self):
        """Closes the database connection."""
        if self.engine is not None:
            self.engine.dispose()
            self.engine = None
            print("Connection closed")
        else:
            print('No active connection')

    def execute_query_to_df(self, after_select_text: str):
        """
        Executes a SQL SELECT query and returns the result as a Pandas DataFrame.

        Parameters:
        after_select_text : str
            The part of the SQL query that follows "SELECT".

        Returns:
        -------
        pd.DataFrame or None
            A DataFrame containing the query results, or None if an error occurs.
        """
        if self.engine is None:
            print("No connection to database, please create a connection")
            return None

        sql_statement = f"SELECT {after_select_text}"
        try:
            df = pd.read_sql(sql_statement, con=self.engine)
            return df
        except Exception as e:
            print(f"Something went wrong: {e}")
            raise

    def execute_query(self, sql_statement: str):
        """
        Executes a SQL query that does not return a result (INSERT, UPDATE, DELETE).

        Parameters:
        sql_statement : str
            The SQL statement to execute.

        Returns:
        -------
        None
        """
        if self.engine is None:
            print("No connection to database, please create a connection")
            return None

        try:
            with self.engine.connect() as connection:
                connection.execute(text(sql_statement))
                connection.commit()
        except Exception as e:
            print(f"Something went wrong while executing the query: {e}")
            raise

    def create_table(self, table_name: str, database: str, data_frame: pd.DataFrame):
        """
        Creates a table in the specified database based on the structure of a given DataFrame.

        Parameters:
        table_name : str
            The name of the table to create.
        database : str
            The database where the table should be created.
        data_frame : pd.DataFrame
            The DataFrame whose structure defines the table schema.

        Returns:
        -------
        None
        """
        columns_and_types = {
            col: self.ssms_mapping_types.get(str(dtype), "NVARCHAR(MAX)")
            for col, dtype in data_frame.dtypes.to_dict().items()
        }

        if self.engine is None:
            print("No connection to database, please create a connection")
            return None

        try:
            temp_df = self.execute_query_to_df(
                f"TABLE_NAME FROM [{database}].INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
            )
            if not temp_df.empty:
                print("Table already exists, please select another method (Append, Overwrite, Truncate)")
                return None

            print("Table does not exist and will be created...")
            columns_definition = ", ".join(
                [f"{col} {data_type}" for col, data_type in columns_and_types.items()]
            )

            create_table_query = (
                f"CREATE TABLE [{database}].[dbo].[{table_name}] ({columns_definition})"
            )
            self.execute_query(create_table_query)

            check_table_query = (
                f"* FROM [{database}].INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
            )
            temp_df = self.execute_query_to_df(check_table_query)
            if not temp_df.empty:
                print(f"Table '{table_name}' created successfully in [{database}]")
            else:
                print(f"Failed to create table '{table_name}'")
        except Exception as e:
            print(f"Something went wrong: {e}")
            raise

    def append_existing_table(self, table_name: str, database: str, data_frame: pd.DataFrame):
        """
        Appends data from a DataFrame to an existing table in the database.

        Parameters:
        table_name : str
            The name of the table where data will be appended.
        database : str
            The database containing the table.
        data_frame : pd.DataFrame
            The DataFrame with data to be inserted.

        Returns:
        -------
        None
        """
        if self.engine is None:
            print("No connection to database, please create a connection")
            return None

        try:
            temp_df = self.execute_query_to_df(
                f"TABLE_NAME FROM [{database}].INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}'"
            )
            if temp_df.empty:
                print("Table does not exist, please create a table")
                return None

            print("Data will be inserted in a moment")
            data_frame.to_sql(
                name=table_name, schema="dbo", if_exists="append", con=self.engine, index=False
            )
            print("Data successfully appended")
        except Exception as e:
            print(f"Something went wrong: {e}")
            raise
