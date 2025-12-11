"""
Veritabanı connection string helper fonksiyonları
"""

import urllib.parse


# ============================================================================
# MSSQL CONNECTION STRINGS
# ============================================================================

def get_mssql_connection(server: str, database: str) -> str:
    """MSSQL için Windows Authentication connection string oluşturur"""
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};"
        f"Trusted_Connection=yes;TrustServerCertificate=yes;"
    )
    params = urllib.parse.quote_plus(connection_string)
    return f"mssql+pyodbc:///?odbc_connect={params}"


def get_mssql_connection_with_auth(server: str, database: str, username: str, password: str) -> str:
    """MSSQL için SQL Authentication connection string oluşturur (ODBC driver)"""
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={username};PWD={password};"
        f"TrustServerCertificate=yes;"
    )
    params = urllib.parse.quote_plus(connection_string)
    return f"mssql+pyodbc:///?odbc_connect={params}"


def get_mssql_simple(host: str, database: str, username: str, password: str, port: int = 1433) -> str:
    """MSSQL için basit connection string (pymssql driver gerektirir: pip install pymssql)"""
    return f"mssql+pymssql://{username}:{password}@{host}:{port}/{database}"


def get_mssql_pyodbc_simple(host: str, database: str, username: str, password: str, port: int = 1433) -> str:
    """MSSQL için basit connection string (pyodbc driver)"""
    driver = urllib.parse.quote_plus("ODBC Driver 17 for SQL Server")
    return f"mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver={driver}"


# ============================================================================
# POSTGRESQL CONNECTION STRINGS
# ============================================================================

def get_postgres_connection(host: str, database: str, username: str, password: str, port: int = 5432) -> str:
    """PostgreSQL connection string oluşturur"""
    return f"postgresql://{username}:{password}@{host}:{port}/{database}"


# ============================================================================
# DİĞER VERİTABANLARI
# ============================================================================

def get_sqlite_connection(db_path: str) -> str:
    """SQLite connection string oluşturur"""
    return f"sqlite:///{db_path}"


def get_mysql_connection(host: str, database: str, username: str, password: str, port: int = 3306) -> str:
    """MySQL connection string oluşturur"""
    return f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"

