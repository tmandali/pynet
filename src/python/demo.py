import logging
# import psycopg2
# import psycopg2.extras
# import os
import platform
from turtle import st
from typing import Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ado_conn_str = os.getenv("ConnectionStrings__weather")
# logger.info(f"ADO Connection String: {ado_conn_str}")
# ado_conn_dict = dict(item.split("=") for item in ado_conn_str.split(";"))
# cnx = psycopg2.connect(dbname=ado_conn_dict["Database"], user=ado_conn_dict["Username"], password=ado_conn_dict["Password"], host=ado_conn_dict["Host"], port=ado_conn_dict["Port"])
# cursor = cnx.cursor(cursor_factory=psycopg2.extras.DictCursor)

def greetings(name: str) -> str:
    logger.info(f"Greetings to {name}")
    return f"Hello, {name}!"

# if __name__ == "__main__":
#     print(greetings("John"))

def return_dict() -> dict[str, int]:
    return {'a': 1, 'b': 2, 'c': 3}

def get_info() -> dict[str, str]:
    """Return system information."""
    import platform
    return {
        "system": platform.system(),
        "version": platform.version(),
        "machine": platform.machine()
    }

def get_obj() -> Any:
    """Return system information."""
    import platform
    return {"mesaj": "Ana Sunucu Aktif"} 