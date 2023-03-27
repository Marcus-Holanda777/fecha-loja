from sqlalchemy import create_engine
from urllib.parse import quote_plus


class Conexao:
    DRIVER = "{ODBC Driver 17 for Sql Server}"
    DNS = (
        "Driver={};"
        "Server={};"
        "Database={};"
        "Trusted_Connection=Yes;"
    )

    def __init__(self, server: str = 'cosmos', database: str = 'cosmos_v14b') -> None:
        self.server = server
        self.database = database
        self.con_url = quote_plus(
            self.DNS.format(
                self.DRIVER, self.server, self.database
            )
        )

    def conectar(self):
        return create_engine(
            "mssql+pyodbc:///?odbc_connect=%s" % self.con_url,
            fast_executemany=True
        )
