from controllers.conexao import Conexao
from interface.interface import menu, terminal
import sys

def run(spinner = 'moon'):
    conn = Conexao().conectar()
    conn_bk = Conexao(server='cosmosdw').conectar()
    menu(conn, conn_bk, spinner)


if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        sys.exit()
    except:
        terminal.print_exception()
