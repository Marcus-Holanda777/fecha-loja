from rich.console import Console
from rich.markdown import Markdown
from rich.prompt import IntPrompt, Prompt, Confirm
from rich.tree import Tree
from rich.table import Table
from collections import namedtuple
from pathlib import Path
import pandas as pd
import numpy as np
import re
import os
from sqlalchemy.engine import Engine
from utils.utils import (
    lista_loja, 
    load_categoria, 
    load_estoque, 
    load_venda,
    distribuicao
)

Fecha = namedtuple('Fecha', 'filial dep tipo destino categoria')

terminal = Console()

MARKDOWN = """
# SISTEMA LOJA FECHADA

## descricao rotinas

> CTRL + C fecha app.

1. Filial origem
3. CD destino (gera os fracionados com base no cd)
2. Filiais destino [GR, GO, UF, FILIAIS]
3. Categoria
4. Calculo venda demanda ultimos 6 meses
5. Distribuicao das quantidades
6. Envio dos PDF

* Base de dados UC
* Gerar notas fiscais

"""

def retorno_filiais() -> list[int]:
    """_summary_

    Returns:
        list[int]: _description_
    """
    inteiros = re.compile(r'[1-9\s]')
    while True:
        filiais = Prompt.ask(">> filiais [red b]fil[/]", default='erro', show_default=False)

        if inteiros.match(filiais):
            return list(map(int, filiais.split()))
        
        terminal.print("[prompt.invalid]digitar loja ou lista de lojas")


def retorno_hierarquia(tipo: str) -> dict[str]:
    """_summary_

    Args:
        tipo (str): _description_

    Returns:
        dict[str]: _description_
    """
    textos = re.compile(r'[^1-9]')
    while True:
        hierarquia = Prompt.ask(f">> hierarquia [b red]{tipo}[/]", default='erro', console=terminal, show_default=False)
        
        if textos.match(hierarquia) and hierarquia != 'erro':
            return {tipo: hierarquia}
        
        terminal.print(f"[prompt.invalid]digitar nome do {tipo.upper()}")


def retorno_uf(tipo: str) -> dict[str]:
    """_summary_

    Args:
        tipo (str): _description_

    Returns:
        dict[str]: _description_
    """
    estados = re.compile(r'^\D{2}$')
    while True:
        estado = Prompt.ask(f">> estado [b red]{tipo}[/]", default='erro', console=terminal, show_default=False)
        
        if estados.match(estado) and estado != 'erro':
            return {tipo: estado}
        
        terminal.print(f"[prompt.invalid]digitar nome do {tipo.upper()}")


def retorna_tipo(tipo: str) -> list[int] | dict[str]:
    """_summary_

    Args:
        tipo (str): _description_

    Returns:
        list[int] | dict[str]: _description_
    """
    match tipo:
        case 'gr' | 'go':
            return retorno_hierarquia(tipo)
        case 'uf':
            return retorno_uf(tipo)
        case 'fil':
            return retorno_filiais()


def retorna_categoria() -> list[str] | None:
    """_summary_

    Returns:
        list[str] | None: _description_
    """
    categs = ['term', 'frac', 'env', 'psico']

    while True:
        categ = Prompt.ask(f">> categoria [b magenta]{categs}[/]", console=terminal, default='full')

        if any([c in categs for c in categ.split()]):
            return list(map(str.upper, categ.split()))
        
        if categ == 'full':
            return list(map(str.upper, categs))
        
        terminal.print(f"[prompt.invalid]digitar categ {categs}")


def retorna_definicoes(fecha: Fecha, conn: Engine) -> tuple[int]:
    """_summary_

    Args:
        fecha (Fecha): _description_
        conn (Engine): _description_

    Returns:
        tuple[int]: _description_
    """
    if fecha.tipo == 'fil':
        lojas_destino = tuple(
            set(fecha.destino) - set([fecha.filial])
        )
    else:
        lojas_destino = tuple(
            set(lista_loja(fecha.tipo, conn, **fecha.destino))
            -
            set([fecha.filial])
        )
    
    return lojas_destino


def input_definicoes() -> Fecha:
    """_summary_

    Returns:
        Fecha: _description_
    """
    terminal.rule("DEFININDO OS FILTROS")
    filial = IntPrompt.ask(">> loja origem", console=terminal)
    dep = IntPrompt.ask(">> cd destino", console=terminal)
    tipo = Prompt.ask(">> filtro", console=terminal, choices=['gr', 'go', 'uf', 'fil'])
    destino = retorna_tipo(tipo)
    categoria = retorna_categoria()

    return Fecha(filial=filial, dep=dep, tipo=tipo, destino=destino, categoria=categoria)


def gera_estoque(
    fecha: Fecha, 
    conn: Engine, 
    lojas: tuple[int], 
    spinner: str
) -> tuple[pd.DataFrame]:
    """_summary_

    Args:
        fecha (Fecha): _description_
        conn (Engine): _description_
        lojas (tuple[int]): _description_
        spinner (str): _description_

    Returns:
        tuple[pd.DataFrame]: _description_
    """
    terminal.rule("ESTOQUE [ORIGEM / DESTINO]")
    with terminal.status("[bold green]Gerando estoque / categoria ...", spinner=spinner) as estoque:
        df_categoria = load_categoria(conn, fecha.dep)
        terminal.log(f"Categoria: {df_categoria.shape}")

        df_origem = load_estoque('origem', conn, fecha.filial)
        terminal.log(f"Estoque origem: {df_origem.shape}")

        df_destino = load_estoque('destino', conn, *lojas)
        terminal.log(f"Estoque destino: {df_destino.shape}")
        terminal.log("Estoque concluido")

    return (df_destino, df_origem, df_categoria)


def gera_demanda(
    conn: Engine, 
    conn_bk: Engine, 
    lojas: tuple[int], 
    spinner: str
) -> pd.DataFrame:
    """_summary_

    Args:
        conn (Engine): _description_
        conn_bk (Engine): _description_
        lojas (tuple[int]): _description_
        spinner (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    terminal.rule("DEMANDA [CALCULO]")
    with terminal.status("[bold green]Gerando demanda ...", spinner=spinner) as demanda:
        df_dmd = load_venda(conn, conn_bk, *lojas)
        terminal.log(f"Demanda: {df_dmd.shape}")
        terminal.log("Demanda concluido")
    
    return df_dmd


def listar_arquivos():
    terminal.rule("LISTA [ARQUIVOS]")
    t = Tree(label=":file_folder: arquivos")
    for d in Path('data').iterdir():
        if d.suffix == '.parquet':
            t.add(f':book: [bold green]{d.name}') 
    terminal.print(t)


def table_resumo(
    df: pd.DataFrame, 
    dmd: pd.DataFrame, 
    df_origem: pd.DataFrame
) -> None:
    """_summary_

    Args:
        df (pd.DataFrame): _description_
        dmd (pd.DataFrame): _description_
        df_origem (pd.DataFrame): _description_

    Returns:
        _type_: _description_
    """
    terminal.rule("TABELA [RESUMO]")
    table = Table(title="Resumo Filial Destino")
    table.add_column("FILIAL", justify="center", style="cyan", no_wrap=True)

    for col in range(30, 210, 30):
        table.add_column(f"R$ OP_{col}", justify="right", style="green")


    grupo = (
        pd.merge(
            df, dmd, how='inner', on=['FILIAL', 'CODIGO']
        )
        .merge(df_origem.drop(columns='FILIAL'), on='CODIGO', suffixes=['_dest', '_or'])
    )

    def oportunidade(grp: pd.DataFrame):
        for dias in range(30, 210, 30):
            grp[f'OP_{dias}'] = (
                np.where(
                    (dias * grp['DMD']) - grp['QT_ESTOQUE_dest'] <= 0, 0,
                    np.where(
                        grp['QT_ESTOQUE_or'] - ((dias * grp['DMD']) - grp['QT_ESTOQUE_dest']) >=0, 
                        (dias * grp['DMD']) - grp['QT_ESTOQUE_dest'],
                        grp['QT_ESTOQUE_or']
                    )
                ) * grp['VL_CMPG_or']
            )
        
        return grp

    
    grupo = (
        grupo.assign(TOTAL = grupo['QT_ESTOQUE_dest'] * grupo['VL_CMPG_dest'])
        .pipe(oportunidade)
        .drop(columns=['CODIGO', 'VL_CMPG_or', 'VL_CMPG_dest', 'QT_ESTOQUE_or', 'QT_ESTOQUE_dest'])
        .groupby(['FILIAL'])
        .sum()
        .reset_index()
        .sort_values('TOTAL', ascending=False)
        .loc[:, ["FILIAL"] + [f"OP_{c}" for c in range(30, 210, 30)]]
    )

    for row in grupo.itertuples(index=False):
        lin = [f"{c:.2f}" for c in row]
        table.add_row(*lin)

    terminal.print(table)


def menu(
    conn: Engine, 
    conn_bk: Engine,
    dias_dmd: int,
    spinner: str = 'moon'
) -> None:
    """_summary_

    Args:
        conn (Engine): _description_
        conn_bk (Engine): _description_
        dis_dmd (int): _description_
        spinner (str, optional): _description_. Defaults to 'moon'.
    """
    while True:
        md = Markdown(MARKDOWN, code_theme="monokai")
        terminal.print(md)
        fecha = input_definicoes()

        lojas = retorna_definicoes(fecha, conn)
        df_destino, df_origem, df_categoria = gera_estoque(fecha, conn, lojas, spinner)
        df_dmd = gera_demanda(conn, conn_bk, lojas, spinner)
        listar_arquivos()
        table_resumo(df_destino, df_dmd, df_origem)
        distribuicao(df_origem, df_categoria, df_dmd, df_destino, terminal, dias_dmd, fecha.categoria)

        terminal.rule("RETORNO [???]")
        flag = Confirm.ask("[b]>> Deseja fechar outra filial ?[/b]", console=terminal)
        if flag:
            os.system('cls||clear')
            terminal.clear()
            menu(conn, conn_bk, spinner)
        
        break


