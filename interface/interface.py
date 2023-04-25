from rich.console import Console
from rich.markdown import Markdown
from rich.prompt import IntPrompt, Prompt, Confirm
from rich.tree import Tree
from rich.table import Table
from rich.align import Align
from collections import namedtuple
from pathlib import Path
import pandas as pd
from pandas import DataFrame
import numpy as np
import re
import os
import gc
from sqlalchemy.engine import Engine
from utils.utils import (
    lista_loja, 
    load_categoria, 
    load_estoque, 
    load_venda,
    distribuicao,
    create_pdf
)


Fecha = namedtuple('Fecha', 'filial dep tipo destino categoria dmd')

terminal = Console()

DEFAULT_DMD = 360
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

def retorna_wraper_number(start: int, end: int, prompt: str, msg: str, default: int = DEFAULT_DMD):
    while True:
        filial = IntPrompt.ask(prompt, console=terminal, default=default)
        if filial >=start and filial <= end:
            return filial
        terminal.print(f"[prompt.invalid]{msg}")


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
    categs = ['term', 'frac', 'env', 'psico', 'uc']

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
    filial = retorna_wraper_number(1, 9999, ">> filial origem", "digitar filial entre 1 e 9999", 1)
    dep = retorna_wraper_number(1, 10, ">> deposito destino", "digitar deposito entre 1 e 10", 1)
    dmd = retorna_wraper_number(30, DEFAULT_DMD, ">> demanda venda", "digitar demanda entre 30 e 360")
    tipo = Prompt.ask(">> filtro", console=terminal, choices=['gr', 'go', 'uf', 'fil'])
    destino = retorna_tipo(tipo)
    categoria = retorna_categoria()

    return Fecha(filial=filial, dep=dep, tipo=tipo, destino=destino, categoria=categoria, dmd=dmd)


def gera_estoque(
    fecha: Fecha, 
    conn: Engine,
    lojas: tuple[int], 
    spinner: str
) -> tuple[DataFrame]:
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
) -> DataFrame:
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


def listar_arquivos(extensao = '.parquet'):
    terminal.rule("LISTA [ARQUIVOS]")
    t = Tree(label=":file_folder: arquivos")
    for d in Path('data').iterdir():
        if d.suffix == extensao:
            t.add(f':book: [bold green]{d.name}') 
    terminal.print(t)


def c_format(obj):
    if isinstance(obj, float):
        return f"{obj:_.2f}".replace('.', ',').replace('_', '.')
    elif isinstance(obj, int):
        return f"{obj}"
    else:
        return obj


def table_estoque(
    df_origem: DataFrame,
    df_categoria: DataFrame,
    spinner: str
) -> None:

    terminal.rule("TABELA [RESUMO ESTOQUE]")

    table = Table(title="Resumo Filial Origem")
    center_table = Align.center(table)
    table.add_column("FILIAL", justify="center", style="cyan", no_wrap=True)
    table.add_column("CATEG", justify="left", style="cyan", no_wrap=True)
    table.add_column("R$ ESTOQUE", justify="right", style="green")
    
    with terminal.status("[bold green]Calculando Resumo Estoque ...", spinner=spinner) as live:
        grupo = (
            df_origem
            .merge(df_categoria.loc[:, ['CODIGO', 'CATEG']], how='inner', on='CODIGO', suffixes=['_or', '_dest'])
            .assign(CATEG_or = lambda _df: _df['CATEG_or'].where(~_df['CATEG_or'].isnull(), _df['CATEG_dest']))
            .drop(columns=['CATEG_dest'])
            .rename(columns={'CATEG_or': 'CATEG'})
            .assign(VL_ESTOQUE = lambda _df: _df['QT_ESTOQUE'] * _df['VL_CMPG'])
            .groupby(['FILIAL', 'CATEG'], as_index=False)
            .agg({'VL_ESTOQUE': np.sum})
        )

        for row in grupo.itertuples(index=False):
            lin = [c_format(c) for c in row]
            table.add_row(*lin)

    terminal.print(center_table)
    

def table_resumo(
    df: DataFrame, 
    df_categoria: DataFrame,
    dmd: DataFrame, 
    df_origem: DataFrame,
    spinner: str
) -> None:
    """_summary_

    Args:
        df (DataFrame): _description_
        df_categoria (DataFrame): _description_
        dmd (DataFrame): _description_
        df_origem (DataFrame): _description_
        limite_venda (int, optional): _description_. Defaults to 360.

    Returns:
        _type_: _description_
    """
    terminal.rule("TABELA [RESUMO DISTRIBUICAO]")

    table = Table(title="Resumo Filial Destino")
    center_table = Align.center(table)
    table.add_column("FILIAL", justify="center", style="cyan", no_wrap=True)
    table.add_column("CATEG", justify="left", style="cyan", no_wrap=True)
    
    with terminal.status("[bold green]Calculando Resumo ...", spinner=spinner) as live:
        for col in range(30, 210, 30):
            table.add_column(f"R$ OP_{col}", justify="right", style="green")
        table.add_column(f"R$ OP_{DEFAULT_DMD}", justify="right", style="green")

        grupo = (
            pd.merge(
                df, dmd, how='inner', on=['FILIAL', 'CODIGO']
            )
            .merge(
                df_origem.groupby(['CODIGO', 'VL_CMPG'], as_index=False).agg({'QT_ESTOQUE': np.sum})
              , how='inner', on='CODIGO', suffixes=['_dest', '_or'])
            .merge(df_categoria.loc[:, ['CODIGO', 'CATEG']], how='inner', on='CODIGO')
        )
        
        # TODO: Cria oportunidade
        def oportunidade(grp: pd.DataFrame):
            for dias in range(30, 210, 30):
                grp[f'OP_{dias}'] = (
                    np.where(
                        (dias * grp['DMD']) - grp['QT_ESTOQUE_dest'] <= 0, 0,
                        np.where(
                            grp['QT_ESTOQUE_or'] - round((dias * grp['DMD']) - grp['QT_ESTOQUE_dest'], 0) >=0, 
                            round((dias * grp['DMD']) - grp['QT_ESTOQUE_dest'], 0),
                            grp['QT_ESTOQUE_or']
                        )
                    ) * grp['VL_CMPG_or']
                )
            
            # 360 dias
            grp[f'OP_{DEFAULT_DMD}'] = (
                    np.where(
                        (DEFAULT_DMD * grp['DMD']) - grp['QT_ESTOQUE_dest'] <= 0, 0,
                        np.where(
                            grp['QT_ESTOQUE_or'] - round((DEFAULT_DMD * grp['DMD']) - grp['QT_ESTOQUE_dest'], 0) >=0, 
                            round((DEFAULT_DMD * grp['DMD']) - grp['QT_ESTOQUE_dest'], 0),
                            grp['QT_ESTOQUE_or']
                        )
                    ) * grp['VL_CMPG_or']
                )
            
            return grp
    
        grupo = (
            grupo.assign(TOTAL = grupo['QT_ESTOQUE_dest'] * grupo['VL_CMPG_dest'])
            .pipe(oportunidade)
            .drop(columns=['CODIGO', 'VL_CMPG_or', 'VL_CMPG_dest', 'QT_ESTOQUE_or', 'QT_ESTOQUE_dest'])
            .groupby(['FILIAL', 'CATEG'])
            .sum()
            .reset_index()
            .sort_values(['FILIAL', 'CATEG', 'TOTAL'], ascending=False)
            .loc[:, ["FILIAL", "CATEG"] + [f"OP_{c}" for c in range(30, 210, 30)] + [f'OP_{DEFAULT_DMD}']]
        )
        
        for row in grupo.itertuples(index=False):
            lin = [c_format(c) for c in row]
            table.add_row(*lin)

    terminal.print(center_table)


def gera_distribuicao(
    df_origem: pd.DataFrame, 
    df_categoria: pd.DataFrame, 
    df_dmd: pd.DataFrame, 
    df_destino: pd.DataFrame, 
    dias: int,
    spinner: str,
    filtro_categ: list[str] = None
) -> DataFrame:

    terminal.rule("DISTRIBUICAO")
    with terminal.status("[bold green]Distribuindo loja ...", spinner=spinner) as live:
        df = distribuicao(df_origem, df_categoria, df_dmd, df_destino, terminal, dias, filtro_categ)

    return df


def gera_pdf(
    filial: int, 
    spinner: str, 
    df: DataFrame
) -> None:

    terminal.rule("PDF")
    with terminal.status("[bold green]Geracao PDF ...", spinner=spinner) as live:
        create_pdf(filial, terminal, df)


def menu(
    conn: Engine, 
    conn_bk: Engine,
    spinner: str
) -> None:
    """_summary_

    Args:
        conn (Engine): _description_
        conn_bk (Engine): _description_
        spinner (str): _description_
    """
    while True:
        md = Markdown(MARKDOWN, code_theme="monokai")
        terminal.print(md)
        fecha = input_definicoes()

        lojas = retorna_definicoes(fecha, conn)
        df_destino, df_origem, df_categoria = gera_estoque(fecha, conn, lojas, spinner)
        df_dmd = gera_demanda(conn, conn_bk, lojas, spinner)
        listar_arquivos()

        table_estoque(df_origem, df_categoria, spinner)
        table_resumo(df_destino, df_categoria, df_dmd, df_origem, spinner)
        distrib = gera_distribuicao(df_origem, df_categoria, df_dmd, df_destino, fecha.dmd, spinner, fecha.categoria)
        listar_arquivos('.xlsx')
        
        gera_pdf(filial=fecha.filial, spinner=spinner, df=distrib)

        terminal.rule("RETORNO [???]")
        flag = Confirm.ask("[b]>> Deseja fechar outra filial ?[/b]", console=terminal)
        if flag:
            os.system('cls||clear')
            terminal.clear()
            gc.collect()
            menu(conn, conn_bk, spinner)
        
        break