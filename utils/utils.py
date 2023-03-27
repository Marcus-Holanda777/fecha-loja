import pandas as pd
import numpy as np
from pathlib import Path
from sqlalchemy.engine import Engine
from dateutil.relativedelta import relativedelta
from datetime import date
from rich.console import Console


PATH_SEL = Path() / 'data'


def gera_dataframe(
    sel: str, 
    conn: Engine, 
    name: str
) -> pd.DataFrame:
    """_summary_

    Args:
        sel (str): _description_
        conn (Engine): _description_
        name (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    df = pd.read_sql(sel, con=conn)
    df.to_parquet(PATH_SEL / f'{name}.parquet')

    return df


def load_sel(arquivo: str) -> str:
    """retorna consulta sql

    Args:
        arquivo (str): nome do arquivo 
        referente a consulta

    Returns:
        str: consulta sql
    """

    return (
        PATH_SEL / arquivo
    ).read_text()


def lista_loja(tipo: str, conn: Engine, **kwargs):
    """listar lojas com base em filtros

    Args:
        tipo (str): opcoes do tipo de filtro [gr,go,uf]
        conn (Engine): conexao banco de dados

    Returns:
        tuple[int]: uma tupla de filiais
    """

    kwargs = {c.lower(): k for c, k in kwargs.items()}
    sel = load_sel(f'{tipo}_filtro.sql').format(**kwargs)

    with conn.connect() as query:
        rst = query.execute(
            sel
        ).fetchall()

        rst = tuple([c[0] for c in rst])

    return rst


def load_estoque(tipo: str, conn: Engine, *filiais) -> pd.DataFrame:
    """Estoque das lojas

    Args:
        tipo (str): loja origem ou destino
        conn (Engine): conexao banco de dados

    Returns:
        pd.DataFrame: dataframe pandas com os dados das filiais
    """
    sel = load_sel('estoque.sql')

    if filiais:
        filtro = ','.join(map(str, filiais))
        sel = sel.format(filiais=filtro)

    sel = sel.replace('--', '') if tipo == 'origem' else sel
    df = gera_dataframe(sel, conn, f'estoque_{tipo}')

    return df


def load_categoria(
    conn: Engine, 
    deposito: int
) -> pd.DataFrame:
    """_summary_

    Args:
        conn (Engine): _description_
        deposito (int): _description_

    Returns:
        pd.DataFrame: _description_
    """
    sel = load_sel('categoria.sql').format(deposito=deposito)
    df = gera_dataframe(sel, conn, f'categoria_dep_{deposito:02d}')

    return df


def ultimo_bk(conn: Engine) -> str:
    """_summary_

    Args:
        conn (Engine): _description_

    Returns:
        str: _description_
    """
    sel_bk = '''
        SELECT top 1
            'cosmosdw.' + rtrim(d.name) as name
        from sys.databases d
        where d.name like 'cosmos_v14b_%'
        order by d.create_date desc
    '''

    with conn.connect() as con:
        return (
            con.execute(
                sel_bk
            ).scalar()
        )


def periodos(conn: Engine, *filiais) -> dict[str]:
    """_summary_

    Args:
        conn (Engine): _description_

    Returns:
        dict[str]: _description_
    """
    def add_30(dt: date, days: int):
        return (
            (
                dt + relativedelta(days=days)
            ).strftime('%Y-%m-%d')
        )

    start_date = date.today() - relativedelta(days=181)
    keys = ['@inicio', '@_30', '@_60', '@_90', '@_120', '@_150', '@fim']
    values = [add_30(start_date, days=days) for days in range(30, 210, 30)]

    values.append(start_date.strftime('%Y-%m-%d'))
    data = dict(zip(keys, sorted(values)))

    data['@backup'] = ultimo_bk(conn)
    data['@filial'] = ','.join(map(str, filiais))

    return data


def load_venda(
    conn: Engine, 
    conn_bk: Engine, 
    *filiais
) -> pd.DataFrame:
    """_summary_

    Args:
        conn (Engine): _description_
        conn_bk (Engine): _description_

    Returns:
        pd.DataFrame: _description_
    """
    filtros = periodos(conn_bk, *filiais)
    sel = load_sel('dmd.sql').format(**filtros)

    df = gera_dataframe(sel, conn, 'dmd_vendas')

    return df


def distribuicao(
    df_origem: pd.DataFrame, 
    df_categoria: pd.DataFrame, 
    df_dmd: pd.DataFrame, 
    df_destino: pd.DataFrame, 
    terminal: Console,
    dias = 30,
    filtro_categ: list[str] = None
) -> None:

    # TODO: Realizar o join entre as tabelas -- calcular oportunidade
    df_dist = (
        df_destino.merge(
            df_categoria.loc[df_categoria['CATEG'].isin(filtro_categ), :], 
            how='inner', on='CODIGO'
        )
        .merge(df_dmd, how='inner', on=['FILIAL', 'CODIGO'])
        .assign(RECEBER = lambda _df: np.where(
              ((dias * _df['DMD']) - _df['QT_ESTOQUE']) <= 0, 0,
              round((dias * _df['DMD']) - _df['QT_ESTOQUE'], 0)
              )
         ,DISTRIB = 0
        )
        .astype({'RECEBER': 'int', 'DISTRIB': 'int'})
        .sort_values(['RECEBER'], ascending=False)
    )
    
    # TODO: Criar coluna distribuido na origem
    df_origem = df_origem.assign(DISTRIB = 0).astype({'DISTRIB': 'int'})
    
    # TODO: Fazer a distribuicao

    with terminal.status("[bold green]Distribuindo ...") as espera:
        for enviar in df_origem.itertuples():
            codigo_env = enviar.CODIGO
            qtd_env = enviar.QT_ESTOQUE
            index = enviar.Index
            
            # TODO: Filtrando com base no produto
            filtro = (df_dist['CODIGO'] == codigo_env) & (df_dist['RECEBER'] >= qtd_env)
            filtro_prod = df_dist.loc[filtro, :]
            
            # TODO: Se não tiver oportunidade pular para o proximo item
            if len(filtro_prod) == 0:
                continue

            for receber in filtro_prod.itertuples():
                qtd_recebe = receber.RECEBER
                filial_recebe = receber.FILIAL
                codigo_recebe = receber.CODIGO
                
                # TODO: Caso o Produto Zere é pra sair do loop
                if qtd_env == 0:
                    break
                
                filtro_dist = (df_dist['CODIGO'] == codigo_recebe) & (df_dist['FILIAL'] == filial_recebe)

                if qtd_env > qtd_recebe:
                   diminuir = qtd_recebe
                   df_dist.loc[filtro_dist, 'RECEBER'] -= qtd_recebe
                   df_dist.loc[filtro_dist, 'DISTRIB'] += qtd_recebe
                else:
                   diminuir = qtd_env
                   df_dist.loc[filtro_dist, 'RECEBER'] -= qtd_env
                   df_dist.loc[filtro_dist, 'DISTRIB'] += qtd_env

                qtd_env -= diminuir
                df_origem.loc[index, 'DISTRIB'] += diminuir
        
        terminal.log('Distribuicao conluida')

        df_dist.to_excel('DISTRIBUIDO.xlsx')
        terminal.log('Tabela distribuido')
        
        df_origem.to_excel('ORIGEM.xlsx')
        terminal.log('Tabela origem')
        
