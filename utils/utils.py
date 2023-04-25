import pandas as pd
from pandas import DataFrame
import numpy as np
from pathlib import Path
from sqlalchemy.engine import Engine
from dateutil.relativedelta import relativedelta
from datetime import date
from rich.console import Console
from reports.config import construir_config
from reports.reports import Reports
from PyPDF2 import PdfReader
from datetime import datetime
import zipfile

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
    sel = load_sel('estoque_origem.sql') if tipo == 'origem' else load_sel('estoque.sql')

    if filiais:
        filtro = ','.join(map(str, filiais))
        sel = sel.format(filiais=filtro)

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


def zera_loja(
    df_origem: DataFrame,
    df_dist: DataFrame,
    df_categoria: DataFrame
) -> None:

   # TODO: Gerar base do que ainda falta distribuir
   df_origem = (
    df_origem
     .assign(
        FALTA = lambda _df: _df['QT_ESTOQUE'] - _df['DISTRIB']
     )
     .query('FALTA > 0')
     .merge(
        df_categoria.loc[:, ['CODIGO', 'COD_DV', 'DESCRICAO']],
        how='inner',
        on=['CODIGO']
    )
   )

   # TODO: Encontrar o total de lojas que pode receber o produto
   filiais_recebe = df_dist['FILIAL'].unique().tolist()

   # TODO: Lista com o novo dataframe
   df_novo = []

   # TODO: Colunas no novo dataframe
   cols = ['COD_DV', 'DESCRICAO', 'CATEG']

   # TODO: Definir o item e distribuir o total
   # pela quantidade de lojas
   for enviar in df_origem.itertuples():
       # TODO: Pegar a posicao da linha no daaframe
       index = enviar.Index

       # TODO: Verificar a quantidade de lojas
       len_lojas = (
           enviar.FALTA if len(filiais_recebe) >= enviar.FALTA
           else len(filiais_recebe)
       )

       is_receb = (enviar.FALTA % len_lojas) == 0
       while not is_receb:
           len_lojas -= 1
           is_receb = (enviar.FALTA % len_lojas) == 0
       
       # TODO: Defini a quantidade e faz a distribuicao
       qtd_envia = int(enviar.FALTA / len_lojas)

       for fil in filiais_recebe[:len_lojas]:
           df = (
              df_origem.loc[[index], cols]
              .assign(FILIAL = fil)
              .assign(DISTRIB = qtd_envia)
              .rename(columns={'CATEG': 'CATEG_ORIGEM'})
            )
           df_novo.append(df)
    
   df_novo = pd.concat(df_novo)

   return pd.concat([df_dist, df_novo])


def distribuicao(
    df_origem: pd.DataFrame, 
    df_categoria: pd.DataFrame, 
    df_dmd: pd.DataFrame, 
    df_destino: pd.DataFrame, 
    terminal: Console,
    dias: int,
    filtro_categ: list[str] = None
) -> DataFrame:
    
    # TODO: Verificar se tem UC na origem
    if 'UC' in set(filtro_categ):
        lista_codigo = (
            df_origem.merge(df_categoria.loc[:, ['CODIGO', 'CATEG']], on=['CODIGO'], suffixes=['_or', '_cat'])
            .assign(CATEG_or = lambda _df: _df['CATEG_or'].where(~_df['CATEG_or'].isnull(), _df['CATEG_cat']))
            .drop(columns=['CATEG_cat'])
            .rename(columns={'CATEG_or': 'CATEG'})
            .loc[lambda _df: _df['CATEG'].isin(filtro_categ), 'CODIGO']
            .unique()
            .tolist()
        )
    else:
        lista_codigo = (
            df_categoria.loc[df_categoria['CATEG'].isin(filtro_categ), 'CODIGO']
            .unique()
            .tolist()
        )

    # TODO: Realizar o join entre as tabelas -- calcular oportunidade
    df_dist = (
        df_destino.merge(
            df_categoria.loc[df_categoria['CODIGO'].isin(lista_codigo), :], 
            how='inner', on='CODIGO'
        )
        .merge(df_dmd, how='left', on=['FILIAL', 'CODIGO'])
        .assign(DMD = lambda _df: _df['DMD'].fillna(0))
        .assign(RECEBER = lambda _df: np.where(
              (dias * _df['DMD']) - _df['QT_ESTOQUE'] <= 0, 0,
              round((dias * _df['DMD']) - _df['QT_ESTOQUE'], 0)
              )
         ,ID_MOV = 0
         ,CATEG_ORIGEM = None
         ,DISTRIB = 0
         ,SALDO_ANTERIOR = 0
         ,CUSTO_ORIGEM = 0.0
        )
        .astype(
            {
                'RECEBER': 'int', 
                'ID_MOV': 'int', 
                'DISTRIB': 'int', 
                'SALDO_ANTERIOR': 'int', 
                'CUSTO_ORIGEM': 'float'
            }
        )
        .sort_values(['RECEBER'], ascending=False)
    )
    
    # TODO: Criar coluna distribuido na origem -- filtrando dados da origem
    df_origem = (
        df_origem
        .merge(df_categoria.loc[:, ['CODIGO', 'CATEG']], on=['CODIGO'], suffixes=['_or', '_cat'])
        .assign(CATEG_or = lambda _df: _df['CATEG_or'].where(~_df['CATEG_or'].isnull(), _df['CATEG_cat']))
        .drop(columns=['CATEG_cat'])
        .rename(columns={'CATEG_or': 'CATEG'})
        .loc[lambda _df: _df['CATEG'].isin(filtro_categ), :]
        .assign(DISTRIB = 0).astype({'DISTRIB': 'int'})
    )
    
    # TODO: Fazer a distribuicao
    id_movimento = 1
    for enviar in df_origem.itertuples():
        filial_origem = enviar.FILIAL
        codigo_env = enviar.CODIGO
        qtd_env = enviar.QT_ESTOQUE
        index = enviar.Index
        custo = enviar.VL_CMPG
        categ_or = enviar.CATEG
            
        # TODO: Filtrando com base no produto
        filtro = (df_dist['CODIGO'] == codigo_env) & (df_dist['RECEBER'] > 0)
        filtro_prod = df_dist.loc[filtro, :]
            
        # TODO: Se não tiver oportunidade pular para o proximo item
        if len(filtro_prod) == 0:
            continue
            
        for receber in filtro_prod.itertuples():
            qtd_recebe = receber.RECEBER
            filial_recebe = receber.FILIAL
            codigo_recebe = receber.CODIGO
            categ_dest = receber.CATEG
                
            # TODO: Caso o Produto Zere é pra sair do loop
            if qtd_env == 0:
                break
                
            filtro_dist = (df_dist['CODIGO'] == codigo_recebe) & (df_dist['FILIAL'] == filial_recebe)

            if qtd_env > qtd_recebe:
                diminuir = qtd_recebe
            else:
                diminuir = qtd_env
                
            df_dist.loc[filtro_dist, 'ID_MOV'] = id_movimento
            df_dist.loc[filtro_dist, 'CATEG_ORIGEM'] = (
                categ_or if categ_or is not None 
                else categ_dest 
            )
            df_dist.loc[filtro_dist, 'RECEBER'] -= diminuir
            df_dist.loc[filtro_dist, 'DISTRIB'] += diminuir
            df_dist.loc[filtro_dist, 'SALDO_ANTERIOR'] = qtd_env
            df_dist.loc[filtro_dist, 'CUSTO_ORIGEM'] = custo 
                
            id_movimento += 1
            qtd_env -= diminuir
            df_origem.loc[index, 'DISTRIB'] += diminuir
        
    terminal.log('Distribuicao concluida')
    df_dist.to_excel(f'data/DISTRIBUIDO_{filial_origem}.xlsx', index=False)
    terminal.log('Exportado, distribuicao')
    df_origem.to_excel(f'data/ORIGEM_{filial_origem}.xlsx', index=False)
    terminal.log('Exportado origem')

    terminal.log('Zerando loja origem')
    df_dist = zera_loja(df_origem, df_dist, df_categoria)
    terminal.log('Exportando zera loja')
    df_dist.to_excel(f'data/DISTRIBUIDO_ZERO_{filial_origem}.xlsx', index=False)

    return df_dist


def get_pags(filial, destino, categoria):
    date_rel = datetime.now().strftime('%d/%m/%Y %H:%M')
    arq_output = Path() / 'pdf' / f"{filial:04d}_{date_rel[:-6].replace('/', '')}"
    arquivo =str(
            arq_output
            .joinpath(
                'Transferencia_filial_%04d_%04d_%s.pdf' % (filial, destino, categoria)
        )
    )
    
    meta = PdfReader(arquivo)
    return len(meta.pages)


def zip_categoria(
    filial, 
    categ
) -> None:
    
    categ_name = {
        'UC': 'ULTIMA_CHANCE',
        'TERM': 'TERMOLABEIS',
        'FRAC': 'FRACIONADOS',
        'ENV': 'FRACIONADOS',
        'PSICO': 'PSICOTROPICOS_ANTIBIOTICOS'
    }

    date_rel = datetime.now().strftime('%d/%m/%Y %H:%M')
    pasta = Path() / 'pdf' / f"{filial:04d}_{date_rel[:-6].replace('/', '')}"
    file_zip = pasta / f'{filial:04d}_{categ_name.get(categ)}.zip'

    if categ == 'FRAC':
        compilados = ["**/*ENV*.pdf", "**/*FRAC*.pdf"]
    else:
        compilados = [f"**/*{categ}*.pdf"]

    arquivos = [
        arq for busca in compilados 
        for arq in pasta.glob(busca)
    ]

    with zipfile.ZipFile(file_zip, 'w') as zip:
        for arq in arquivos:
            zip.write(arq, arq.name, compress_type=zipfile.ZIP_DEFLATED)


def create_pdf(
    filial: int, 
    terminal: Console, 
    df: DataFrame
) -> None:

    grups = df.sort_values(['FILIAL', 'CATEG_ORIGEM']).groupby(['FILIAL', 'CATEG_ORIGEM'])
    
    categs = set()

    for keys, data in grups:
        destino, categoria = keys
        dados = list(
            data
            .groupby(['COD_DV', 'DESCRICAO'], as_index=False)
            .agg({'DISTRIB': 'sum'})
            .sort_values('DESCRICAO')
            .to_records(index=False)
        )
 
        categs.add(categoria) # para zipar os arquivos
        config = construir_config(filial=filial, destino=destino, categoria=categoria, dados=dados)
        Reports(**config).go()

        pags = get_pags(filial, destino, categoria)
        terminal.log(f":book: [bold red]Destino -> {destino:04d}[/] {categoria}, pags: {pags}")

    categs.remove('ENV')
    for cat in categs:
        zip_categoria(filial, cat)
        terminal.log(f":package: [bold green]Zip -> {cat}[/]")