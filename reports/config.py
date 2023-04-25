from reports.models import (
    Rect, 
    Texto, 
    Imagem, 
    Line, 
    inch
)
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle
from datetime import datetime
from reports.models import (
    EIXO_X, 
    EIXO_Y, 
    ALTURA, 
    LARGURA_RECT,
    ALTURA_RECT,
    ICON_STR,
    ICON_STR_02,
    EIXO_X_ICON,
    EIXO_Y_ICON,
    LARGURA_ICON,
    ALTURA_ICON,
    ESTILO_FONTE,
    LINHAS_POR_PAGINA
)
from pathlib import Path


def construir_config(
    filial, 
    destino, 
    categoria, 
    dados
) -> dict:

    
    
    date_rel = datetime.now().strftime('%d/%m/%Y %H:%M')
    arq_output = Path() / 'pdf' / f"{filial:04d}_{date_rel[:-6].replace('/', '')}"
    cab = ('COD', 'NOME', 'QTD', 'OBSERVACAO')
    med_cols = (3, 11, 2, 4)

    categ = {
        'UC': 'Ultima Chance', 
        'PSICO': 'Psicotropicos + Antib',
        'FRAC': 'Fracionados',
        'ENV': 'Envelopados',
        'TERM': 'Termolabeis'
    }

    dados.insert(0, cab)
    
    tot_pag = len(dados) // LINHAS_POR_PAGINA
    tot_pag = tot_pag + 1 if len(dados) % LINHAS_POR_PAGINA > 0 else tot_pag
    
    # verifica se arquivo ja existe -- cria se não existir
    if not arq_output.is_dir():
        arq_output.mkdir()


    config = {
        'title': 'Loja %04d' % filial,
        'filename': str(
            arq_output.joinpath(
            'Transferencia_filial_%04d_%04d_%s.pdf' %(filial, destino, categoria)
          )
        ),
        'pags': tot_pag,
        'rects': [
            Rect(
                x=EIXO_X, 
                y=EIXO_Y, 
                width=LARGURA_RECT, 
                height=ALTURA_RECT,
                fill=0, 
                stroke=1
            )
        ],
        'imgs': [
            Imagem(
                image=ICON_STR, 
                x=EIXO_X_ICON, 
                y=EIXO_Y_ICON, 
                width=LARGURA_ICON, 
                height=ALTURA_ICON
            ),
            Imagem(
                image=ICON_STR_02, 
                x=EIXO_X_ICON, 
                y=EIXO_Y_ICON - 50.0, 
                width=LARGURA_ICON - 70.0, 
                height=ALTURA_ICON * 2
            )
        ],
        'lines': [
            Line(
                x1=EIXO_X_ICON + 160.0, 
                y1=ALTURA - 20.0, 
                x2=EIXO_X_ICON + 160.0, 
                y2=ALTURA - 82.0,
                dash=(1, 2),
                width=1.0,
                color='#0056AB'
            )
        ],
        'texts': [
            Texto(
                x=EIXO_X + 200.0, 
                y=EIXO_Y + 50.0,
                size = 16,
                font=ESTILO_FONTE,
                lines=[
                      f'Fechamento loja {filial:04d}, destino filial {destino:04d}'
                    , f'Categoria: {categ.get(categoria)}'
                    , f'Data relatorio: {date_rel}'
               ],
               color='#ED1C24'
            )
        ],
        'table_style': TableStyle(
            [   
                ('BACKGROUND', (0, 0), (4, 0), colors.HexColor('#0056AB')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('BOX', (2,0), (-2, -1), 2, colors.HexColor('#ED1C24')),
                ('ALIGN', (1, 0), (1, -1), 'LEFT'),
                ('FONTSIZE', (0, 0), (-1, -1), 12),
            ]
        ),
        'table': Table(
             data=dados, repeatRows=1, colWidths=[med * (0.4 * inch) for med in med_cols]
        )
        ,'lines_table': len(dados)
    }

    return config