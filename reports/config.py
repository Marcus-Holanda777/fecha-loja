from reports.models import Rect, Texto, Imagem, Line, inch
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
    ESTILO_FONTE
)


def construir_config(filial, destino, categoria, dados):
    date_rel = datetime.now().strftime('%d/%m/%Y %H:%M')
    cab = ('COD', 'NOME', 'QTD', 'OBSERVACAO')

    # inserir titulo na base de dados
    dados.insert(0, cab)
    
    # 20 linhas por pagina
    tot_pag = len(dados) // 20
    tot_pag = tot_pag + 1 if len(dados) % 20 > 0 else tot_pag

    config = {
        'title': 'Loja %04d' % filial,
        'filename': 'Transferencia_filial_%04d_%04d_%s.pdf' %(filial, destino, categoria),
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
                    , f'Categoria: {categoria}'
                    , f'Data relatorio: {date_rel}'
               ],
               color='#ED1C24'
            )
        ],
        'table_style': TableStyle(
            [   
                ('BACKGROUND', (0, 0), (4, 0), colors.HexColor('#602D7D')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('FONTSIZE', (0, 0), (-1, -1), 12),
            ]
        ),
        'table': Table(
             data=dados, repeatRows=1, spaceBefore=1, rowHeights=len(dados)*[0.4*inch]
        )
        ,'lines_table': len(dados)
    }

    return config