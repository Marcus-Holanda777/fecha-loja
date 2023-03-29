from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from reportlab.lib import colors
from models import Rect, Imagem, Texto
from models import (
    EIXO_X,
    EIXO_Y, 
    LARGURA,
    ALTURA, 
    LARGURA_RECT, 
    ALTURA_RECT,
    ICON_STR,
    ALTURA_ICON,
    LARGURA_ICON,
    EIXO_X_ICON,
    EIXO_Y_ICON,
    ESTILO_FONTE
)
from dataclasses import asdict


# TODO: Criar um simples PDF
pdf = canvas.Canvas("teste.pdf", pagesize=A4)

# TODO: Criando e adicionado um relangulo
pdf.saveState()
rect = Rect(x=EIXO_X, y=EIXO_Y, width=LARGURA_RECT, height=ALTURA_RECT, fill=0, stroke=1)
pdf.rect(**asdict(rect))
pdf.restoreState()

# TODO: Importar e adicionar uma imagem
pdf.saveState()
logo = Imagem(image=ICON_STR, x=EIXO_X_ICON, y=EIXO_Y_ICON, width=LARGURA_ICON, height=ALTURA_ICON)
pdf.drawImage(**asdict(logo))
caminhao = Imagem(image='reports/caminhao.png', x=EIXO_X_ICON, y=EIXO_Y_ICON - 50.0, width=80.0, height=40.0)
pdf.drawImage(**asdict(caminhao))
pdf.restoreState()

# TODO: Desenhando um linha
pdf.saveState()
pdf.setStrokeColor(colors.HexColor("#0056AB"))
pdf.setDash(1, 2)
pdf.setLineWidth(1.0)
pdf.line(EIXO_X_ICON + 160.0, ALTURA - 20.0, EIXO_X_ICON + 160.0, ALTURA - 82.0)
pdf.restoreState()

# TODO: Escrever um texto com o
# Numero da loja e nome
# Categoria dos produtos -- UC --

pdf.saveState()
texto = pdf.beginText(x=EIXO_X + 200.0, y=EIXO_Y + 50.0)
texto.setFont(ESTILO_FONTE, size=16)
texto.setFillColor(colors.HexColor("#ED1C24"))
texto.textLines(
    '''
    Fechamento loja 0760, destino filial 0175
    Categoria: Ultima chance
    Data relatorio: 29/03/2023 16:28
    '''
)
pdf.drawText(texto)
pdf.restoreState()

# TODO: Salvando o arquivo
pdf.save()