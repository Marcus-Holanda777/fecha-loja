from dataclasses import dataclass
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from typing import Optional

# MEDIDAS DA PAGINA -- E FONTE
LARGURA, ALTURA = A4
ESTILO_FONTE = 'Helvetica'
LINHAS_POR_PAGINA = 35

# VALORES SCALARES
DISTANCIA_X = 0.2
MULTI_EIXO_X = 2.0

# DADOS DOS RETANGULOS
EIXO_X = DISTANCIA_X * inch
ALTURA_RECT = inch
EIXO_Y = ALTURA - (ALTURA_RECT + EIXO_X)
LARGURA_RECT = LARGURA - (EIXO_X * MULTI_EIXO_X)

# DADOS ICONES
ICON_STR = 'reports/logo.png'
ICON_STR_02 = 'reports/caminhao.png'
EIXO_X_ICON = 0.3 * inch
EIXO_Y_ICON = ALTURA - 40.0
LARGURA_ICON = 150.0
ALTURA_ICON = 20.0

@dataclass
class Rect:
    x: int
    y: int
    width: float
    height: float
    fill: int
    stroke: int

@dataclass
class Imagem:
    image: str
    x: float
    y: float
    width: Optional[float] = None
    height: Optional[float] = None
    mask: str = 'auto'

@dataclass
class Texto:
    x: float
    y: float
    size: int
    font: str
    lines: list[str]
    color: str

@dataclass
class Line:
    x1: float
    y1: float
    x2: float
    y2: float
    dash: tuple[int, int]
    width: float
    color: str