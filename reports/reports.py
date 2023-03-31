from reportlab.pdfgen.canvas import Canvas
from reportlab.platypus import Table, TableStyle, SimpleDocTemplate
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from reportlab.lib import colors
from models import Rect, Imagem, Texto, Line
from models import EIXO_X 
from dataclasses import asdict


class Reports:
    def __init__(
        self,
        title: str,
        filename: str,
        pags: int,
        rects: list[Rect],
        imgs:  list[Imagem],
        lines: list[Line],
        texts: list[Texto],
        table_style: TableStyle,
        table: Table,
        lines_table: int
    ) -> None:

        self.title = title
        self.filename = filename
        self.pags = pags
        self.rects = rects
        self.imgs = imgs
        self.lines = lines
        self.texts = texts
        self.table_style = table_style
        self.table = table
        self.lines_table = lines_table

    def draw_rects(self, canvas: Canvas) -> None:
        for rect in self.rects:
            canvas.saveState()
            canvas.rect(**asdict(rect))
            canvas.restoreState()
    
    def import_imgs(self, canvas: Canvas) -> None:
        for img in self.imgs:
            canvas.saveState()
            canvas.drawImage(**asdict(img))
            canvas.restoreState()

    def draw_lines(self, canvas: Canvas) -> None:
        for line in self.lines:
            canvas.saveState()
            canvas.setStrokeColor(colors.HexColor(line.color))
            canvas.setDash(line.dash)
            canvas.setLineWidth(line.width)

            position = {k:v for k, v in asdict(line).items() if k[0] in 'yx'}
            canvas.line(**position)
            canvas.restoreState()

    def draw_texts(self, canvas: Canvas) -> None:
        for text in self.texts:
            canvas.saveState()
            texto = canvas.beginText(x=text.x, y=text.y)
            texto.setFont(text.font, size=text.size)
            texto.setFillColor(colors.HexColor(text.color))
            texto.textLines(text.lines)
            canvas.drawText(texto)
            canvas.restoreState()
    
    def doc_rodape(self, canvas: Canvas, doc: SimpleDocTemplate):
        canvas.saveState()
        canvas.drawString(EIXO_X, EIXO_X, "Pagina: %02d de %02d" % (doc.page, self.pags))
        canvas.restoreState()

    def doc_first_page(
        self, 
        canvas: Canvas, 
        doc: SimpleDocTemplate
    ) -> None:
        
        self.draw_rects(canvas)
        self.import_imgs(canvas)
        self.draw_lines(canvas)
        self.draw_texts(canvas)
        self.doc_rodape(canvas, doc)
    
    def go(self) -> None:
        pdf = SimpleDocTemplate(
            filename=self.filename,
            pagesize=A4,
            title=self.title,
            topMargin=1.9 * inch
        )

        self.tabela.setStyle(self.table_style)

        # ALTERAR A COR
        for i in range(1, self.lines_table):
            if i % 2 == 0:
                bc = colors.HexColor('#F2F2F2')
            else:
                bc = colors.white

            ts = TableStyle(
                [('BACKGROUND', (0, i), (-1, i), bc)]
            )

            self.tabela.setStyle(ts)

        # Spacer(1, 1.0 * inch)
        elm = []
        elm.append(self.table)
        pdf.build(elm, onFirstPage=self.doc_first_page, onLaterPages=self.doc_first_page)
