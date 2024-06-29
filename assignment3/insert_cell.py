from sqlalchemy import create_engine, text
from sqlalchemy import Column, Integer, Text, create_engine, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from geoalchemy2 import Geometry

errors = []
Base = declarative_base()

class GridCells(Base):
    __tablename__ = 'grid_cells'
    id = Column(Integer, primary_key=True)
    geom = Column(Geometry(geometry_type='POLYGON', srid=4326))

class LinhaCell(Base):
    __tablename__ = 'linha_cell'
    id = Column(Integer, primary_key=True, autoincrement=True)
    linha = Column(Text)
    garagem_cell_id = Column(Integer, ForeignKey('grid_cells.id'))
    ponto_inicial_cell_id = Column(Integer, ForeignKey('grid_cells.id'))
    ponto_final_cell_id = Column(Integer, ForeignKey('grid_cells.id'))

engine = create_engine('postgresql://localhost:5432/fetranspor')
Session = sessionmaker(bind=engine)
session = Session()

Base.metadata.create_all(engine)

allowed_linhas = [
    '483', '864', '639', '3', '309', '774', '629', '371', '397', '100',
    '838', '315', '624', '388', '918', '665', '328', '497', '878', '355',
    '138', '606', '457', '550', '803', '917', '638', '2336', '399', '298',
    '867', '553', '565', '422', '756', '186012003', '292', '554', '634',
    '232', '415', '2803', '324', '852', '557', '759', '343', '779', '905', '108'
]

for linha in allowed_linhas:
    print(f'Processing linha {linha}...')
    sql_query = f"""
    SELECT
        grid_cells.id
    FROM
        grid_cells
    JOIN
        gps_entry
    ON
        ST_Contains(grid_cells.geom, ST_SetSRID(ST_MakePoint(gps_entry.longitude, gps_entry.latitude), 4326))
        WHERE LINHA = '{linha}'
            AND (
                (DATAHORASERVIDOR >= TO_TIMESTAMP('2024-05-06 00:00:00', 'YYYY-MM-DD HH24:MI')
                AND DATAHORASERVIDOR < TO_TIMESTAMP('2024-05-06 08:00:00', 'YYYY-MM-DD HH24:MI'))
            )
    GROUP BY
        grid_cells.id
    HAVING
        COUNT(*) > 100
    ORDER BY COUNT(*) DESC;
    """

    result = session.execute(text(sql_query)).fetchall()

    if not result or len(result) < 3:
        errors.append(linha)
        continue

    ponto_final_cell_id = result[0][0]
    ponto_inicial_cell_id = result[1][0]
    garagem_cell_id = result[2][0]

    cells = LinhaCell(
        linha=linha,
        garagem_cell_id=garagem_cell_id,
        ponto_inicial_cell_id=ponto_inicial_cell_id,
        ponto_final_cell_id=ponto_final_cell_id
    )
    session.add(cells)
    session.commit()


print(errors)