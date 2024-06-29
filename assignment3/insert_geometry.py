from sqlalchemy import create_engine, text
from sqlalchemy import Column, Integer, Float, Text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from geoalchemy2 import Geometry

errors = []
Base = declarative_base()

class LinhaGeometry(Base):
    __tablename__ = 'linha_geometry'
    id = Column(Integer, primary_key=True, autoincrement=True)
    linha = Column(Text)
    geometry = Column(Geometry(geometry_type='MULTIPOLYGON', srid=4326))
    dia_util = Column(Text)

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
    WITH CLUSTERED_POINTS AS (
       SELECT
           ID,
           LONGITUDE,
           LATITUDE,
           ST_CLUSTERDBSCAN(ST_MAKEPOINT(LONGITUDE, LATITUDE)::GEOMETRY, EPS := .0008, MINPOINTS := 5) OVER () AS CLUSTER_ID
       FROM gps_entry
       WHERE LINHA = '{linha}'
        AND (
            (DATAHORASERVIDOR >= TO_TIMESTAMP('2024-05-06 08:00:00', 'YYYY-MM-DD HH24:MI')
            AND DATAHORASERVIDOR < TO_TIMESTAMP('2024-05-06 22:00:00', 'YYYY-MM-DD HH24:MI'))
        )
    ),
    MAIN_CLUSTER AS (
        SELECT CLUSTER_ID
        FROM CLUSTERED_POINTS
        GROUP BY CLUSTER_ID
        ORDER BY COUNT(*) DESC
        LIMIT 3
    )
    SELECT (
        ST_UNION(
                ST_ALPHASHAPE(
                        ST_BUFFER(ST_MAKEPOINT(LONGITUDE, LATITUDE)::GEOGRAPHY, 30)::GEOMETRY,
                        100.1,
                        TRUE
                    )
            )::GEOMETRY
    ) AS GEOM
    FROM CLUSTERED_POINTS
    WHERE CLUSTER_ID IN (SELECT CLUSTER_ID FROM MAIN_CLUSTER);
    """

    result = session.execute(text(sql_query)).fetchone()

    if not result or result[0] is None:
        errors.append(linha + ' - No result found (dia util)')
        continue

    geom = result[0]

    geometry = LinhaGeometry(
        linha=linha,
        geometry=geom,
        dia_util='Y'
    )
    session.add(geometry)
    session.commit()

for linha in allowed_linhas:
    print(f'Processing linha {linha}...')
    sql_query = f"""
    WITH CLUSTERED_POINTS AS (
       SELECT
           ID,
           LONGITUDE,
           LATITUDE,
           ST_CLUSTERDBSCAN(ST_MAKEPOINT(LONGITUDE, LATITUDE)::GEOMETRY, EPS := .0008, MINPOINTS := 5) OVER () AS CLUSTER_ID
       FROM gps_entry
       WHERE LINHA = '{linha}'
        AND (
            (DATAHORASERVIDOR >= TO_TIMESTAMP('2024-05-05 08:00:00', 'YYYY-MM-DD HH24:MI')
            AND DATAHORASERVIDOR < TO_TIMESTAMP('2024-05-05 22:00:00', 'YYYY-MM-DD HH24:MI'))
        )
    ),
    MAIN_CLUSTER AS (
        SELECT CLUSTER_ID
        FROM CLUSTERED_POINTS
        GROUP BY CLUSTER_ID
        ORDER BY COUNT(*) DESC
        LIMIT 3
    )
    SELECT (
        ST_UNION(
                ST_ALPHASHAPE(
                        ST_BUFFER(ST_MAKEPOINT(LONGITUDE, LATITUDE)::GEOGRAPHY, 30)::GEOMETRY,
                        100.1,
                        TRUE
                    )
            )::GEOMETRY
    ) AS GEOM
    FROM CLUSTERED_POINTS
    WHERE CLUSTER_ID IN (SELECT CLUSTER_ID FROM MAIN_CLUSTER);
    """

    result = session.execute(text(sql_query)).fetchone()

    if not result or result[0] is None:
        errors.append(linha + ' - No result found (fim de semana)')
        continue

    geom = result[0]

    geometry = LinhaGeometry(
        linha=linha,
        geometry=geom,
        dia_util='N'
    )
    session.add(geometry)
    session.commit()
print(errors)