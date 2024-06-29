from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import folium
import json
import os

engine = create_engine('postgresql://localhost:5432/fetranspor')
Session = sessionmaker(bind=engine)
session = Session()

errors = []

geojson_pontos = 'data/geojson/pontos'
html_pontos = 'data/html/pontos'
os.makedirs(geojson_pontos, exist_ok=True)
os.makedirs(html_pontos, exist_ok=True)

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
    SELECT ST_AsGeoJSON(
        ST_union(
            ARRAY(
                SELECT GEOM
                FROM grid_cells
                WHERE ID IN (
                    SELECT PONTO_INICIAL_CELL_ID FROM LINHA_CELL WHERE LINHA = '371'
                    UNION
                    SELECT PONTO_FINAL_CELL_ID FROM LINHA_CELL WHERE LINHA = '371'
                    UNION
                    SELECT GARAGEM_CELL_ID FROM LINHA_CELL WHERE LINHA = '371'
                )
            )
        )
    );
    """

    result = session.execute(text(sql_query)).fetchone()

    if not result or result[0] is None:
        errors.append(linha + ' - No result found')
        continue

    geom_json = result[0]
    geojson_filename = os.path.join(geojson_pontos, f'geom_{linha}.json')
    with open(geojson_filename, 'w') as f:
        json.dump(json.loads(geom_json), f)

    m = folium.Map(location=[-22.90278, -43.2075], zoom_start=12)
    folium.GeoJson(geojson_filename).add_to(m)
    html_filename = os.path.join(html_pontos, f'map_{linha}.html')
    m.save(html_filename)
    print(f'Done!')

print(errors)
