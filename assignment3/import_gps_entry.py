import json
import os
from datetime import datetime
from sqlalchemy import Column, Integer, Float, Text, DateTime, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class GpsEntry(Base):
    __tablename__ = 'gps_entry'
    id = Column(Integer, primary_key=True, autoincrement=True)
    ordem = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    datahora = Column(DateTime)
    velocidade = Column(Float)
    linha = Column(Text)
    datahoraenvio = Column(DateTime)
    datahoraservidor = Column(DateTime)

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

def process_file(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        for item in data:
            if item['linha'] in allowed_linhas:
                latitude = float(item['latitude'].replace(',', '.'))
                longitude = float(item['longitude'].replace(',', '.'))

                if -23.10 <= latitude <= -22.60 and -43.75 <= longitude <= -43.00:
                    item['datahora'] = datetime.fromtimestamp(int(item['datahora']) / 1000.0)
                    item['datahoraenvio'] = datetime.fromtimestamp(int(item['datahoraenvio']) / 1000.0)
                    item['datahoraservidor'] = datetime.fromtimestamp(int(item['datahoraservidor']) / 1000.0)

                    gps_entry = GpsEntry(
                        ordem=item['ordem'],
                        latitude=latitude,
                        longitude=longitude,
                        datahora=item['datahora'],
                        velocidade=float(item['velocidade']),
                        linha=item['linha'],
                        datahoraenvio=item['datahoraenvio'],
                        datahoraservidor=item['datahoraservidor']
                    )
                    session.add(gps_entry)
                    session.commit()

    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON in file {file_path}: {e}")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

root_directory = './data'

for root, dirs, files in os.walk(root_directory):
    for file_name in files:
        if file_name.endswith('.json'):
            file_path = os.path.join(root, file_name)
            print(f"Processing file {file_path}...")
            process_file(file_path)
            os.remove(file_path)
            print(f"Done! {file_path} deleted.")
session.close()
