from salesforcecdpconnector.connection import SalesforceCDPConnection
from sqlalchemy import create_engine, inspect, Column, String, DateTime, Numeric, Date, Integer, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timedelta
import time
import config

conn = SalesforceCDPConnection(
    config.salesforce_url,
    config.salesforce_username,
    config.salesforce_password,
    config.salesforce_client_id,
    config.salesforce_client_secret
)

# PostgreSQL 접속 설정
postgres_host = config.postgres_host
postgres_port = config.postgres_port
postgres_database = config.postgres_database
postgres_user = config.postgres_user
postgres_password = config.postgres_password

# SQLAlchemy 엔진 생성
engine = create_engine(f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}')

# SQLAlchemy 세션 생성
Session = sessionmaker(bind=engine)
session = Session()

# Base 선언
Base = declarative_base()

# 테이블 목록
tables = [
    {
        'source_table_name': 'UnifiedLinkssotIndividualRule__dlm',
        'source_fields': '*',
        'source_condition': '',
        'target_schema_name': 'public',
        'target_table_name': 'unified_link',
        'do_truncate': True,
        'is_kst': True
    },
    # 다른 테이블들 추가 가능
]

cdp_cursor = conn.cursor()

def get_column(field_type):
    if field_type == 'DECIMAL':
        return Column(Numeric)
    elif field_type == 'TIMESTAMP WITH TIME ZONE':
        return Column(DateTime)
    elif field_type == 'DATE':
        return Column(Date)
    else:
        return Column(String)


def create_table_class(target_schema_name, table_name, field_names, field_types):
    class_name = ''.join(word.capitalize() for word in table_name.split('_'))
    table_columns = {
        field_name: get_column(field_type)
        for field_name, field_type in zip(field_names, field_types)
    }
    table_columns['session_id'] = Column(Integer, primary_key=True, autoincrement=True)

    table_class = type(
        class_name,
        (Base,),
        {
            '__tablename__': table_name,
            '__table_args__': {'schema': target_schema_name, 'extend_existing': True},
            **table_columns
        }
    )
    return table_class



def process_table(source_table_name, source_fields, source_condition, target_schema_name, target_table_name, do_truncate, is_kst):
    # 데이터 조회
    print(f"실행 쿼리 : {f'SELECT {source_fields} FROM {source_table_name} {source_condition}'}")
    cdp_cursor.execute(f'SELECT {source_fields} FROM {source_table_name} {source_condition}')
    results = cdp_cursor.fetchall()
    field_names = [desc[0] for desc in cdp_cursor.description]
    field_types = [desc[1] for desc in cdp_cursor.description]

    # 테이블 클래스 생성
    TableClass = create_table_class(target_schema_name, target_table_name, field_names, field_types)

    # 테이블 생성 및 truncate
    inspector = inspect(engine)
    if not inspector.has_schema(target_schema_name):
        session.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{target_schema_name}"'))
    if not inspector.has_table(target_table_name, schema=target_schema_name):
        TableClass.__table__.create(bind=engine, checkfirst=True)
    if do_truncate:
        session.execute(text(f'TRUNCATE TABLE "{target_schema_name}"."{target_table_name}"'))

    # 데이터 변환 및 삽입
    converted_data = []
    for row in results:
        converted_row = {}
        for i, value in enumerate(row):
            field_name = field_names[i]
            if value is None:
                converted_row[field_name] = None
            elif isinstance(value, datetime):
                if is_kst:
                    kst_value = value + timedelta(hours=9)
                    converted_row[field_name] = kst_value
                else:
                    converted_row[field_name] = value
            else:
                converted_row[field_name] = value
        converted_data.append(converted_row)

    # 데이터 삽입
    session.bulk_insert_mappings(TableClass, converted_data)
    session.commit()

if __name__ == '__main__':
    for table in tables:
        start = time.time()
        print(f"{table['source_table_name']} 테이블의 수집을 시작합니다.")
        process_table(
            table['source_table_name'],
            table['source_fields'],
            table['source_condition'],
            table['target_schema_name'],
            table['target_table_name'],
            table['do_truncate'],
            table['is_kst']
        )
        print(f"\"{table['target_table_name']}\"의 적재가 완료되었습니다.")
        end = time.time()
        print(f"{end - start:.5f} sec")