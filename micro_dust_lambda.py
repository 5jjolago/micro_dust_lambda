import json
import xmltodict
import urllib.request
import logging
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def classify_concentration(concentration, pollutant):
    # 입력 값이 숫자로 변환 가능한지 확인
    try:
        concentration = float(concentration)
    except ValueError:
        # 변환할 수 없는 경우
        return '점검 중'

    # 대기오염 상중하 처리 
    if pollutant == 'CO':
        if concentration < 4.5:
            return '상'
        elif concentration < 9.5:
            return '중'
        else:
            return '하'
    elif pollutant == 'PM10':
        if concentration < 15.1:
            return '상'
        elif concentration < 35.1:
            return '중'
        else:
            return '하'


def lambda_handler(event, context):
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')
    
    es = Elasticsearch(
        "https://kibana.nalraon.kr:9200",
        ssl_assert_fingerprint=CERT_FINGERPRINT,
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )

    district_code_array = [111123, 111121, 111131, 111142, 111141,
                          111152, 111151, 111161, 111291, 111171,
                          111311, 111181, 111191, 111201, 111301,
                          111212, 111221, 111281, 111231, 111241,
                          111251, 111262, 111261, 111273, 111274]
    
    all_data = []

    for district_code in district_code_array:
        url = f'http://openapi.seoul.go.kr:8088/{api_key}/xml/ListAirQualityByDistrictService/1/5/{district_code}/'

        try:
            response = urllib.request.urlopen(url)
            xml_data = response.read().decode("utf-8")
            parsed_data = xmltodict.parse(xml_data)
            if 'RESULT' in parsed_data and parsed_data['RESULT']['CODE'] != 'INFO-000':
                print(f'Error: {parsed_data["RESULT"]["MESSAGE"]}')
            elif 'ListAirQualityByDistrictService' in parsed_data:
                response_data = parsed_data['ListAirQualityByDistrictService']['row']
                if isinstance(response_data, dict):
                    response_data = [response_data]
                all_data.extend(response_data)
            else:
                print(f'Error: ListAirQualityByDistrictService not found in API response. Response: {parsed_data}')

        except Exception as e:
            print(f'Error: {str(e)}')

    docs = []

    for micro_dust_data in all_data:
        co_grade = classify_concentration(micro_dust_data['CARBON'], 'CO')
        pm10_grade = classify_concentration(micro_dust_data['PM10'], 'PM10')
        
        docs.append({
            '_index': '대기오염도_실시간_데이터',
            '_id': micro_dust_data['MSRSTENAME'], 
            '_source': {
                "대기오염도_측정날짜": micro_dust_data['MSRDATE'],
                "지역_행정코드": micro_dust_data['MSRADMCODE'],
                "지역": micro_dust_data['MSRSTENAME'],
                "대기환경등급": micro_dust_data['GRADE'],
                "일산화탄소_농도": micro_dust_data['CARBON'],
                "일산화탄소_분류": co_grade,
                "미세먼지_농도": micro_dust_data['PM10'],
                "미세먼지_분류": pm10_grade
            }
        })

    try:
        result = helpers.bulk(es, docs)
        print("Indexed %d documents." % result[0])
        if result[1]:
            print("But there were errors.")
            for error in result[1]:
                print(error)
    except Exception as e:
        print(f'Error: {str(e)}')

    return {
        'statusCode': 200,
        'body': json.dumps(all_data, ensure_ascii=False, indent=2)
    }