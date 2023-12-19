from bs4 import BeautifulSoup
import requests
import mysql.connector
import json
import os
from decouple import config

# 현재 스크립트의 디렉토리를 가져와서 현재 작업 디렉토리로 설정
current_script_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_script_directory)

# sportPigCrawlingDeploy 디렉토리로 이동
os.chdir(os.path.abspath(os.path.join(current_script_directory, '..')))

# 환경 변수에서 값을 가져오기
web_db_endpoint = config('Web_DB_EndPoint')
web_db_id = config('Web_DB_ID')
web_db_pwd = config('Web_DB_PWD')

file_path = './k_league_player_id.json'

# JSON 파일 읽기
with open(file_path, "r", encoding="utf-8") as json_file:
    player_id_list = json.load(json_file)

# player_id_list 변수에 JSON 데이터가 저장됩니다
print(len(player_id_list))
player_detail_list = []
# 웹 페이지의 URL
for i in range(len(player_id_list)):
    url = f"https://www.kleague.com/record/playerDetail.do?playerId={player_id_list[i]}"

    # 웹 페이지 내용을 요청하여 가져옵니다.
    response = requests.get(url)
    html = response.text

    # BeautifulSoup 객체를 생성합니다.
    soup = BeautifulSoup(html, 'html.parser')

    # 선수 정보를 포함하는 div 요소를 찾습니다.
    player_info_element = soup.find("div", class_="f-wrap player")

    # name = player_info_element.find("span", class_="name").text
    # window 에선 text -> string 바꿔줘야함
    name = player_info_element.find("th", string="이름").find_next('td').text
    # 소속 구단을 가져옵니다.
    team = player_info_element.find("th", string="소속구단").find_next("td").text
    # 포지션을 가져옵니다.
    position = player_info_element.find(
        "th", string="포지션").find_next("td").text

    # 배번을 가져옵니다. ㅁ
    number = player_info_element.find("th", string="배번").find_next("td").text

    # 국적을 가져옵니다.
    nationality = player_info_element.find(
        "th", string="국적").find_next("td").text

    height = player_info_element.find("th", string="키").find_next("td").text

    # 몸무게를 가져옵니다.
    weight = player_info_element.find("th", string="몸무게").find_next("td").text

    # 생년월일을 가져옵니다.
    birthdate = player_info_element.find(
        "th", string="생년월일").find_next("td").text

    player_detail = {
        'k_league_player_id': player_id_list[i],
        'player_name': name,
        'player_team': team,
        'player_position': position,
        'player_number': number,
        'player_nationality': nationality,
        'player_height': height,
        'player_weight': weight,
        'player_birthdate': birthdate
    }
    player_detail_list.append(player_detail)
# db connect
db_config = {
    "host": web_db_endpoint,
    "port": 3306,
    "user": web_db_id,
    "password": web_db_pwd,
    "database": "sportInfo"
}

# 데이터베이스 연결 생성
db = mysql.connector.connect(**db_config)

try:
    # 커서 생성
    cursor = db.cursor()

    # player_data 테이블 생성 (playerId 컬럼은 AUTO_INCREMENT로 설정)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS k_league_player_detail (
        k_league_player_id VARCHAR(20) PRIMARY KEY , 
        player_name VARCHAR(50) NOT NULL ,
        player_team VARCHAR(50) NOT NULL ,
        player_position VARCHAR(50) NOT NULL ,
        player_number VARCHAR(50) NOT NULL ,
        player_nationality VARCHAR(50) NOT NULL ,
        player_height VARCHAR(50) NOT NULL ,
        player_weight VARCHAR(50) NOT NULL ,
        player_birthdate VARCHAR(50) NOT NULL 
    );
    """

    cursor.execute(create_table_sql)
    db.commit()

    # JSON 데이터를 MySQL 데이터베이스에 삽입
    for player_detail in player_detail_list:
        sql = "INSERT INTO k_league_player_detail (k_league_player_id , player_name, player_team, player_position, player_number , player_nationality ,player_height , player_weight , player_birthdate) VALUES (%s ,%s, %s, %s, %s , %s , %s, %s , %s)"
        val = (player_detail["k_league_player_id"],
               player_detail["player_name"],
               player_detail["player_team"],
               player_detail["player_position"],
               player_detail["player_number"],
               player_detail["player_nationality"],
               player_detail["player_height"],
               player_detail["player_weight"],
               player_detail["player_birthdate"])
        cursor.execute(sql, val)
        db.commit()

except mysql.connector.Error as err:
    print(f"Error: {err}")

finally:
    # 커넥션 닫기
    if db.is_connected():
        cursor.close()
        db.close()
