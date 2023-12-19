import json
import requests
from bs4 import BeautifulSoup
import mysql.connector
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

file_path = './KboPlayers_hrefOrigin.json'
kbo_hitters = []
count = 0

try:
    with open(file_path, 'r', encoding='utf-8') as json_file:
        # JSON 파일을 파싱
        players_href = json.load(json_file)
            # https://www.koreabaseball.com/Futures/Player/HitterDetail.aspx?playerId=53103
        for i in range(len(players_href)):
            # https://www.koreabaseball.com/Record/Player/PitcherDetail/Basic.aspx?playerId=67119
            url = f'https://www.koreabaseball.com/Futures/Player/HitterDetail.aspx?playerId={players_href[i]}'
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # player_position
            players_position_elements = soup.find(
                'span', id='cphContents_cphContents_cphContents_ucPlayerProfile_lblPosition').text
            # player_Name
            player_Name = soup.find(
                'span', id='cphContents_cphContents_cphContents_ucPlayerProfile_lblName').text
            # db 연계 상 player_Id 맞추려 autoIncrement 대신 사용
            count += 1

            if '투수' not in players_position_elements:
                # player_data
                table = soup.find('table')
                tbody = table.find('tbody')
                td_elements = tbody.find_all('td')
                # 이미지 URL 초기값 설정
                player_img_url = None

                # 먼저 첫 번째 경우의 이미지 태그를 찾고 이미지 URL 가져오기 시도
                img_tag = soup.find('img', id='cphContents_cphContents_cphContents_playerProfile_imgProgile')
                if img_tag and 'src' in img_tag.attrs: # img_tag.attrs -> {'src': 'example.jpg', 'alt': 'Example Image'}
                    player_img_url = img_tag['src']
                    print(player_img_url)
                else:
                    # 두 번째 경우의 이미지 태그를 찾고 이미지 URL 가져오기 시도
                    img_tag = soup.find('img', id='cphContents_cphContents_cphContents_ucPlayerProfile_imgProfile')
                    if img_tag and 'src' in img_tag.attrs:
                        player_img_url = img_tag['src']
                        print(player_img_url)

                player_href = players_href[i]

                player_Team = td_elements[0].text if td_elements and len(
                    td_elements) > 0 else ""
                player_AVG = td_elements[1].text if td_elements and len(
                    td_elements) > 1 else ""
                player_Game = td_elements[2].text if td_elements and len(
                    td_elements) > 2 else ""
                player_AB = td_elements[3].text if td_elements and len(
                    td_elements) > 3 else ""
                player_R = td_elements[4].text if td_elements and len(
                    td_elements) > 4 else ""
                player_H = td_elements[5].text if td_elements and len(
                    td_elements) > 5 else ""
                player_2B = td_elements[6].text if td_elements and len(
                    td_elements) > 6 else ""
                player_3B = td_elements[7].text if td_elements and len(
                    td_elements) > 7 else ""
                player_HR = td_elements[8].text if td_elements and len(
                    td_elements) > 8 else ""
                kbo_hittersData = {
                    'player_Id': count,
                    'player_Name': player_Name,
                    'player_href': player_href,
                    'player_Team': player_Team,
                    'player_AVG': player_AVG,
                    'player_Game': player_Game,
                    'player_AB': player_AB,
                    'player_R': player_R,
                    'player_H': player_H,
                    'player_2B': player_2B,
                    'player_3B': player_3B,
                    'player_HR': player_HR,
                    'player_Image_URL': player_img_url if player_img_url else ""  # 이미지 URL이 없는 경우 빈 문자열로 처리
                }
                kbo_hitters.append(kbo_hittersData)
                # JSON 데이터를 MySQL 데이 터베이스에 삽입

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
        CREATE TABLE IF NOT EXISTS kbo_hitter (
            player_id BIGINT PRIMARY KEY,
            player_name VARCHAR(255) NOT NULL,
            player_href VARCHAR(255) NOT Null, 
            player_team VARCHAR(255),
            player_avg VARCHAR(255),
            player_game VARCHAR(255),
            player_ab VARCHAR(255),
            player_r VARCHAR(255),
            player_h VARCHAR(255),
            player_2b VARCHAR(255),
            player_3b VARCHAR(255),
            player_hr VARCHAR(255),
            player_image_url VARCHAR(255)
        );
        """
        cursor.execute(create_table_sql)
        db.commit()

        for kbo_hitter in kbo_hitters:
            sql = """
            INSERT INTO kbo_hitter(player_id , player_name , player_href, player_team, player_avg, player_game,  
            player_ab, player_r, player_h, player_2b, player_3b, player_hr, player_image_url)
            VALUES (%s , %s ,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            val = (kbo_hitter["player_Id"],
                   kbo_hitter["player_Name"],
                   kbo_hitter["player_href"],
                   kbo_hitter["player_Team"],
                   kbo_hitter["player_AVG"],
                   kbo_hitter["player_Game"],
                   kbo_hitter["player_AB"],
                   kbo_hitter["player_R"],
                   kbo_hitter["player_H"],
                   kbo_hitter["player_2B"],
                   kbo_hitter["player_3B"],
                   kbo_hitter["player_HR"],
                   kbo_hitter.get("player_Image_URL", None))  # 이미지 URL이 없는 경우 None 처리
            cursor.execute(sql, val)
            db.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        # 변경 내용을 저장
        db.commit()

    finally:
        # 커넥션 닫기
        if db.is_connected():
            cursor.close()
            db.close()
        # sql 구문에 val를 넣겠다 .

except Exception as err:
    print(err)
    # JSON 파일 경로
