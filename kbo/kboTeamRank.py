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

# sql 삽입 전 저장할 list
kbo_TeamData = []

try:
    url = "https://www.koreabaseball.com/Record/TeamRank/TeamRank.aspx"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    table = soup.find("table", {"summary": "순위, 팀명,승,패,무,승률,승차,최근10경기,연속,홈,방문"})

    # 테이블의 각 행을 반복하여 데이터 추출
    for row in table.find("tbody").find_all("tr"):
        td_elements = row.find_all("td")
        td_elements_data = {
            "team_Ranking": td_elements[0].text.strip(),
            "team_Name": td_elements[1].text.strip(),
            "team_Game": td_elements[2].text.strip(),
            "team_Win": td_elements[3].text.strip(),
            "team_Lose": td_elements[4].text.strip(),
            "team_Draw": td_elements[5].text.strip(),
            "team_Win_Rate": td_elements[6].text.strip(),
            "team_Games_Behind": td_elements[7].text.strip(),
            "team_Last_Ten_Games": td_elements[8].text.strip(),
            "team_Continuity": td_elements[9].text.strip(),
            "team_Home_Record": td_elements[10].text.strip(),
            "team_Away_Record": td_elements[11].text.strip()
        }
        kbo_TeamData.append(td_elements_data)

    db_config = {
        "host": web_db_endpoint,
        "port": 3306,
        "user": web_db_id,
        "password": web_db_pwd,
        "database": "sportInfo"
    }

    # MySQL 데이터베이스 연결 설정
    # db_config = {
    #     "host": "localhost",
    #     "port": 3306,  # 포트 번호를 별도로 지정
    #     "user": "root",
    #     "password": "1234",
    #     "database": "lee"
    # }

    # MySQL 데이터베이스에 연결
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # kbo_TeamData 테이블 생성
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS kbo_teamdata(
        team_Id BIGINT AUTO_INCREMENT PRIMARY KEY,
        team_Ranking VARCHAR(255),
        team_Name VARCHAR(255), 
        team_Game VARCHAR(255),
        team_Win VARCHAR(255),
        team_Lose VARCHAR(255),
        team_Draw VARCHAR(255),
        team_Win_Rate VARCHAR(255),
        team_Games_Behind VARCHAR(255),
        team_Last_Ten_Games VARCHAR(255),
        team_Continuity VARCHAR(255),
        team_Home_Record VARCHAR(255),
        team_Away_Record VARCHAR(255)
    ) ;
    """
    cursor.execute(create_table_sql)
    connection.commit()

    # 데이터를 MySQL 테이블에 삽입
    for kbo_team in kbo_TeamData:
        insert_query = """
        INSERT INTO kbo_teamdata (
            team_Ranking, team_Name, team_Game, team_Win, team_Lose, team_Draw,
            team_Win_Rate, team_Games_Behind, team_Last_Ten_Games, team_Continuity,
            team_Home_Record, team_Away_Record
        ) 
        VALUES (%s, %s , %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """
        val =(
            kbo_team["team_Ranking"],
            kbo_team["team_Name"],
            kbo_team["team_Game"],
            kbo_team["team_Win"],
            kbo_team["team_Lose"],
            kbo_team["team_Draw"],
            kbo_team["team_Win_Rate"],
            kbo_team["team_Games_Behind"],
            kbo_team["team_Last_Ten_Games"],
            kbo_team["team_Continuity"],
            kbo_team["team_Home_Record"],
            kbo_team["team_Away_Record"],
        )
        cursor.execute(insert_query, val)

    # 변경 내용을 커밋
    connection.commit()

    # 연결 종료
    cursor.close()
    connection.close()

except requests.exceptions.HTTPError as http_err:
    print(f"HTTP 오류 발생: {http_err}")
except requests.exceptions.RequestException as req_err:
    print(f"요청 오류 발생: {req_err}")
except Exception as e:
    print(f"오류 발생: {e}")