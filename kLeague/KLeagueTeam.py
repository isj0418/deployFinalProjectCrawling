from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import time
import json
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

options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
driver = webdriver.Chrome(options=options)
try:
    url = 'https://www.kleague.com/record/team.do'
    driver.get(url)
    time.sleep(2)
    finalGroupA = tr_elements = driver.find_elements(
        By.XPATH, '//*[@id="ts1"]/tbody/tr')
    finalGroupB = tr_elements = driver.find_elements(
        By.XPATH, '//*[@id="ts2"]/tbody/tr')
    k_league_teamList = []
    for i in range(len(finalGroupA)+(len(finalGroupB))):
        if (i < 6):
            td_elements = finalGroupA[i].find_elements(By.TAG_NAME, 'td')
        if (i >= 6):
            td_elements = finalGroupB[i-6].find_elements(By.TAG_NAME, 'td')
        k_league_Ranking = td_elements[0].text
        k_league_clupName = td_elements[1].text
        k_league_clupGame = td_elements[2].text
        k_league_WinPoint = td_elements[3].text
        k_league_Win = td_elements[4].text
        k_league_Draw = td_elements[5].text
        k_league_Lose = td_elements[6].text
        k_league_Score = td_elements[7].text
        k_league_LoseScore = td_elements[8].text
        k_league_GainorLoss = td_elements[9].text
        k_league_recent = td_elements[10].text

        k_league_Team = {
            "k_league_Ranking": k_league_Ranking,
            "k_league_clupName": k_league_clupName,
            "k_league_clupGame": k_league_clupGame,
            "k_league_WinPoint": k_league_WinPoint,
            "k_league_Win": k_league_Win,
            "k_league_Draw": k_league_Draw,
            "k_league_Lose": k_league_Lose,
            "k_league_Score": k_league_Score,
            "k_league_LoseScore": k_league_LoseScore,
            "k_league_GainorLoss": k_league_GainorLoss,
            "k_league_recent": k_league_recent
        }
        k_league_teamList.append(k_league_Team)
    print(k_league_teamList)
    # MySQL 데이터베이스 설정
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
        CREATE TABLE IF NOT EXISTS K_league_Team (
            k_Team_Id BIGINT AUTO_INCREMENT PRIMARY KEY,
            k_league_Ranking VARCHAR(255) NOT NULL,
            k_league_club_Name VARCHAR(255) NOT NULL, 
            k_league_club_Game VARCHAR(255) NOT NULL ,
            k_league_Win_Point VARCHAR(255) NOT NULL, 
            k_league_Win VARCHAR(255) NOT NULL, 
            k_league_Draw VARCHAR(255) NOT NULL ,
            k_league_Lose VARCHAR(255) NOT NULL  , 
            k_league_Score VARCHAR(255) NOT NULL  , 
            k_league_Lose_Score VARCHAR(255) NOT NULL  , 
            k_league_Gainor_Loss VARCHAR(255) NOT NULL  , 
            k_league_recent VARCHAR(255) NOT NULL 
        );
        """

        cursor.execute(create_table_sql)
        db.commit()

        # JSON 데이터를 MySQL 데이터베이스에 삽입
        for k_league in k_league_teamList:
            sql = "INSERT INTO K_league_Team (k_league_Ranking, k_league_club_Name, k_league_club_Game, k_league_Win_Point, k_league_Win , k_league_Draw, k_league_Lose , k_league_Score ,  k_league_Lose_Score , k_league_Gainor_Loss , k_league_recent) VALUES (%s, %s, %s, %s, %s, %s , %s , %s , %s ,%s , %s)"
            val = (k_league["k_league_Ranking"],
                   k_league["k_league_clupName"],
                   k_league["k_league_clupGame"],
                   k_league["k_league_WinPoint"],
                   k_league["k_league_Win"],
                   k_league["k_league_Draw"],
                   k_league["k_league_Lose"],
                   k_league["k_league_Score"],
                   k_league["k_league_LoseScore"],
                   k_league["k_league_GainorLoss"],
                   k_league["k_league_recent"])
            cursor.execute(sql, val)
            db.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # 커넥션 닫기
        if db.is_connected():
            cursor.close()
            db.close()

    # 브라우저 닫기
    driver.quit()

except Exception as err:
    print("Error" + err)
