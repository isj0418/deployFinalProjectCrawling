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

player_id_list = []
k_league_player_list = []
try:

    infoList = ['01', '02', '03', '04', '05',
                '09', '10', '17', '18', '21', '22', '29']
    for i in range(len(infoList)):
        url = f'https://www.kleague.com/player.do?type=active&leagueId=1&teamId=K{infoList[i]}'
        driver.get(url)

        team_select = Select(driver.find_element(By.ID, 'clubList'))
        selected_option = team_select.first_selected_option
        K_League_Team_Name = selected_option.text
        time.sleep(2)

        K_League_Players = driver.find_elements(
            By.XPATH, '/html/body/div[2]/div/div[3]/div/div[1]/div')

        for K_League_Player in K_League_Players:
            onclick_value = K_League_Player.get_attribute('onclick')
            player_id = onclick_value.split('(')[1].split(')')[0]

            img_element = K_League_Player.find_element(By.TAG_NAME, 'img')
            player_img_url = img_element.get_attribute('src')
            player_name_and_local = K_League_Player.find_element(
                By.XPATH, './/div[2]/div/span[1]').text
            player_num = K_League_Player.find_element(
                By.XPATH, './/div[2]/div/span[2]').text

            k_league_player = {
                'k_league_player_id': player_id,
                'k_league_player_team': K_League_Team_Name,
                'k_league_player_name': player_name_and_local,
                'k_league_player_num': player_num,
                'k_league_player_img_url': player_img_url,
            }
            k_league_player_list.append(k_league_player)
            player_id_list.append(player_id)
        try:
            # if 2page exists
            js_code = "goToPage(2);"
            driver.execute_script(js_code)

            time.sleep(2)

            K_League_Players = driver.find_elements(
                By.XPATH, '/html/body/div[2]/div/div[3]/div/div[1]/div')

            for K_League_Player in K_League_Players:

                onclick_value = K_League_Player.get_attribute('onclick')
                player_id = onclick_value.split('(')[1].split(')')[0]

                img_element = K_League_Player.find_element(By.TAG_NAME, 'img')
                player_img_url = img_element.get_attribute('src')
                player_name_and_local = K_League_Player.find_element(
                    By.XPATH, './/div[2]/div/span[1]').text
                player_num = K_League_Player.find_element(
                    By.XPATH, './/div[2]/div/span[2]').text

                k_league_player = {
                    'k_league_player_id': player_id,
                    'k_league_player_team': K_League_Team_Name,
                    'k_league_player_name': player_name_and_local,
                    'k_league_player_num': player_num,
                    'k_league_player_img_url': player_img_url,
                }
                k_league_player_list.append(k_league_player)
                player_id_list.append(player_id)
        except Exception as err:
            print("Error" + err)

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
        CREATE TABLE IF NOT EXISTS K_League_Player (
            k_league_player_id VARCHAR(20) PRIMARY KEY, 
            k_league_player_team VARCHAR(50) NOT NULL, 
            k_league_player_name VARCHAR(50) NOT NULL,
            k_league_player_num VARCHAR(50) NOT NULL,
            k_league_player_img_url VARCHAR(255) NOT NULL
        );
        """

        cursor.execute(create_table_sql)
        db.commit()

        # JSON 데이터를 MySQL 데이터베이스에 삽입
        for k_league_player in k_league_player_list:
            sql = "INSERT INTO K_League_Player (k_league_player_id , k_league_player_team, k_league_player_name, k_league_player_num, k_league_player_img_url) VALUES (%s ,%s, %s, %s, %s)"
            val = (k_league_player["k_league_player_id"],
                   k_league_player["k_league_player_team"],
                   k_league_player["k_league_player_name"],
                   k_league_player["k_league_player_num"],
                   k_league_player["k_league_player_img_url"])
            cursor.execute(sql, val)
            db.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")

    finally:
        # 커넥션 닫기
        if db.is_connected():
            cursor.close()
            db.close()
    driver.quit()

    try:
        with open("./k_league_player_id.json", "w", encoding="utf-8") as json_file:
            json.dump(player_id_list, json_file, ensure_ascii=False, indent=4)
    except Exception as json_err:
        print("JSON 파일 생성 오류:", str(json_err))

except Exception as e:
    print("예외 발생: " + str(e))  # 예외 객체를 문자열로 변환하여 출력
