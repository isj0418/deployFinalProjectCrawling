from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException

import time
import json
import mysql.connector
from datetime import datetime
from bs4 import BeautifulSoup
import requests

options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
driver = webdriver.Chrome(options=options)

url_list = []
url = 'https://sports.news.naver.com/kbaseball/schedule/index.nhn'
driver.get(url)
time.sleep(2)

try:
    todaykboGame = driver.find_element(By.CSS_SELECTOR, 'div.sch_tb2.selected')
except NoSuchElementException:
    todaykboGame = driver.find_element(By.CSS_SELECTOR, 'div.sch_tb.selected')

todayLeftTeams = todaykboGame.find_elements(By.CSS_SELECTOR, 'span.team_lft')
todayRightTeams = todaykboGame.find_elements(By.CSS_SELECTOR, 'span.team_rgt')
today = todaykboGame.find_element(By.CSS_SELECTOR, 'span.td_date')
date_parts = today.text.split()
month, day = map(int, date_parts[0].split('.'))

todayTeam = []
teamLink = []
TodayGameList = []
for todayLeftTeam in todayLeftTeams:
    todayTeam.append(todayLeftTeam.text)

for todayRightTeam in todayRightTeams:
    todayTeam.append(todayRightTeam.text)
    # 각 tr 태그에서 td 태그를 가져옵니다.
# 형식을 변경하고 출력합니다.
formatted_date = f"{month:02d}{day:02d}"
for i in range((len(todayTeam) // 2)):
    leftTeam = todayTeam[(i)]
    rightTeam = todayTeam[(i + (len(todayTeam) // 2))]
    if (leftTeam == '한화'):
        teamLink.append('HH')
    elif (leftTeam == 'KT'):
        teamLink.append('KT')
    elif (leftTeam == '롯데'):
        teamLink.append('LT')
    elif (leftTeam == '키움'):
        teamLink.append('WO')
    elif (leftTeam == 'SSG'):
        teamLink.append('SK')
    elif (leftTeam == 'LG'):
        teamLink.append('LG')
    elif (leftTeam == '삼성'):
        teamLink.append('SS')
    elif (leftTeam == 'KIA'):
        teamLink.append('HT')
    elif (leftTeam == 'NC'):
        teamLink.append('NC')
    elif (leftTeam == '두산'):
        teamLink.append('OB')

    if (rightTeam == '한화'):
        teamLink.append('HH')
    elif (rightTeam == 'KT'):
        teamLink.append('KT')
    elif (rightTeam == '롯데'):
        teamLink.append('LT')
    elif (rightTeam == '키움'):
        teamLink.append('WO')
    elif (rightTeam == 'SSG'):
        teamLink.append('SK')
    elif (rightTeam == 'LG'):
        teamLink.append('LG')
    elif (rightTeam == '삼성'):
        teamLink.append('SS')
    elif (rightTeam == 'KIA'):
        teamLink.append('HT')
    elif (rightTeam == 'NC'):
        teamLink.append('NC')
    elif (rightTeam == '두산'):
        teamLink.append('OB')

for i in range(len(teamLink) // 2):
    href = formatted_date + teamLink[i * 2] + teamLink[(i*2) + 1]
    url = f'https://m.sports.naver.com/game/2023{href}02023/preview'
    driver.get(url)
    time.sleep(2)

    State_Game = driver.find_element(
        By.XPATH, '//*[@id="content"]/div/div/section[1]/div[2]/div[1]').text
    url_list.append(url)

    LeftTeam_Name = driver.find_element(
        By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[1]/div[1]/div[1]/div[1]').text
    RightTeam_Name = driver.find_element(
        By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[1]/div[1]/div[3]/div[1]').text

    try:
        leftTeam_Score = driver.find_element(
            By.XPATH, '//*[@id="content"]/div/div/section[1]/div[2]/div[2]/div[2]/strong').text
        rightTeam_Score = driver.find_element(
            By.XPATH, '//*[@id="content"]/div/div/section[1]/div[2]/div[3]/div[2]/strong').text
        score_text = driver.find_element(
            By.XPATH, '//*[@id="content"]/div/div/section[1]/div[3]/div').text
    except NoSuchElementException:
        leftTeam_Score = "요소를 찾을 수 없음"
        rightTeam_Score = "요소를 찾을 수 없음"
        score_text = '요소를 찾을수 가 없다.'

    # TodayGameInfo
    TodayLeftGameInfo = driver.find_element(
        By.XPATH, '//*[@id="content"]/div/div/section[1]/div[2]/div[2]/div').text
    TodayRightGameInfo = driver.find_element(
        By.XPATH, '//*[@id="content"]/div/div/section[1]/div[2]/div[3]/div').text
    LeftImage_xpath = '//*[@id="content"]/div/div/section[1]/div[2]/div[2]/div/div[1]/img'
    LeftImage_element = driver.find_element(By.XPATH, LeftImage_xpath)
    LeftImage_src = LeftImage_element.get_attribute('src')
    RightImage_path = '//*[@id="content"]/div/div/section[1]/div[2]/div[3]/div/div[1]/img'
    RightImage_element = driver.find_element(By.XPATH, RightImage_path)
    Rightimage_src = RightImage_element.get_attribute('src')

    leftTeam = []
    rightTeam = []
    leftVestPitcher = []
    rightVestPitcher = []
    leftVestHitter = []
    rightVestHitter = []
    leftTeamLineUp = []
    rightTeamLineUp = []

    # TeamInfo
    TeamInfo = driver.find_elements(
        By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[1]/div[1]/div')
    leftRecord = driver.find_elements(
        By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[1]/div[2]/table/tbody/tr/td[1]/div')
    rightRecord = driver.find_elements(
        By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[1]/div[2]/table/tbody/tr/td[2]/div')

    leftTeam.append(TeamInfo[0].text)
    rightTeam.append(TeamInfo[2].text)

    for i in range(len(leftRecord)):
        leftTeam.append(leftRecord[i].text)
        rightTeam.append(rightRecord[i].text)

        # pitcherw
        leftPitcherName = driver.find_element(
            By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[2]/div[1]/a/div[2]')
        leftPitherInfo = driver.find_elements(
            By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[3]/table/tbody/tr/td[1]')
        rightPitcherName = driver.find_element(
            By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[2]/div[3]/a/div[2]')
        rightPitcherInfo = driver.find_elements(
            By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[3]/table/tbody/tr/td[2]')

        leftVestPitcher.append(leftPitcherName.text)
        rightVestPitcher.append(rightPitcherName.text)

        for i in range(len(leftPitherInfo)):
            leftVestPitcher.append(leftPitherInfo[i].text)
            rightVestPitcher.append(rightPitcherInfo[i].text)

        # hitter
        leftHitterName = driver.find_element(
            By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[5]/div[1]/a/div[2]')
        rightHitterName = driver.find_element(
            By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[5]/div[3]/a/div[2]')
        leftHitterInfo = driver.find_elements(
            By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[6]/table/tbody//td[1]')
        rightHitterInfo = driver.find_elements(
            By.XPATH, '//*[@id="content"]/div/div/section[2]/div[2]/div/div[6]/table/tbody/tr/td[2]'
        )

        leftVestHitter.append(leftHitterName.text)
        rightVestHitter.append(rightHitterName.text)

    for i in range(len(leftHitterInfo)):
        leftVestHitter.append(leftHitterInfo[i].text)
        rightVestHitter.append(rightHitterInfo[i].text)

    TodayGame = {
        'TodayLeftGameInfo': TodayLeftGameInfo,
        'TodayRightGameInfo': TodayRightGameInfo,
        'LeftImage_src': LeftImage_src,
        'RightImage_src': Rightimage_src,
        'LeftTeam_Name': LeftTeam_Name,
        'RightTeam_Name': RightTeam_Name,
        'leftTeam': leftTeam,
        'rightTeam': rightTeam,
        'State_Game': State_Game,
        'leftVestPitcher': leftVestPitcher,
        'rightVestPitcher': rightVestPitcher,
        'leftVestHitter': leftVestHitter,
        'rightVestHitter': rightVestHitter,
        'leftTeam_Score': leftTeam_Score,
        'rightTeam_Score': rightTeam_Score,
        'score_text': score_text,
    }
    TodayGameList.append(TodayGame)

print(TodayGameList)

# with open("todayGameUrl.json", "w", encoding="utf-8") as json_file:
#     json.dump(url_list, json_file, ensure_ascii=False, indent=4)
