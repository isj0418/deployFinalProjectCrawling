
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import time
import json
import mysql.connector
from selenium.common.exceptions import NoSuchElementException

options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
driver = webdriver.Chrome(options=options)

url = 'https://sports.news.naver.com/kfootball/schedule/index.nhn?category=kleague'

try:
    driver.get(url)
    time.sleep(2)
    division_elements = driver.find_elements(By.CLASS_NAME, 'Division')

    table = driver.find_element(
        By.XPATH, '//*[@id="_monthlyScheduleList"]')
    tr_elements = table.find_elements(By.TAG_NAME, 'tr')
    month_game_dict = {}
    month_game_list = []
    th_element = ''

    for i in range(len(tr_elements)):
        if '경기가 없습니다' in tr_elements[i].text:
            continue
        try:
            th_element = tr_elements[i].find_element(By.TAG_NAME, 'th').text
        except NoSuchElementException:
            pass
        td_elements = tr_elements[i].find_elements(By.TAG_NAME, 'td')

        month_game_dict = {
            'day': th_element,
            'start_time': td_elements[0].text,
            'team_vs': td_elements[1].text
        }

        month_game_list.append(month_game_dict)

    print(month_game_list)


except Exception as err:
    print(err)
