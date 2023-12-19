
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import time
import json
import mysql.connector
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import requests

options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
driver = webdriver.Chrome(options=options)

first_game_list = []
two_game_list = []
try:
    url = 'https://www.kleague.com'
    driver.get(url)
    time.sleep(1)
    first_game_day = driver.find_element(
        By.XPATH, '//*[@id="match-league1"]/h3[1]').text
    first_games = driver.find_elements(
        By.XPATH, '//*[@id="match-league1"]/ul[1]/li')
    two_game_day = driver.find_element(
        By.XPATH, '//*[@id="match-league1"]/h3[2]').text
    two_games = driver.find_elements(
        By.XPATH, '//*[@id="match-league1"]/ul[2]/li')

    for i in range(len(first_games)):
        first_game_list.append(first_games[i].text)
    for j in range(len(two_games)):
        two_game_list.append(two_games[j].text)

    current_game_list = [{
        first_game_day: first_game_list,
        two_game_day: two_game_list
    }]
    print(current_game_list)
except Exception as e:
    print(e)
