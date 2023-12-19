from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import mysql.connector
from datetime import datetime
import os
from decouple import config

# 현재 스크립트의 디렉토리를 가져와서 현재 작업 디렉토리로 설정
current_script_directory = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_script_directory)

# sportPigCrawlingDeploy 디렉토리로 이동
os.chdir(os.path.abspath(os.path.join(current_script_directory, '..')))

# .env 파일의 위치를 설정 (kbo 디렉토리 내부)
os.path.abspath(os.path.join(current_script_directory, 'kbo', '.env'))

# 환경 변수에서 값을 가져오기
web_db_endpoint = config('Web_DB_EndPoint')
web_db_id = config('Web_DB_ID')
web_db_pwd = config('Web_DB_PWD')

# 현재 날짜 가져오기
current_date = datetime.now()
date_string = current_date.strftime("%Y%m%d")


# MySQL 연결 설정
db_config = {
    "host": web_db_endpoint,
    "port": 3306,
    "user": web_db_id,
    "password": web_db_pwd,
    "database": "sportInfo"
}

# MySQL 연결
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# MySQL 테이블 생성 쿼리
create_table_query = '''
CREATE TABLE IF NOT EXISTS news (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    content TEXT,
    url VARCHAR(255),
    news_content TEXT,
    press VARCHAR(255),
    news_time VARCHAR(255),
    image_url VARCHAR(255)
)
'''

cursor.execute(create_table_query)
conn.commit()

options = webdriver.ChromeOptions()
options.add_experimental_option("excludeSwitches", ["enable-logging"])
driver = webdriver.Chrome(options=options)

# WebDriverWait를 사용하여 페이지 로딩 대기 시간 설정
wait = WebDriverWait(driver, 10)

# 크롤링한 데이터를 저장할 빈 리스트 생성
data_list = []
# 시작 페이지 설정
start_page = 1
last_page_content = None  # 마지막 페이지의 내용을 저장하기 위한 변수

# 네이버 스포츠 페이지 열기
driver.get('https://sports.news.naver.com/')

# "종목별" 메뉴 클릭
# (이 부분은 실제 웹 페이지 구조에 맞게 클릭 코드를 추가해야 합니다.)

# 이전에 크롤링한 뉴스의 제목을 저장할 집합(set) 생성
crawled_news_titles = set()

cursor.execute("SELECT title FROM news")
existing_titles = set(row[0] for row in cursor.fetchall())

# 이미지 URL 추출 함수
def extract_image_url(news_item):
    image_element = news_item.select_one('img.lazyLoadImage')
    if image_element:
        image_url = image_element.get('src', '')  # 이미지 URL 추출

        # ?type= 이전 부분 추출
        base_url = image_url.split('?type=')[0]

        # ?type= 이후의 부분을 변경하여 다른 크기로 설정 (예: w900)
        image_url = base_url + "?type=w900"

        return image_url

    return None

while True:
    # 페이지 URL 생성
    page_url = f'https://sports.news.naver.com/kbaseball/news/index?isphoto=N&date={date_string}&page={start_page}'
    driver.get(page_url)

    # 명시적 대기 시간 설정
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '#_newsList > ul > li')))

    # 페이지 소스 가져오기
    page_source = driver.page_source

    # BeautifulSoup를 사용하여 HTML 파싱
    soup = BeautifulSoup(page_source, 'html.parser')

    # CSS 선택자를 사용하여 뉴스 아이템 추출
    news_list = soup.select('#_newsList > ul > li')

    # 현재 페이지의 내용을 가져옴
    current_page_content = [news_item.text for news_item in news_list]

    # 현재 페이지와 이전 페이지의 내용이 같으면 마지막 페이지로 간주하고 루프 종료
    if current_page_content == last_page_content:
        break

    # 마지막 페이지가 아니라면 뉴스 크롤링을 계속 진행
    for news_item in news_list:
        try:
            title_element = news_item.select_one('div > a > span')
            content_element = news_item.select_one('div > p')
            press_element = news_item.select_one('span.press')
            time_element = news_item.select_one('span.time > span.bar')

            if title_element and content_element:
                title = title_element.text.strip()
                content = content_element.text.strip()

                # 이미 크롤링한 뉴스인지 또는 데이터베이스에 있는 뉴스인지 확인
                if title not in crawled_news_titles and title not in existing_titles:
                    # 언론사 가져오기
                    press = press_element.text.strip() if press_element else None

                    # 시간 정보 가져오기
                    time_text = time_element.next_sibling.strip() if time_element else None
                    # 시간 형식 가공
                    news_time = datetime.strptime(time_text, '%Y.%m.%d %H:%M').strftime('%Y-%m-%d %H:%M:%S') if time_text else None

                    # 이미지 URL 추출
                    image_url = extract_image_url(news_item)

                    # 뉴스 아이템의 URL 가져오기
                    news_url = news_item.select_one('div > a')['href']
                    news_url = news_url.replace("/kbaseball/news/read?", "https://sports.news.naver.com/news?")

                    # 뉴스 아이템 페이지로 이동
                    driver.get(news_url)

                    # 명시적 대기 시간 설정
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.content_area > div.news_end')))

                    # 내용 크롤링
                    news_page_source = driver.page_source
                    news_soup = BeautifulSoup(news_page_source, 'html.parser')

                    # 특정 요소 제외
                    reporter_area = news_soup.select_one('#newsEndContents > div.reporter_area')
                    if reporter_area:
                        reporter_area.extract()

                    copyright = news_soup.select_one('#newsEndContents > div.copyright')
                    if copyright:
                        copyright.extract()

                    guide = news_soup.select_one('#_article_section_guide')
                    if guide:
                        guide.extract()

                    promotion = news_soup.select_one('#newsEndContents > div.promotion')
                    if promotion:
                        promotion.extract()

                    # 내용 추출
                    news_content_element = news_soup.select_one('div.content_area > div.news_end')
                    if news_content_element:
                        news_content = news_content_element.text.strip()

                    # MySQL에 데이터베이스에 저장되지 않은 새로운 뉴스만 저장
                    insert_query = '''
                    INSERT INTO news (title, content, url, news_content, press, news_time, image_url) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    '''
                    data = (title, content, news_url, news_content, press, news_time, image_url)
                    cursor.execute(insert_query, data)
                    conn.commit()

                    # 크롤링한 데이터를 리스트에 추가
                    news_item_data = {
                        'title': title,
                        'content': content,
                        'url': news_url,
                        'news_content': news_content,
                        'press': press,
                        'news_time': news_time,
                        'image_url': image_url
                    }
                    data_list.append(news_item_data)

                    print("--------------------------------------")
                    print(f"제목 : {title}")
                    print()
                    print(f"내용 : {content}")
                    print()
                    print(f"URL : {news_url}")
                    print()
                    print(f"뉴스 내용 : {news_content}")
                    print()
                    print(f"언론사 : {press}")
                    print()
                    print(f"시간 : {news_time}")
                    print()
                    print(f"이미지 URL : {image_url}")
                    print("--------------------------------------")

                    # 크롤링한 뉴스의 제목을 집합에 추가
                    crawled_news_titles.add(title)
        except AttributeError as e:
            print(f"오류 발생: {e}")

    # 현재 페이지의 내용을 이전 페이지 내용으로 설정
    last_page_content = current_page_content

    # "다음" 버튼 클릭하여 다음 페이지로 이동
    next_button = driver.find_element(By.CLASS_NAME, 'next')
    next_button.click()

    # 다음 페이지로 이동 후 페이지 번호 증가
    start_page += 1

# MySQL 연결 해제
cursor.close()
conn.close()

# 드라이버 종료
driver.quit()
