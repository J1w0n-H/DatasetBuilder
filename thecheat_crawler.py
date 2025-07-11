import os
import time
import yaml
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib.parse
import re

# config 읽기
def load_config():
    # 기본 설정 파일 읽기
    try:
        with open('config.yaml', 'r', encoding='utf-8-sig') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"config.yaml 읽기 실패: {e}")
        config = {}
    
    # 민감한 정보 파일 읽기
    try:
        with open('config_secret.yaml', 'r', encoding='utf-8-sig') as f:
            config_secret = yaml.safe_load(f)
        # 민감한 정보를 기본 설정에 병합
        if config_secret:
            config.update(config_secret)
    except Exception as e:
        print(f"config_secret.yaml 읽기 실패: {e}")
    
    return config

def get_post_ids(data_path):
    if not os.path.exists(data_path):
        return set()
    df = pd.read_csv(data_path, encoding='utf-8-sig')
    return set(df['id'].astype(str))

def save_posts(posts, data_path):
    df_new = pd.DataFrame(posts)
    if os.path.exists(data_path):
        df_old = pd.read_csv(data_path, encoding='utf-8-sig')
        df_old.set_index('id', inplace=True)
        df_new.set_index('id', inplace=True)
        for idx, row in df_new.iterrows():
            if idx in df_old.index:
                old_content = str(df_old.at[idx, 'content']).strip()
                new_content = str(row['content']).strip()
                # 기존 본문이 빈칸이고, 새 본문이 있으면 업데이트
                if (not old_content) and new_content:
                    for col in df_new.columns:
                        df_old.at[idx, col] = row[col]
                # 기존 본문이 있고, 새 본문이 더 길면 업데이트
                elif old_content and new_content and len(new_content) > len(old_content):
                    for col in df_new.columns:
                        df_old.at[idx, col] = row[col]
                # 기존 본문과 새 본문이 다르고, 새 본문이 더 짧으면 합치기
                elif old_content and new_content and old_content != new_content and len(new_content) <= len(old_content):
                    merged_content = old_content
                    if new_content not in old_content:
                        merged_content += "\n" + new_content
                    df_old.at[idx, 'content'] = merged_content
            else:
                # 기존에 없는 id는 추가
                df_old.loc[idx] = row
        df = df_old.reset_index()
    else:
        df = df_new.reset_index()
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    df.to_csv(data_path, index=False, encoding='utf-8-sig')

def setup_driver():
    """Chrome 드라이버 설정"""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # 헤드리스 모드 (필요시 주석 해제)
    # chrome_options.add_argument("--headless")
    
    driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(10)
    return driver

def get_post_list(driver, page=1):
    """게시판 목록에서 게시글 URL들을 가져오기"""
    url = f"https://thecheat.co.kr/rb/?m=bbs&bid=cheat_guest&type=global&page={page}"
    driver.get(url)
    time.sleep(3)
    
    posts = []
    try:
        # 게시글 목록 찾기 (CSS 셀렉터는 실제 사이트 구조에 맞게 조정 필요)
        post_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='wr_id']")
        
        for link in post_links:
            try:
                href = link.get_attribute('href')
                if href and 'wr_id=' in href:
                    # 게시글 ID 추출
                    wr_id = re.search(r'wr_id=(\d+)', href)
                    if wr_id:
                        post_id = wr_id.group(1)
                        
                        # 제목 추출 (링크 텍스트 또는 부모 요소에서)
                        title = link.text.strip()
                        if not title:
                            # 부모 요소에서 제목 찾기
                            parent = link.find_element(By.XPATH, "./..")
                            title = parent.text.strip()
                        
                        posts.append({
                            'id': post_id,
                            'title': title,
                            'url': href
                        })
            except Exception as e:
                print(f"게시글 정보 추출 중 오류: {e}")
                continue
                
    except Exception as e:
        print(f"게시글 목록 가져오기 실패: {e}")
    
    return posts

def get_post_content(driver, post_url):
    """개별 게시글의 내용 가져오기"""
    driver.get(post_url)
    time.sleep(2)
    
    try:
        # 제목 추출
        title_selectors = [
            "h1", "h2", "h3", ".title", ".subject", ".post_title",
            "[class*='title']", "[class*='subject']"
        ]
        title = ""
        for selector in title_selectors:
            try:
                title_elem = driver.find_element(By.CSS_SELECTOR, selector)
                title = title_elem.text.strip()
                if title:
                    break
            except:
                continue
        
        # 본문 내용 추출
        content_selectors = [
            ".content", ".post_content", ".article", ".text", ".body",
            "[class*='content']", "[class*='text']", "[class*='body']",
            "div[class*='post']", "div[class*='article']"
        ]
        content = ""
        for selector in content_selectors:
            try:
                content_elem = driver.find_element(By.CSS_SELECTOR, selector)
                content = content_elem.text.strip()
                if content and len(content) > 50:  # 최소 길이 확인
                    break
            except:
                continue
        
        # 작성자 추출
        author_selectors = [
            ".author", ".writer", ".user", ".name",
            "[class*='author']", "[class*='writer']", "[class*='user']"
        ]
        author = ""
        for selector in author_selectors:
            try:
                author_elem = driver.find_element(By.CSS_SELECTOR, selector)
                author = author_elem.text.strip()
                if author:
                    break
            except:
                continue
        
        # 작성일 추출
        date_selectors = [
            ".date", ".time", ".created", ".posted",
            "[class*='date']", "[class*='time']", "[class*='created']"
        ]
        post_date = ""
        for selector in date_selectors:
            try:
                date_elem = driver.find_element(By.CSS_SELECTOR, selector)
                post_date = date_elem.text.strip()
                if post_date:
                    break
            except:
                continue
        
        return {
            'title': title,
            'content': content,
            'author': author,
            'post_date': post_date
        }
        
    except Exception as e:
        print(f"게시글 내용 가져오기 실패: {e}")
        return {
            'title': '',
            'content': '',
            'author': '',
            'post_date': ''
        }

def crawl_thecheat_posts(max_pages=10):
    """더치트 국제 피해 게시판 크롤링"""
    driver = setup_driver()
    
    try:
        all_posts = []
        existing_posts = get_post_ids('data/thecheat_posts.csv')
        
        for page in range(1, max_pages + 1):
            print(f"페이지 {page} 크롤링 중...")
            
            # 게시글 목록 가져오기
            post_list = get_post_list(driver, page)
            
            if not post_list:
                print(f"페이지 {page}에서 게시글을 찾을 수 없습니다.")
                break
            
            for post in post_list:
                post_id = post['id']
                
                # 이미 크롤링한 게시글은 건너뛰기
                if post_id in existing_posts:
                    print(f"이미 크롤링된 게시글 건너뛰기: {post_id}")
                    continue
                
                print(f"게시글 크롤링 중: {post_id} - {post['title']}")
                
                # 게시글 내용 가져오기
                content_data = get_post_content(driver, post['url'])
                
                # 결과 저장
                post_data = {
                    'id': post_id,
                    'title': content_data['title'] or post['title'],
                    'content': content_data['content'],
                    'author': content_data['author'],
                    'post_date': content_data['post_date'],
                    'url': post['url'],
                    'crawled_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                all_posts.append(post_data)
                
                # 중간 저장 (메모리 절약)
                if len(all_posts) % 10 == 0:
                    save_posts(all_posts, 'data/thecheat_posts.csv')
                    print(f"중간 저장 완료: {len(all_posts)}개 게시글")
                
                time.sleep(1)  # 서버 부하 방지
            
            time.sleep(2)  # 페이지 간 대기
        
        # 최종 저장
        if all_posts:
            save_posts(all_posts, 'data/thecheat_posts.csv')
            print(f"크롤링 완료! 총 {len(all_posts)}개 게시글을 저장했습니다.")
        else:
            print("새로 크롤링할 게시글이 없습니다.")
            
    except Exception as e:
        print(f"크롤링 중 오류 발생: {e}")
    finally:
        driver.quit()

def main():
    """메인 실행 함수"""
    print("=== 더치트 국제 피해 게시판 크롤링 시작 ===")
    
    # 크롤링할 최대 페이지 수 설정
    max_pages = int(input("크롤링할 최대 페이지 수를 입력하세요 (기본값: 10): ") or "10")
    
    crawl_thecheat_posts(max_pages)

if __name__ == "__main__":
    main() 