import requests
from bs4 import BeautifulSoup, Tag
import pandas as pd
import yaml
import os
import time
from datetime import datetime
import urllib.parse
import sys

CONFIG_PATH = 'config.yaml'

def get_output_path(input_path, suffix):
    base, ext = os.path.splitext(input_path)
    return f"{base}{suffix}{ext}"

# config 읽기
def load_config():
    # 기본 설정 파일 읽기
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"config.yaml 읽기 실패: {e}")
        config = {}
    
    # 민감한 정보 파일 읽기
    try:
        with open('config_secret.yaml', 'r', encoding='utf-8') as f:
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
    df = pd.read_csv(data_path, encoding='euc-kr')
    return set(df['id'].astype(str))

def save_posts(posts, data_path):
    df = pd.DataFrame(posts)
    # 오늘 날짜를 id에 'date:YYYY-MM-DD'로 추가 (최상단에)
    today_str = datetime.now().strftime('%Y-%m-%d')
    date_row = {col: '' for col in df.columns}
    if 'id' in date_row:
        date_row['id'] = f'date:{today_str}'
    df = pd.concat([pd.DataFrame([date_row]), df], ignore_index=True)
    if os.path.exists(data_path):
        df_old = pd.read_csv(data_path, encoding='euc-kr')
        # 기존 파일에서 첫 행이 id가 'date:'로 시작하면 제거
        if 'id' in df_old.columns and str(df_old.iloc[0].get('id', '')).startswith('date:'):
            df_old = df_old.iloc[1:]
        df = pd.concat([df_old, df], ignore_index=True)
        df = df.drop_duplicates(subset=['id'])
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    df.to_csv(data_path, index=False, encoding='euc-kr', errors='ignore')

def get_post_content(post_url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Referer': 'https://www.missyusa.com/',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        resp = requests.get(post_url, headers=headers)
        resp.encoding = 'euc-kr'
        soup = BeautifulSoup(resp.text, 'html.parser')
        content_div = soup.select_one('div.detail_content')
        # 본문 텍스트
        content = content_div.get_text("\n", strip=True) if content_div else ''
        # 이미지 URL 추출
        image_urls = []
        if content_div:
            for img in content_div.find_all('img'):
                if isinstance(img, Tag):
                    src = img.get('src')
                    if src and src not in image_urls:
                        image_urls.append(src)
        if not image_urls:
            image_urls = ['없음']
        return content, image_urls
    except Exception as e:
        print(f"[ERROR] Failed to fetch content from {post_url}: {e}")
        return '', ['없음']

def crawl_posts(config, data_path):
    existing_ids = get_post_ids(data_path)
    all_new_posts = []
    for keyword in config['keywords']:
        page = 1
        while True:
            encoded_keyword = urllib.parse.quote(keyword, encoding='euc-kr')
            url = config['missyusa']['search_url'].format(keyword=encoded_keyword, page=page)
            headers = {
                'User-Agent': 'Mozilla/5.0',
                'Referer': 'https://www.missyusa.com/',
                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
            }
            try:
                resp = requests.get(url, headers=headers)
                resp.encoding = 'euc-kr'
                # 에러 페이지 감지
                if "An error occurred on the server" in resp.text:
                    print(f"[WARNING] Server error on page {page}, skipping...")
                    page += 1
                    time.sleep(2)
                    continue
                soup = BeautifulSoup(resp.text, 'html.parser')
            except Exception as e:
                print(f"[ERROR] Exception on page {page}: {e}, skipping...")
                page += 1
                time.sleep(2)
                continue

            # 게시글 링크 추출 (중복 없이, 실제 구조에 맞게)
            post_links = []
            seen = set()
            for td in soup.find_all('td', attrs={'align': 'left'}):
                if not isinstance(td, Tag):
                    continue
                for a in td.find_all('a', href=True):
                    if isinstance(a, Tag):
                        href = a.get('href')
                        href_str = str(href) if href is not None else ''
                        if href_str and 'board_read.asp' in href_str and href_str not in seen:
                            seen.add(href_str)
                            post_links.append(a)
            print(f"[DEBUG] Found {len(post_links)} post links on page {page}")

            if not post_links:
                break

            new_posts = []
            for a in post_links:
                if not isinstance(a, Tag):
                    continue
                href = a.get('href')
                href_str = str(href) if href is not None else ''
                post_id = href_str.split('idx=')[-1].split('&')[0] if href_str else ''
                if not href_str or post_id in existing_ids:
                    continue
                post_url = 'https://www.missyusa.com' + href_str if href_str.startswith('/') else href_str
                title = a.get_text(strip=True)
                content, image_urls = get_post_content(post_url)
                new_posts.append({
                    'id': post_id,
                    'url': post_url,
                    'title': title,
                    'content': content,
                    'image_urls': ','.join(image_urls),
                    'keyword': keyword,
                    'crawled_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
            if new_posts:
                print(f"[INFO] {len(new_posts)} new posts saved.")
                all_new_posts.extend(new_posts)
            else:
                print("[INFO] No new posts found on this page.")
            page += 1
            time.sleep(1)  # 페이지당 딜레이
    if all_new_posts:
        save_posts(all_new_posts, data_path)
    else:
        print("[INFO] No new posts found.")

def main():
    args = sys.argv[1:]
    config = load_config()
    if len(args) >= 1:
        data_path = args[0]
    else:
        data_path = config.get('missyusa', {}).get('data_path', 'data/mu_posts.csv')
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    # 파일이 없으면 빈 파일 생성
    if not os.path.exists(data_path):
        columns = ['id', 'title', 'content', 'image_urls', 'url', 'keyword', 'crawled_at']
        pd.DataFrame({col: [] for col in columns}).to_csv(data_path, index=False, encoding='euc-kr')
    print(f"[INFO] 데이터 저장 경로: {data_path}")
    while True:
        print(f"[INFO] Crawling at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        crawl_posts(config, data_path)
        print(f"[INFO] Sleeping for {config['interval_minutes']} minutes...")
        time.sleep(config['interval_minutes'] * 60)

if __name__ == '__main__':
    main() 