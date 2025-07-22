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
import sys

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

def get_output_path(input_path, suffix):
    base, ext = os.path.splitext(input_path)
    return f"{base}{suffix}{ext}"

def get_post_ids(data_path):
    if not os.path.exists(data_path):
        return set()
    df = pd.read_csv(data_path, encoding='utf-8-sig')
    return set(df['id'].astype(str))

def get_post_ids_and_contents(data_path):
    """
    nv_posts.csv에서 id와 content를 모두 딕셔너리로 반환
    """
    if not os.path.exists(data_path):
        return dict()
    df = pd.read_csv(data_path, encoding='utf-8-sig')
    return dict(zip(df['id'].astype(str), df['content'].fillna('')))

def save_posts(posts, data_path):
    columns = ['keyword', 'id', 'title', 'content', 'image_urls', 'url', 'crawled_at']
    df_new = pd.DataFrame(posts)
    for col in columns:
        if col not in df_new.columns:
            df_new[col] = ''
    df_new = df_new[columns]

    today_str = datetime.now().strftime('%Y-%m-%d')
    date_row = {col: '' for col in columns}
    date_row['id'] = f'최종업데이트:{today_str}'

    if os.path.exists(data_path):
        df_old = pd.read_csv(data_path, encoding='utf-8-sig')
        # 기존 날짜 행(최종업데이트:) 모두 제거
        df_old = df_old[~df_old['id'].astype(str).str.startswith('최종업데이트:')]
        for col in columns:
            if col not in df_old.columns:
                df_old[col] = ''
        df_old = df_old[columns]
        df_old.set_index('id', inplace=True)
        df_new.set_index('id', inplace=True)
        for idx, row in df_new.iterrows():
            if idx in df_old.index:
                for col in columns:
                    old_val = str(df_old.at[idx, col]) if col in df_new.columns else ''
                    new_val = str(row[col])
                    if (not old_val or old_val.strip() == '' or old_val == 'nan') and new_val and new_val.strip() != '' and new_val != 'nan':
                        df_old.at[idx, col] = new_val
            else:
                df_old.loc[idx] = row
        df = df_old.reset_index()
        # 오늘 날짜 행을 맨 위에 추가
        df = pd.concat([pd.DataFrame([date_row]), df], ignore_index=True)
    else:
        df = pd.concat([pd.DataFrame([date_row]), df_new], ignore_index=True)
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    df.to_csv(data_path, index=False, encoding='utf-8-sig')

def login_to_naver(driver, config):
    # 수동 로그인 옵션
    manual_login = input("자동 로그인을 시도하시겠습니까? (y/n, 기본값: y): ").strip().lower()
    if manual_login == 'n':
        print("[INFO] 수동 로그인 모드로 전환합니다.")
        print("[INFO] 브라우저가 열리면 직접 로그인해주세요.")
        print("[INFO] 로그인 완료 후 Enter 키를 눌러주세요.")
        
        driver.get("https://nid.naver.com/nidlogin.login")
        input("로그인 완료 후 Enter 키를 눌러주세요...")
        
        # 로그인 상태 확인
        driver.get("https://www.naver.com")
        time.sleep(3)
        
        try:
            # 로그인된 상태에서 보이는 요소들 확인
            login_indicators = [
                'a[href*="nid.naver.com/user2"]',
                '.gnb_login_area .user',
                '.gnb_my_area',
                '.sc_login'
            ]
            
            for indicator in login_indicators:
                try:
                    element = driver.find_element(By.CSS_SELECTOR, indicator)
                    if element:
                        print("[INFO] 수동 로그인 성공 확인")
                        return True
                except:
                    continue
            
            print("[ERROR] 수동 로그인 상태를 확인할 수 없습니다.")
            return False
            
        except Exception as e:
            print(f"[ERROR] 수동 로그인 확인 중 오류: {e}")
            return False

    try:
        print("[INFO] 네이버 로그인 시도 중...")
        driver.get("https://nid.naver.com/nidlogin.login")
        time.sleep(3)
        
        # ID 입력 필드 찾기 (여러 가능한 셀렉터 시도)
        id_selectors = [
            'input[name="id"]',
            'input[id="id"]',
            'input[placeholder*="아이디"]',
            'input[placeholder*="ID"]',
            '#id',
            '.id_input'
        ]
        
        id_field = None
        for selector in id_selectors:
            try:
                id_field = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                print(f"[DEBUG] ID 필드 찾음: {selector}")
                break
            except:
                continue
        
        if not id_field:
            print("[ERROR] ID 입력 필드를 찾을 수 없습니다.")
            return False
        
        # PW 입력 필드 찾기
        pw_selectors = [
            'input[name="pw"]',
            'input[id="pw"]',
            'input[type="password"]',
            'input[placeholder*="비밀번호"]',
            'input[placeholder*="Password"]',
            '#pw',
            '.pw_input'
        ]
        
        pw_field = None
        for selector in pw_selectors:
            try:
                pw_field = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                print(f"[DEBUG] PW 필드 찾음: {selector}")
                break
            except:
                continue
        
        if not pw_field:
            print("[ERROR] 비밀번호 입력 필드를 찾을 수 없습니다.")
            return False
        
        # 로그인 정보 입력
        id_field.clear()
        id_field.send_keys(config['naver']['login_id'])
        time.sleep(1)
        
        pw_field.clear()
        pw_field.send_keys(config['naver']['login_pw'])
        time.sleep(1)

        # Stay Signed in 체크박스 체크
        try:
            keep_checkbox = driver.find_element(By.ID, "nvlong")
            if not keep_checkbox.is_selected():
                keep_checkbox.click()
                print("[DEBUG] 'Stay Signed in' 체크박스 선택 완료")
        except Exception as e:
            print(f"[WARNING] 'Stay Signed in' 체크박스 선택 실패: {e}")
        
        # 로그인 버튼 클릭
        login_selectors = [
            'input[type="submit"]',
            'button[type="submit"]',
            '.btn_login',
            '.btn_global',
            'input[value*="로그인"]',
            'button:contains("로그인")'
        ]
        
        login_btn = None
        for selector in login_selectors:
            try:
                login_btn = driver.find_element(By.CSS_SELECTOR, selector)
                print(f"[DEBUG] 로그인 버튼 찾음: {selector}")
                break
            except:
                continue
        
        if login_btn:
            login_btn.click()
        else:
            # Enter 키로 로그인 시도
            pw_field.send_keys(Keys.RETURN)
        
        time.sleep(10)  # 로그인 처리 시간 증가

        # 로그인 성공 확인 (강화)
        try:
            # 네이버 메인 페이지로 이동해서 로그인 상태 확인
            driver.get("https://www.naver.com")
            time.sleep(3)
            login_success = False
            login_indicators = [
                'a[href*="nid.naver.com/user2"]',  # 마이페이지 링크
                '.gnb_login_area .user',            # 사용자 영역
                '.gnb_my_area',                     # 내 정보 영역
                '.sc_login',                        # 로그인 상태 표시
                'a#gnb_logout_button',              # 로그아웃 버튼
                'a.gnb_my',                         # 마이페이지
                'div.gnb_my_name',                  # 내 이름
                'a.link_login',                     # 로그인 링크 (있으면 로그인 안 된 것)
            ]
            for indicator in login_indicators:
                try:
                    element = driver.find_element(By.CSS_SELECTOR, indicator)
                    # 로그인 링크가 있으면 로그인 안 된 것
                    if indicator == 'a.link_login':
                        print("[ERROR] 로그인 링크가 보임: 로그인 실패")
                        login_success = False
                        break
                    if element and element.is_displayed():
                        print(f"[INFO] 로그인 성공 확인: {indicator}")
                        login_success = True
                        break
                except Exception as e:
                    continue
            if not login_success:
                print("[ERROR] 네이버 로그인 실패: 로그인된 사용자 정보가 보이지 않음")
                return False
        except Exception as e:
            print(f"[ERROR] 로그인 성공 여부 확인 중 예외 발생: {e}")
            return False
            
    except Exception as e:
        print(f"[ERROR] 로그인 중 오류: {e}")
        return False

def search_in_cafe(driver, keyword):
    """카페에서 검색 수행"""
    # 1. 카페 메인으로 이동
    driver.get("https://cafe.naver.com/gotousa")
    time.sleep(3)
    
    # 2. iframe 찾기 및 전환
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    print(f"[DEBUG] Found {len(iframes)} iframes on the cafe page.")
    for idx, iframe in enumerate(iframes):
        print(f"[DEBUG] Iframe {idx}: id={iframe.get_attribute('id')}, name={iframe.get_attribute('name')}")
    
    # 3. 검색을 위해 검색 페이지로 직접 이동
    keyword_encoded = urllib.parse.quote(keyword, encoding='euc-kr')
    search_url = f"https://cafe.naver.com/ArticleSearchList.nhn?search.clubid=10854519&search.searchBy=0&search.query={keyword_encoded}&search.page=1&userDisplay=50"
    driver.get(search_url)
    time.sleep(3)
    
    # 4. 검색 결과 페이지의 iframe 찾기
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    print(f"[DEBUG] Found {len(iframes)} iframes on search page.")
    for idx, iframe in enumerate(iframes):
        print(f"[DEBUG] Iframe {idx}: id={iframe.get_attribute('id')}, name={iframe.get_attribute('name')}")
    
    # 5. cafe_main iframe으로 전환
    try:
        WebDriverWait(driver, 10).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "cafe_main"))
        )
        print("[INFO] cafe_main iframe으로 전환 성공")
        # Save HTML source for debugging
        with open("cafe_main_iframe_debug.html", "w", encoding="utf-8", errors="replace") as f:
            f.write(driver.page_source)
        print("[DEBUG] Saved cafe_main_iframe_debug.html for selector inspection.")
    except TimeoutException:
        print("[ERROR] cafe_main iframe을 찾을 수 없습니다.")
        return False
    
    return True

def get_post_content_and_images(driver, post_url):
    driver.get(post_url)
    time.sleep(1)

    try:
        WebDriverWait(driver, 3).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "cafe_main"))
        )
    except Exception:
        pass

    # 1. 안내문 div 직접 탐색 (guide_box, tit_level)
    try:
        guide_boxes = driver.find_elements(By.CSS_SELECTOR, "div.guide_box, p.tit_level")
        for box in guide_boxes:
            if "등급이 되시면 읽기가 가능한 게시판 입니다." in box.text:
                return "권한부족", []
    except Exception:
        pass

    # 2. body 전체 텍스트에서 안내문 체크 (백업)
    try:
        iframe_text = driver.find_element(By.TAG_NAME, "body").text
        if "등급이 되시면 읽기가 가능한 게시판 입니다." in iframe_text:
            return "권한부족", []
    except Exception:
        pass

    # 3. 본문 추출 (안내문이 없을 때만)
    content = ""
    for selector in [
        'div.se-main-container',
        'div.ContentRenderer',
        'div.article_viewer',
        'div.ArticleContentBox__content',
        'div#app div.ArticleContentBox__content',
        'div#app .se-main-container',
        'div#app .article_viewer',
    ]:
        elements = driver.find_elements(By.CSS_SELECTOR, selector)
        if elements:
            content = elements[0].get_attribute('innerText') or elements[0].text
            break

    # 4. 혹시 본문에 안내문이 있으면 한 번 더 체크
    if "등급이 되시면 읽기가 가능한 게시판 입니다." in content:
        return "권한부족", []

    # 5. 이미지 추출 등 이하 생략
    image_urls = []
    for selector in [
        'div.se-main-container img',
        'div.ContentRenderer img',
        'div.article_viewer img',
        'div.ArticleContentBox__content img',
        'div#app div.ArticleContentBox__content img',
        'div#app .se-main-container img',
        'div#app .article_viewer img',
    ]:
        img_elements = driver.find_elements(By.CSS_SELECTOR, selector)
        for img in img_elements:
            src = img.get_attribute('src')
            if src and src not in image_urls:
                image_urls.append(src)
    # 이미지가 없으면 '없음'으로 표시
    if not image_urls:
        image_urls = ['없음']
    return content, image_urls

def crawl_posts(config, data_path):
    """게시글 크롤링 메인 함수"""
    id_content_map = get_post_ids_and_contents(data_path)
    
    # Chrome 옵션 설정
    chrome_options = Options()
    chrome_options.binary_location = config['naver']['chrome_path']
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36')
    
    # Set page load strategy via options (Selenium 4+)
    chrome_options.set_capability("pageLoadStrategy", "eager")
    
    # Chrome 드라이버 시작
    service = Service(config['naver']['chromedriver_path'])
    driver = None
    try:
        print("[INFO] Chrome 드라이버 시작 중...")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_page_load_timeout(30)  # 30초 페이지 로드 타임아웃
        driver.implicitly_wait(10)
        
        print("[INFO] 네이버 로그인 시도 중...")
        if not login_to_naver(driver, config):
            print("[ERROR] 네이버 로그인 실패")
            return
        
        for keyword in config['keywords']:
            print(f"[INFO] 키워드 '{keyword}' 검색 시작")
            
            # 검색 페이지로 이동
            if not search_in_cafe(driver, keyword):
                continue
            
            new_posts = []
            page = 1
            
            while True:
                print(f"[INFO] 페이지 {page} 처리 중...")
                
                # 게시글 링크 수집
                post_links = []
                elements = driver.find_elements(By.CSS_SELECTOR, 'div.board-list a.article')
                print(f"[DEBUG] Found {len(elements)} post links on page {page}")
                for element in elements:
                    post_url = element.get_attribute('href')
                    if not post_url or 'ArticleRead' not in post_url:
                        continue
                    article_id_match = re.search(r'articleid=(\d+)', post_url)
                    if not article_id_match:
                        continue
                    post_id = article_id_match.group(1)
                    # 이미 수집된 게시글이라도 content가 비어있거나 image_urls가 비어있으면 다시 크롤링
                    content_exists = post_id in id_content_map and str(id_content_map[post_id]).strip() != ''
                    # 기존에 저장된 이미지 정보 확인 (data_path에서 image_urls 컬럼 읽기)
                    image_exists = False
                    df_check = pd.read_csv(data_path, encoding='utf-8-sig')
                    if 'id' in df_check.columns and 'image_urls' in df_check.columns:
                        row = df_check[df_check['id'].astype(str) == str(post_id)]
                        if not row.empty:
                            image_urls_val = str(row.iloc[0]['image_urls'])
                            if image_urls_val and image_urls_val.strip() != '' and image_urls_val != 'nan':
                                image_exists = True
                        else:
                            image_urls_val = ''  # 기본값
                    if content_exists and image_exists:
                        print(f"[DEBUG] 이미 수집된 게시글(본문/이미지 있음) 건너뜀: {post_id}")
                        continue
                    elif content_exists or image_exists:
                        print(f"[DEBUG] 이미 수집된 게시글(본문 또는 이미지 비어있음) 재수집: {post_id}")
                    title = element.text.strip()
                    if not title:
                        continue
                    # 네이버는 상대경로로 주므로 절대경로로 변환
                    if post_url.startswith('/'):
                        post_url = 'https://cafe.naver.com' + post_url
                    post_links.append((post_id, post_url, title))

                if not post_links:
                    print("[INFO] 검색 결과가 없습니다.")
                    break

                for post_id, post_url, title in post_links:
                    try:
                        driver.execute_script("window.open(arguments[0]);", post_url)
                        driver.switch_to.window(driver.window_handles[-1])
                        content, image_urls = get_post_content_and_images(driver, post_url)
                        driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                        driver.switch_to.default_content()
                        WebDriverWait(driver, 10).until(
                            EC.frame_to_be_available_and_switch_to_it((By.ID, "cafe_main"))
                        )
                        print(f"[DEBUG] post_id: {post_id}")
                        print(f"[DEBUG] title: {title}")
                        print(f"[DEBUG] content: {content[:100]}...")
                        post_data = {
                            'keyword': keyword,
                            'id': post_id,
                            'title': title,
                            'content': content,
                            'image_urls': ','.join(image_urls),
                            'url': post_url,
                            'crawled_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        save_posts([post_data], data_path)
                        id_content_map[post_id] = content
                        time.sleep(0.2)
                    except Exception as e:
                        print(f"[WARNING] 게시글 처리 중 오류: {e}")
                        continue
                
                print(f"[INFO] 페이지 {page}에서 {len(post_links)}개 게시글 수집 완료")
                
                # 다음 페이지 이동
                try:
                    # 여러 방법으로 다음 페이지 버튼 찾기
                    next_page_elem = None
                    
                    # 방법 1: prev-next 영역에서 다음 페이지 번호 찾기
                    pagination = driver.find_elements(By.CSS_SELECTOR, 'div.prev-next a')
                    current_page = page
                    
                    print(f"[DEBUG] 페이지네이션 요소 {len(pagination)}개 발견")
                    for a in pagination:
                        try:
                            page_text = a.text.strip()
                            print(f"[DEBUG] 페이지 버튼: '{page_text}'")
                            page_num = int(page_text)
                            if page_num > current_page:
                                next_page_elem = a
                                print(f"[DEBUG] 다음 페이지 {page_num} 발견")
                                break
                        except Exception as e:
                            print(f"[DEBUG] 페이지 번호 파싱 실패: {e}")
                            continue
                    
                    # 방법 2: 다음 페이지 버튼 직접 찾기
                    if not next_page_elem:
                        try:
                            next_selectors = [
                                'a.pgR',  # 다음 페이지 버튼
                                'a[href*="page=' + str(page + 1) + '"]',  # 다음 페이지 링크
                                '.prev-next a:not(.on)',  # 현재 페이지가 아닌 버튼
                                'a[onclick*="page=' + str(page + 1) + '"]'  # onclick으로 다음 페이지
                            ]
                            
                            for selector in next_selectors:
                                try:
                                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                                    for elem in elements:
                                        if elem.text.strip() and elem.text.strip().isdigit():
                                            next_page_num = int(elem.text.strip())
                                            if next_page_num > current_page:
                                                next_page_elem = elem
                                                print(f"[DEBUG] 다음 페이지 버튼 발견: {selector} -> {next_page_num}")
                                                break
                                    if next_page_elem:
                                        break
                                except:
                                    continue
                        except Exception as e:
                            print(f"[DEBUG] 다음 페이지 버튼 찾기 실패: {e}")
                    
                    # 방법 3: 현재 페이지가 마지막 페이지인지 확인
                    if not next_page_elem:
                        try:
                            # 현재 페이지가 마지막 페이지인지 확인
                            current_page_elem = driver.find_element(By.CSS_SELECTOR, 'div.prev-next a.on')
                            if current_page_elem:
                                print("[DEBUG] 현재 페이지가 마지막 페이지로 확인됨")
                        except:
                            pass
                    
                    if next_page_elem:
                        print(f"[INFO] 페이지 {page + 1}로 이동 중...")
                        next_page_elem.click()
                        time.sleep(3)  # 페이지 로딩 대기 시간 증가
                        # 다음 페이지에서 iframe 재전환
                        driver.switch_to.default_content()
                        WebDriverWait(driver, 10).until(
                            EC.frame_to_be_available_and_switch_to_it((By.ID, "cafe_main"))
                        )
                        page += 1
                    else:
                        print("[INFO] 더 이상 다음 페이지가 없습니다.")
                        break
                except Exception as e:
                    print(f"[INFO] 다음 페이지 이동 중 오류: {e}")
                    break
            
            print(f"[INFO] 키워드 '{keyword}' 크롤링 완료")
        
    except Exception as e:
        print(f"[ERROR] 크롤링 중 오류 발생: {e}")
    finally:
        if driver:
            print("[INFO] Chrome 드라이버 종료")
            driver.quit()

def main():
    # config 및 인자 처리
    args = sys.argv[1:]
    config = load_config()
    # 오늘 날짜 (월일) 구하기
    today_str = datetime.now().strftime('%m%d')
    if len(args) >= 1:
        data_path = args[0]
    else:
        data_path = config.get('naver', {}).get('data_path', 'data/gu_posts.csv')
    os.makedirs(os.path.dirname(data_path), exist_ok=True)
    # 파일이 없을 때 posts 컬럼명에 맞는 헤더만 있는 빈 CSV를 생성하도록 main 함수에서 처리. (encoding='utf-8-sig')
    if not os.path.exists(data_path):
        columns = ['keyword', 'id', 'title', 'content', 'image_urls', 'url', 'crawled_at']
        import pandas as pd
        pd.DataFrame({col: [] for col in columns}).to_csv(data_path, index=False, encoding='utf-8-sig')
    print(f"[INFO] 데이터 저장 경로: {data_path}")
    while True:
        print(f"[INFO] Crawling at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        crawl_posts(config, data_path)
        print(f"[INFO] Sleeping for {config['interval_minutes']} minutes...")
        time.sleep(config['interval_minutes'] * 60)

if __name__ == '__main__':
    main() 