import pandas as pd
from googletrans import Translator
import time
import yaml
import os
import sys

def get_output_path(input_path, suffix):
    base, ext = os.path.splitext(input_path)
    return f"{base}{suffix}{ext}"

def safe_translate(text, translator, src='ko', dest='en'):
    if not isinstance(text, str) or not text.strip():
        return ""
    try:
        return translator.translate(text, src=src, dest=dest).text
    except Exception as e:
        print(f"Exception: {e}")
        return ""

def main():
    # config.yaml에서 기본 경로 읽기
    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    # 인자 처리
    args = sys.argv[1:]
    if len(args) >= 1:
        input_file = args[0]
    else:
        input_file = config['naver']['data_path']
    if len(args) >= 2:
        output_file = args[1]
    else:
        output_file = get_output_path(input_file, "_translated")
    # 인코딩 자동 결정
    encoding = "euc-kr" if "mu_" in os.path.basename(input_file) else "utf-8"
    print(f"입력 파일: {input_file}")
    print(f"출력 파일: {output_file}")
    print(f"인코딩: {encoding}")
    df = pd.read_csv(input_file, encoding=encoding)
    translator = Translator()
    eng_titles = []
    eng_contents = []
    total = len(df)
    for i, row in df.iterrows():
        if str(row.get('id', '')).startswith('date:'):
            eng_titles.append("")
            eng_contents.append("")
            continue
        print(f"{i+1}/{total} 번역중...")
        eng_titles.append(safe_translate(row["title"], translator))
        eng_contents.append(safe_translate(row["content"], translator))
    df["Eng_title"] = eng_titles
    df["Eng_Contents"] = eng_contents
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"{output_file} 파일을 확인하세요.")

if __name__ == "__main__":
    main() 