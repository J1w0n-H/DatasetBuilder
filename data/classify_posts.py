import pandas as pd
import requests
import json
import time
from typing import Dict, List, Optional
import re
import yaml
import os
import sys
from transformers.pipelines import pipeline

def get_output_path(input_path, suffix):
    base, ext = os.path.splitext(input_path)
    return f"{base}{suffix}{ext}"

class TextClassifier:
    def __init__(self, api_token: Optional[str] = None, config: Optional[dict] = None):
        """
        텍스트 분류기 초기화
        api_token: Hugging Face API 토큰 (선택사항)
        config: 설정 딕셔너리 (선택사항)
        """
        # 한국어 특화 zero-shot classification 모델(jhgan/ko-sroberta-multitask) 사용
        self.local_classifier = pipeline(
            "zero-shot-classification",
            model="jhgan/ko-sroberta-multitask",
            device=0,
            hypothesis_template="이 문장은 {} 와 관련이 있다."
        )
        # candidate_labels는 한글 value로!
        self.confidence_threshold = 0.3  # 기본값
        if config and 'text_classification' in config:
            tc = config['text_classification']
            if 'confidence_threshold' in tc:
                self.confidence_threshold = tc['confidence_threshold']

        # config에서 분류 설정 읽기 (scam_type, scam_topic, scam_method로 변경)
        self.scam_type_categories = {}
        self.scam_type_patterns = {}
        self.scam_topic_categories = {}
        self.scam_topic_patterns = {}
        self.scam_method_categories = {}
        self.scam_method_patterns = {}
        if config and 'text_classification' in config:
            tc = config['text_classification']
            if 'scam_type' in tc:
                self.scam_type_categories = tc['scam_type']['categories']
                self.scam_type_patterns = tc['scam_type']['patterns']
            if 'scam_topic' in tc:
                self.scam_topic_categories = tc['scam_topic']['categories']
                self.scam_topic_patterns = tc['scam_topic']['patterns']
            if 'scam_method' in tc:
                self.scam_method_categories = tc['scam_method']['categories']
                self.scam_method_patterns = tc['scam_method']['patterns']
        else:
            self.scam_type_categories = {}
            self.scam_type_patterns = {}
            self.scam_topic_categories = {}
            self.scam_topic_patterns = {}
            self.scam_method_categories = {}
            self.scam_method_patterns = {}

    def classify_with_keywords(self, text: str, patterns_dict: dict, default: str = "other", multi: bool = False, is_scam_related: bool = True) -> tuple:
        """
        키워드가 한번이라도 포함되면 해당 카테고리로 분류 (multi=True면 모든 매칭 카테고리 +로 연결)
        Returns: (classification, 매칭여부, 매칭된 키워드)
        """
        if not text or pd.isna(text):
            # 매칭이 하나도 없으면 patterns_dict의 첫 번째 key로 분류
            first_key = next(iter(patterns_dict.keys()), default)
            return first_key, 0, ""
        text = str(text).lower()
        matched = []
        matched_keywords = []
        for category, patterns in patterns_dict.items():
            for pattern in patterns:
                if pattern.lower() in text:
                    matched.append(category)
                    matched_keywords.append(pattern)
                    if not multi:
                        return category, 1, pattern  # 최초 매칭 카테고리, 키워드 반환
        if multi and matched:
            return "+".join(sorted(set(matched))), 1, "+".join(sorted(set(matched_keywords)))
        # 매칭이 하나도 없으면 patterns_dict의 첫 번째 key로 분류
        first_key = next(iter(patterns_dict.keys()), default)
        return first_key, 0, ""

    def classify_scam_type(self, text: str) -> tuple:
        """
        타입 분류 (confidence 포함)
        """
        return self.classify_with_keywords(text, self.scam_type_patterns, default="unclear")

    def classify_scam_topic(self, text: str) -> tuple:
        """
        사기 주제 분류 (confidence 포함)
        """
        return self.classify_with_keywords(text, self.scam_topic_patterns, default="other")

    def classify_scam_method(self, text: str) -> tuple:
        """
        사기 방법 분류 (confidence 포함, 복수 선택 가능)
        """
        return self.classify_with_keywords(text, self.scam_method_patterns, default="", multi=True)

    def classify_with_api(self, text: str) -> Optional[dict]:
        """
        로컬 zero-shot classification을 사용한 텍스트 분류
        """
        if not text or pd.isna(text):
            return None

        # candidate_labels를 한글 value로
        type_labels = list(self.scam_type_categories.values())
        topic_labels = list(self.scam_topic_categories.values())
        method_labels = list(self.scam_method_categories.values())
        candidate_labels = type_labels + topic_labels + method_labels

        if not candidate_labels:
            raise ValueError("candidate_labels가 비어 있습니다. config.yaml을 확인하세요.")

        result = self.local_classifier(text, candidate_labels, multi_label=True)
        labels = result["labels"] if isinstance(result, dict) else []
        scores = result["scores"] if isinstance(result, dict) else []
        label_score_dict = dict(zip(labels, scores))

        # label_map: value(한글) → key(영문)
        type_label_map = {v: k for k, v in self.scam_type_categories.items()}
        topic_label_map = {v: k for k, v in self.scam_topic_categories.items()}
        method_label_map = {v: k for k, v in self.scam_method_categories.items()}

        # 무조건 가장 높은 score의 label로 분류 (fallback 없음)
        type_label = None
        type_score = None
        topic_label = None
        topic_score = None
        method_label = None
        method_score = None
        for lbl in labels:
            if lbl in type_labels and type_label is None:
                type_label = type_label_map.get(lbl, lbl)
                type_score = label_score_dict[lbl]
            elif lbl in topic_labels and topic_label is None:
                topic_label = topic_label_map.get(lbl, lbl)
                topic_score = label_score_dict[lbl]
            elif lbl in method_labels and method_label is None:
                method_label = method_label_map.get(lbl, lbl)
                method_score = label_score_dict[lbl]
            if type_label and topic_label and method_label:
                break
        # labels가 비어있으면 강제 분류
        if not labels:
            type_label = next(iter(type_label_map.values()), "")
            topic_label = next(iter(topic_label_map.values()), "")
            method_label = next(iter(method_label_map.values()), "")
        # 그래도 못 찾으면 labels[0]을 그대로 사용
        if type_label is None and labels:
            type_label = type_label_map.get(labels[0], labels[0])
            type_score = label_score_dict[labels[0]]
        if topic_label is None and labels:
            topic_label = topic_label_map.get(labels[0], labels[0])
            topic_score = label_score_dict[labels[0]]
        if method_label is None and labels:
            method_label = method_label_map.get(labels[0], labels[0])
            method_score = label_score_dict[labels[0]]

        return {
            "scam_type": type_label,
            "scam_topic": topic_label,
            "scam_method": method_label,
            "scam_type_score": type_score,
            "scam_topic_score": topic_score,
            "scam_method_score": method_score
        }

    def classify_texts(self, text: str, use_api: bool = False) -> dict:
        # 1단계: 키워드 기반 분류 시도
        type_result, type_matched, type_kw = self.classify_scam_type(text)
        topic_result, topic_matched, topic_kw = self.classify_scam_topic(text)
        method_result, method_matched, method_kw = self.classify_scam_method(text)
        # 2단계: 하나라도 매칭 실패하면 해당 matched_*_keyword만 'API'로 기록, 값은 fallback 결과 사용
        api_result = None
        if (type_matched == 0 or topic_matched == 0 or method_matched == 0) and use_api:
            print(f"일부 카테고리 키워드 매칭 실패. 부족한 부분은 Transformer 분류기를 사용합니다.")
            api_result = self.classify_with_api(text)
        return {
            "scam_type": api_result["scam_type"] if type_matched == 0 and api_result else type_result,
            "scam_topic": api_result["scam_topic"] if topic_matched == 0 and api_result else topic_result,
            "scam_method": api_result["scam_method"] if method_matched == 0 and api_result else method_result,
            "matched_scam_type_keyword": "API" if type_matched == 0 else type_kw,
            "matched_scam_topic_keyword": "API" if topic_matched == 0 else topic_kw,
            "matched_scam_method_keyword": "API" if method_matched == 0 else method_kw,
            "matched_scam_type_score": api_result["scam_type_score"] if type_matched == 0 and api_result else None,
            "matched_scam_topic_score": api_result["scam_topic_score"] if topic_matched == 0 and api_result else None,
            "matched_scam_method_score": api_result["scam_method_score"] if method_matched == 0 and api_result else None
            }

def process_csv_file(input_file: str, output_file: str, api_token: Optional[str] = None, use_api: bool = False, config: Optional[dict] = None):
    """
    CSV 파일을 읽어서 분류 결과를 추가하여 저장
    """
    # 여러 인코딩을 시도하여 CSV 파일 읽기
    encodings = ['euc-kr', 'utf-8-sig', 'utf-8', 'cp949', 'latin1']
    df = None
    
    for encoding in encodings:
        try:
            df = pd.read_csv(input_file, encoding=encoding)
            print(f"파일을 {encoding} 인코딩으로 성공적으로 읽었습니다.")
            break
        except UnicodeDecodeError:
            print(f"{encoding} 인코딩으로 읽기 실패, 다음 인코딩 시도...")
            continue
        except Exception as e:
            print(f"{encoding} 인코딩으로 읽기 중 오류: {e}")
            continue
    
    if df is None:
        raise Exception("모든 인코딩으로 파일 읽기 실패")
    
    print(f"총 {len(df)}개의 행을 처리합니다...")
    
    # 기존 분류된 파일이 있는지 확인
    existing_df = None
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file, encoding='utf-8-sig')
            print(f"기존 분류 파일을 찾았습니다. {len(existing_df)}개의 행이 있습니다.")
        except Exception as e:
            print(f"기존 파일 읽기 실패: {e}")
    
    # 분류기 초기화
    classifier = TextClassifier(api_token, config)
    
    # 분류 결과를 저장할 리스트
    classifications_scam_type = []
    classifications_scam_topic = []
    classifications_scam_method = []
    matched_scam_type_keywords = []
    matched_scam_topic_keywords = []
    matched_scam_method_keywords = []
    
    output_exists = os.path.exists(output_file)
    for idx, (_, row) in enumerate(df.iterrows()):
        if str(row.get('id', '')).startswith('date:'):
            classifications_scam_type.append("")
            classifications_scam_topic.append("")
            classifications_scam_method.append("")
            matched_scam_type_keywords.append("")
            matched_scam_topic_keywords.append("")
            matched_scam_method_keywords.append("")
            continue
        # 기존 파일이 있고, 현재 행이 이미 분류되어 있는지 확인
        should_reclassify = True
        if existing_df is not None:
            existing_row = existing_df[existing_df['id'] == row['id']]
            if not existing_row.empty and 'scam_type' in existing_row.columns and 'scam_topic' in existing_row.columns and 'scam_method' in existing_row.columns:
                existing_scam_type = existing_row.iloc[0]['scam_type']
                existing_scam_topic = existing_row.iloc[0]['scam_topic']
                existing_scam_method = existing_row.iloc[0]['scam_method']
                # 예전 변수명/값(unclear, unknown, 빈값) 완전 제거: config 기반 값만 허용
                if (existing_scam_type not in ['unclear', 'unknown', ''] and 
                    existing_scam_topic not in ['unclear', 'unknown', ''] and 
                    existing_scam_method not in ['unclear', 'unknown', '']):
                    # 기존 분류 결과 사용
                    classifications_scam_type.append(existing_scam_type)
                    classifications_scam_topic.append(existing_scam_topic)
                    classifications_scam_method.append(existing_scam_method)
                    matched_scam_type_keywords.append(existing_row.iloc[0].get('matched_scam_type_keyword', ''))
                    matched_scam_topic_keywords.append(existing_row.iloc[0].get('matched_scam_topic_keyword', ''))
                    matched_scam_method_keywords.append(existing_row.iloc[0].get('matched_scam_method_keyword', ''))
                    should_reclassify = False
                continue
            else:
                print(f"ID {row['id']}: 기존 분류가 fallback 값(unclear/unknown/빈값)이므로 재분류합니다.")
        if should_reclassify:
            # 제목과 내용을 결합하여 분류
            title = str(row.get('title', ''))
            content = str(row.get('content', ''))
            eng_title = str(row.get('Eng_title', ''))
            eng_content = str(row.get('Eng_Contents', ''))
            # 한국어 텍스트 우선, 없으면 영어 텍스트 사용
            text_to_classify = ""
            if title and title != 'nan':
                text_to_classify += title + " "
            if content and content != 'nan':
                text_to_classify += content + " "
            # 한국어 텍스트가 없으면 영어 텍스트 사용
            if not text_to_classify.strip():
                if eng_title and eng_title != 'nan':
                    text_to_classify += eng_title + " "
                if eng_content and eng_content != 'nan':
                    text_to_classify += eng_content + " "
            # 분류 수행: fallback 없이 항상 config 카테고리 중 하나로만 분류
            result = classifier.classify_texts(text_to_classify.strip(), use_api)
            classifications_scam_type.append(result['scam_type'])
            classifications_scam_topic.append(result['scam_topic'])
            classifications_scam_method.append(result['scam_method'])
            matched_scam_type_keywords.append(result.get('matched_scam_type_keyword', ''))
            matched_scam_topic_keywords.append(result.get('matched_scam_topic_keyword', ''))
            matched_scam_method_keywords.append(result.get('matched_scam_method_keyword', ''))
        # 진행상황 출력
        if (idx + 1) % 10 == 0:
            print(f"진행률: {idx + 1}/{len(df)} ({((idx + 1)/len(df)*100):.1f}%)")
        # API 사용 시 요청 간격 조절
        if use_api:
            time.sleep(0.5)  # 0.5초 대기
        result_row = dict(row)  # 원본 row의 모든 컬럼 포함
        result_row.update({
                "scam_type": result['scam_type'],
            "scam_topic": result['scam_topic'],
            "scam_method": result['scam_method'],
                "matched_scam_type_keyword": result.get('matched_scam_type_keyword', ''),
                "matched_scam_topic_keyword": result.get('matched_scam_topic_keyword', ''),
                "matched_scam_method_keyword": result.get('matched_scam_method_keyword', '')
        })
        result_df = pd.DataFrame([result_row])
        result_df.to_csv(
            output_file,
            mode='a',
            header=not output_exists and idx == 0,  # 첫 행만 헤더
            index=False,
            encoding='utf-8-sig'
        )
        output_exists = True
    
    # 분류 결과를 데이터프레임에 추가
    df['scam_type'] = classifications_scam_type
    df['scam_topic'] = classifications_scam_topic
    df['scam_method'] = classifications_scam_method
    df['matched_scam_type_keyword'] = matched_scam_type_keywords
    df['matched_scam_topic_keyword'] = matched_scam_topic_keywords
    df['matched_scam_method_keyword'] = matched_scam_method_keywords
    
    # 결과 저장 (utf-8-sig 인코딩 사용)
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # 분류 결과 통계 출력
    print("\n=== 분류 결과 통계 ===")
    type_counts = df['scam_type'].value_counts()
    for category, count in type_counts.items():
        percentage = (count / len(df)) * 100
        print(f"{category}: {count}개 ({percentage:.1f}%)")
    
    print(f"\n분류 완료! 결과가 {output_file}에 저장되었습니다.")

def main():
    """
    메인 실행 함수
    """
    # 현재 스크립트의 디렉토리를 기준으로 경로 설정
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    # 커맨드라인 인자 처리
    args = sys.argv[1:]
    # config.yaml에서 기본 설정 읽기
    try:
        with open(os.path.join(project_root, "config.yaml"), 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"config.yaml 읽기 실패: {e}")
        config = {}
    # 입력 파일: 인자 > config > 기본값
    if len(args) >= 1:
        input_file = args[0]
    else:
        input_file = config.get('naver', {}).get('data_path', os.path.join(script_dir, "gu_posts.csv"))
    # 출력 파일: 인자 > 자동 생성
    if len(args) >= 2:
        output_file = args[1]
    else:
        # 입력 파일명에서 _translated를 _classified로 변경
        if "_translated" in input_file:
            output_file = get_output_path(input_file.replace("_translated", ""), "_classified")
        else:
            output_file = get_output_path(input_file, "_classified")
    config_secret_file = os.path.join(project_root, "config_secret.yaml")
    # config_secret.yaml에서 민감한 정보 읽기
    try:
        with open(config_secret_file, 'r', encoding='utf-8') as f:
            config_secret = yaml.safe_load(f)
        if config_secret:
            config.update(config_secret)
    except Exception as e:
        print(f"config_secret.yaml 읽기 실패: {e}")
    api_token = config.get('huggingface_api_key')
    use_api = False if not api_token else True
    print("=== 텍스트 분류 시작 ===")
    print(f"입력 파일: {input_file}")
    print(f"출력 파일: {output_file}")
    print(f"API 사용: {use_api}")
    if use_api:
        print("Hugging Face API를 사용하여 분류합니다...")
    else:
        print("키워드 기반 분류를 사용합니다...")
    process_csv_file(input_file, output_file, api_token, use_api, config)

if __name__ == "__main__":
    main() 