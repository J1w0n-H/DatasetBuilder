# scams

## 프로젝트 소개

**scams** 프로젝트는 온라인 커뮤니티(네이버 카페, 미씨USA 등)에서 사기 관련 게시글을 **자동 크롤링**하고, 필요시 **자동 번역** 후, 다양한 사기 유형/주제/수단으로 **자동 분류**하는 데이터 파이프라인을 제공합니다. 크롤러, 번역기, 분류기 등 각 단계가 유기적으로 연동되어, 사기 사례 데이터셋을 쉽고 체계적으로 구축할 수 있습니다.

---

## 전체 파이프라인 흐름

1. **커뮤니티 크롤러**
   - 네이버 카페/미씨USA 등에서 사기 키워드로 게시글을 자동 수집
   - 결과는 CSV로 저장
2. **게시글 번역기 (선택)**
   - 한글 게시글을 영어로 자동 번역 (분류기에서 영문도 활용 가능)
3. **사기 분류기**
   - 게시글을 사기 유형/주제/수단별로 자동 분류 (키워드+zero-shot)
   - 결과는 통합 CSV로 저장 및 통계 출력

---

## 주요 기능

- **네이버 카페/미씨USA 등 커뮤니티 크롤러 제공**
- **게시글 자동 번역(translate_posts.py)**
- **사기 유형, 주제, 수단별 자동 분류**
- **한글 zero-shot 분류** (Hugging Face transformers 기반)
- **config.yaml 기반 유연한 카테고리/패턴 관리**
- **CSV 파일 일괄 처리 및 증분 저장**
- **분류 결과 통계 출력**

---

## 설치 및 환경

1. Python 3.8 이상 권장
2. 필수 패키지 설치:
   ```bash
   pip install -r requirements.txt
   ```
3. (옵션) GPU 사용 시 CUDA 환경 권장

---

## 사용법: 전체 데이터 파이프라인

### 1. config.yaml 설정

- 크롤러/분류기 모두에서 사용
- `text_classification` 아래에 `scam_type`, `scam_topic`, `scam_method`의 카테고리와 패턴을 정의

### 2. 커뮤니티 크롤링
- 네이버 카페: `naver_cafe_crawler/gu_crawler.py`
- 미씨USA: `missyusa_crawler/mu_crawler.py`
- 크롤러 실행 전 `config.yaml` 및 `config_secret.yaml`에서 로그인 정보, 검색 키워드 등 설정 필요
- 예시:
  ```bash
  python naver_cafe_crawler/gu_crawler.py
  python missyusa_crawler/mu_crawler.py
  ```
- 크롤링 결과는 `data/` 폴더에 CSV로 저장됨 (예: `gu_posts.csv`)

### 3. 게시글 번역 (선택)
- 한글 게시글을 영어로 자동 번역 (영문 분류, 다국어 분석에 활용)
- 예시:
  ```bash
  python data/translate_posts.py data/gu_posts.csv data/gu_posts_translated.csv
  ```
- 번역 결과는 *_translated.csv로 저장됨

### 4. 사기 분류
- 번역된 CSV 또는 원본 CSV를 입력으로 사용
- 예시:
  ```bash
  python data/classify_posts.py data/gu_posts_translated.csv data/gu_posts_classified.csv
  ```
- 분류 결과는 scam_type, scam_topic, scam_method 등 컬럼이 추가된 CSV로 저장

### 5. Hugging Face API 키 (옵션)
- `config_secret.yaml`에 huggingface_api_key를 입력하면 API 기반 분류도 지원

---

## 폴더 구조

```
scams_git/
├── data/                  # 분류/전처리/번역 스크립트
├── naver_cafe_crawler/    # 네이버 카페 크롤러
├── missyusa_crawler/      # 미씨USA 크롤러
├── config.yaml            # 분류 카테고리/패턴 설정
├── config_secret.yaml     # 민감 정보(API 키 등)
├── requirements.txt       # 의존성 목록
├── README.md              # 프로젝트 설명
└── ...
```

---

## 기여 방법

- 이슈/PR 환영합니다! (크롤러 개선, 번역기/분류기 개선, 카테고리 추가 등)
- 코드 스타일: PEP8 권장

---

## 라이선스

- MIT License
