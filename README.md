# 뉴스 크롤링 서버

비동기 뉴스 크롤링 시스템으로 RSS 피드 수집과 웹 크롤링을 통합 관리합니다.

## 🚀 주요 기능

- **비동기 RSS 피드 수집** (aiohttp)
- **웹 크롤링** (BeautifulSoup4, robots.txt 준수)
- **스케줄링** (APScheduler)
- **자동 재시도** (tenacity)
- **PostgreSQL 저장** (SQLAlchemy)
- **시사오늘 전용 크롤러**
- **모듈화된 구조**

## 📁 프로젝트 구조

```
crawling_server/
│
├── web_crawler/
│   ├── sisaon_scraper.py         # 시사오늘 기자별 기사 크롤링
│   ├── general_scraper.py        # 타 신문사 웹 크롤링
│   └── utils/
│       ├── parser_utils.py       # HTML 파싱 공통 함수
│       └── request_utils.py      # 요청/헤더 관리 함수
│
├── rss_collector/
│   ├── feed_fetcher.py           # aiohttp로 RSS URL들 비동기 요청
│   ├── feed_parser.py            # feedparser로 RSS 파싱
│   └── rss_sources.yaml          # 수집할 RSS 주소 리스트 설정
│
├── scheduler/
│   └── job_manager.py            # APScheduler로 크롤러 스케줄 관리
│
├── config/
│   └── settings.py               # DB 연결정보, 환경설정, 로깅
│
└── main.py                       # 전체 파이프라인 실행 진입점
```

## 🛠️ 설치 및 설정

### 1. 가상환경 생성 및 활성화

```bash
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
# 또는
venv\Scripts\activate     # Windows
```

### 2. 패키지 설치

```bash
pip install -r requirements.txt
```

### 3. 환경변수 설정

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=newspaper_db
export DB_USER=postgres
export DB_PASSWORD=your_password
```

### 4. PostgreSQL 데이터베이스 생성

```sql
CREATE DATABASE newspaper_db;
```

## 🚀 사용법

### 전체 서버 실행

```bash
cd crawling_server
python main.py
```

### RSS 수집만 실행

```bash
python main.py rss                    # 모든 소스
python main.py rss sisaoneul          # 특정 소스만
```

### 웹 크롤링만 실행

```bash
python main.py crawl                  # 모든 소스
python main.py crawl sisaoneul        # 특정 소스만
```

### 서버 상태 확인

```bash
python main.py status
```

## ⚙️ 설정

### RSS 소스 설정

`crawling_server/rss_collector/rss_sources.yaml` 파일에서 RSS 피드 URL과 크롤링 설정을 관리합니다.

```yaml
rss_sources:
  sisaoneul:
    name: "시사오늘"
    base_url: "https://www.sisaoneul.com"
    feeds:
      - url: "https://www.sisaoneul.com/rss.xml"
        category: "전체"

crawling_settings:
  enabled_sources:
    - sisaoneul
    - yonhap
    - hankookilbo
  
  priorities:
    sisaoneul: 10
    yonhap: 8
  
  intervals:
    sisaoneul: 30    # 30분마다
    yonhap: 60       # 60분마다
```

### 크롤링 설정

`crawling_server/config/settings.py`에서 크롤링 관련 설정을 조정할 수 있습니다.

```python
CRAWLING_CONFIG = {
    'request_delay': 1.0,  # 요청 간격 (초)
    'timeout': 30,         # 요청 타임아웃 (초)
    'max_retries': 3,      # 최대 재시도 횟수
}
```

## 📊 모니터링

### 로그 확인

```bash
tail -f logs/crawler.log
```

### 작업 상태 조회

```python
from crawling_server.scheduler.job_manager import JobManager

job_manager = JobManager()
status = job_manager.get_scheduler_info()
print(status)
```

## 🔧 개발

### 새로운 뉴스 사이트 추가

1. `rss_sources.yaml`에 RSS 피드 URL 추가
2. `general_scraper.py`의 `site_configs`에 사이트별 선택자 추가
3. 필요시 전용 스크래퍼 생성

### 데이터베이스 저장 로직 추가

`main.py`의 콜백 함수에서 데이터베이스 저장 로직을 구현합니다:

```python
async def _on_rss_collection_complete(self, result):
    # 데이터베이스 저장 로직
    await self._save_articles_to_db(result['articles'])
```

## 🐛 문제 해결

### 일반적인 문제들

1. **PostgreSQL 연결 실패**
   - 환경변수 확인
   - PostgreSQL 서비스 실행 상태 확인

2. **RSS 피드 접근 실패**
   - 네트워크 연결 확인
   - RSS URL 유효성 확인

3. **크롤링 실패**
   - robots.txt 확인
   - 사이트 구조 변경 확인

### 디버깅

```bash
# 상세 로그 레벨 설정
export LOG_LEVEL=DEBUG
python main.py
```

## 📝 라이선스

MIT License

## 🤝 기여

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request 