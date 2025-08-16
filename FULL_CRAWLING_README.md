# 시사오늘 전체 카테고리 완전 크롤링 시스템

모든 카테고리의 모든 페이지를 크롤링하여 데이터베이스에 저장하는 완전한 크롤링 시스템입니다.

## ⚠️ 주의사항

### 1. 데이터 규모
- **예상 기사 수**: 약 137,600개 (860페이지 × 20기사 × 8카테고리)
- **예상 소요 시간**: 8-12시간 (네트워크 상태에 따라 다름)
- **데이터베이스 용량**: 약 2-5GB (기사 내용 포함)

### 2. 서버 부하 고려
- 시사오늘 서버에 과도한 부하를 주지 않도록 설계됨
- 적절한 지연 시간과 병렬 처리 제한
- robots.txt 준수

### 3. 안전장치
- 중단/재시작 기능
- 실시간 모니터링
- 오류 복구 메커니즘
- 중복 기사 방지

## 🚀 사용법

### 1. 페이지 수 추정 (권장)

먼저 전체 페이지 수를 확인해보세요:

```bash
python full_crawling_script.py --estimate-only
```

### 2. 전체 크롤링 실행

#### 기본 실행 (모든 카테고리, 모든 페이지)
```bash
python full_crawling_script.py
```

#### 옵션을 사용한 실행
```bash
# 워커 수 조정 (기본값: 5)
python full_crawling_script.py --workers 3

# 카테고리당 최대 페이지 수 제한
python full_crawling_script.py --max-pages 100

# 카테고리 간 대기 시간 조정 (기본값: 10초)
python full_crawling_script.py --delay 15

# 특정 카테고리 건너뛰기
python full_crawling_script.py --skip "자동차" "IT"

# 조합 사용
python full_crawling_script.py --workers 3 --max-pages 50 --delay 20
```

### 3. 모니터링

#### 실시간 모니터링
```bash
# 60초 간격으로 모니터링
python crawling_monitor.py --monitor

# 30초 간격으로 모니터링
python crawling_monitor.py --monitor --interval 30
```

#### 현재 상태 확인
```bash
python crawling_monitor.py --stats
```

#### 완료 시간 추정
```bash
python crawling_monitor.py --estimate
```

### 4. 제어

#### 크롤링 중단
```bash
python crawling_monitor.py --stop
```

#### 일시정지 (5분)
```bash
python crawling_monitor.py --pause 300
```

## 📊 예상 결과

### 카테고리별 예상 기사 수
- **정치**: ~17,200개 (860페이지)
- **경제**: ~17,200개 (860페이지)
- **산업**: ~17,200개 (860페이지)
- **건설·부동산**: ~17,200개 (860페이지)
- **IT**: ~17,200개 (860페이지)
- **유통·바이오**: ~17,200개 (860페이지)
- **사회**: ~17,200개 (860페이지)
- **자동차**: ~17,200개 (860페이지)

**총 예상**: ~137,600개 기사

### 예상 기자 수
- **활동 기자**: 50-100명
- **주요 기자**: 20-30명

## 🔧 설정 옵션

### 성능 조정

#### 워커 수 (--workers)
- **낮은 값 (1-3)**: 안정성 우선, 서버 부하 최소화
- **중간 값 (4-6)**: 균형잡힌 성능 (권장)
- **높은 값 (7-10)**: 빠른 처리, 서버 부하 증가

#### 카테고리 간 대기 시간 (--delay)
- **10초**: 기본값, 적절한 부하 분산
- **15-20초**: 서버 부하 최소화
- **5초**: 빠른 처리, 주의 필요

### 안전 설정

#### 최대 페이지 수 제한 (--max-pages)
```bash
# 테스트용 (각 카테고리 10페이지만)
python full_crawling_script.py --max-pages 10

# 중간 규모 (각 카테고리 100페이지만)
python full_crawling_script.py --max-pages 100
```

## 📈 모니터링 지표

### 실시간 확인 가능한 정보
- 전체 기사 수
- 시사오늘 기사 수
- 기자 수
- 오늘 수집된 기사 수
- 카테고리별 분포
- 최근 활동 기자

### 성능 지표
- 평균 처리 속도 (기사/분)
- 페이지당 평균 시간
- 오류율
- 중복 기사 비율

## 🛠️ 문제 해결

### 1. 크롤링 중단 시
```bash
# 현재 상태 확인
python crawling_monitor.py --stats

# 크롤링 재시작 (중복 방지됨)
python full_crawling_script.py
```

### 2. 메모리 부족 시
```bash
# 워커 수 줄이기
python full_crawling_script.py --workers 2

# 페이지 수 제한
python full_crawling_script.py --max-pages 50
```

### 3. 네트워크 오류 시
```bash
# 대기 시간 늘리기
python full_crawling_script.py --delay 30

# 특정 카테고리만 재시도
python full_crawling_script.py --skip "정치" "경제"
```

### 4. 데이터베이스 오류 시
```bash
# 데이터베이스 연결 확인
python fix_database_schema.py

# 테이블 재생성
python setup_and_clear.py
```

## 📝 로그 파일

### 크롤링 로그
- `full_crawling.log`: 전체 크롤링 진행 상황
- `sisaon_crawler.log`: 개별 크롤러 로그

### 통계 파일
- `crawling_stats.json`: 실시간 통계 데이터
- `crawling_control.json`: 제어 신호 파일

## 🔄 재시작 및 복구

### 중단된 크롤링 재시작
```bash
# 중복 방지 기능으로 안전하게 재시작
python full_crawling_script.py
```

### 특정 카테고리만 재크롤링
```bash
# 다른 카테고리는 건너뛰고 특정 카테고리만
python full_crawling_script.py --skip "정치" "경제" "산업" "건설·부동산" "IT" "유통·바이오" "사회"
```

## 📊 완료 후 확인

### 1. 전체 통계 확인
```bash
python sisaon_crawler_with_ranking.py --mode rank
```

### 2. 기자 순위 확인
```bash
python sisaon_crawler_with_ranking.py --mode rank --force-refresh
```

### 3. 트렌드 분석
```bash
python sisaon_crawler_with_ranking.py --mode trend --trend-days 30
```

## ⚡ 최적화 팁

### 1. 네트워크 최적화
- 안정적인 인터넷 연결 사용
- VPN 사용 시 서버 위치 고려

### 2. 시스템 최적화
- 충분한 메모리 확보 (최소 4GB 권장)
- SSD 사용 권장
- CPU 코어 수에 맞는 워커 수 설정

### 3. 시간대 최적화
- 서버 부하가 적은 시간대 선택
- 한국 시간 새벽 2-6시 권장

## 🚨 주의사항

1. **서버 부하**: 과도한 요청으로 서버에 부하를 주지 마세요
2. **데이터 용량**: 충분한 디스크 공간 확보 (최소 10GB)
3. **네트워크**: 안정적인 인터넷 연결 필수
4. **전력**: 장시간 실행으로 인한 전력 소모 고려
5. **백업**: 중요한 데이터는 미리 백업

## 📞 지원

문제가 발생하거나 질문이 있으시면:
1. 로그 파일 확인
2. 현재 상태 모니터링
3. 설정 조정 시도
4. 필요시 크롤링 중단 및 재시작 