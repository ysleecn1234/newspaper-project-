#!/usr/bin/env python3
"""
URL 기반 뉴스 기사 크롤러
 - 한국경제(hankyung.com) 기사 페이지 크롤링에 맞춘 셀렉터/패턴 적용
 - 입력: 단일 URL 또는 URL 목록 파일
 - 출력: 표준출력(JSON Lines) 또는 --out 파일(JSON Lines)
 - 옵션: --save-db 사용 시 DB 저장(choongang_database_manager.db_manager 활용)
"""

import argparse
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse, urljoin, parse_qs
from collections import deque

import requests
from bs4 import BeautifulSoup

try:
    from C_ke_database_manager import db_manager
    DB_AVAILABLE = True
except Exception as e:
    logger = logging.getLogger(__name__)
    logger.warning(f"데이터베이스 매니저를 불러올 수 없습니다: {e}")
    db_manager = None
    DB_AVAILABLE = False


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('choongang_url_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# 서울신문 기본 URL 목록 (인자를 주지 않으면 이 목록을 사용)
DEFAULT_URLS: List[str] = [
    # 필요 시 아래 URL을 원하는 기사 URL로 변경하세요
    'https://www.hankyung.com/',
]

# 카테고리 하드코딩 입력 (인자 없으면 이 목록을 사용)
DEFAULT_CATEGORY_URLS: List[str] = [
    'https://www.hankyung.com/all-news', #전체
    'https://www.hankyung.com/all-news-opinion', #오피니언
    'https://www.hankyung.com/all-news-economy', #경제
    'https://www.hankyung.com/all-news-politics', #정치
    'https://www.hankyung.com/all-news-society', #사회
    'https://www.hankyung.com/all-news-finance', #증권
    'https://www.hankyung.com/all-news-realestate', #부동산
    'https://www.hankyung.com/all-news-international', #국제
    'https://www.hankyung.com/all-news-it', #IT/과학
    'https://www.hankyung.com/all-news-life', #생활/문화
    'https://www.hankyung.com/all-news-sports', #스포츠
    'https://www.hankyung.com/all-news-entertainment', #연예   
]

# 카테고리별 한글명 매핑 (한국경제)
CATEGORY_MAP = {
    'https://www.hankyung.com/all-news': '전체',
    'https://www.hankyung.com/all-news-opinion': '오피니언',
    'https://www.hankyung.com/all-news-economy': '경제',
    'https://www.hankyung.com/all-news-politics': '정치',
    'https://www.hankyung.com/all-news-society': '사회',
    'https://www.hankyung.com/all-news-finance': '증권',
    'https://www.hankyung.com/all-news-realestate': '부동산',
    'https://www.hankyung.com/all-news-international': '국제',
    'https://www.hankyung.com/all-news-it': 'IT/과학',
    'https://www.hankyung.com/all-news-life': '생활/문화',
    'https://www.hankyung.com/all-news-sports': '스포츠',
    'https://www.hankyung.com/all-news-entertainment': '연예',
}

# 카테고리 하드코딩시 최대 탐색 페이지 수
DEFAULT_CATEGORY_PAGES: int = 1


class UrlArticleCrawler:
    """일반 URL 기사 크롤러 (요청/파싱 로직은 sisaon 크롤러 스타일 참고)"""

    def __init__(self, timeout: int = 15, max_retries: int = 3, request_delay: float = 0.8):
        self.timeout = timeout
        self.max_retries = max_retries
        self.request_delay = request_delay

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _initialize_database(self) -> bool:
        """데이터베이스 초기화 및 테이블 생성"""
        if not DB_AVAILABLE or not db_manager:
            logger.error("데이터베이스 매니저를 사용할 수 없습니다.")
            return False
        
        try:
            # 연결 풀 초기화
            if not getattr(db_manager, 'connection_pool', None):
                logger.info("데이터베이스 연결 풀을 초기화합니다...")
                if not db_manager.initialize_pool():
                    logger.error("데이터베이스 연결 풀 초기화에 실패했습니다.")
                    return False
            
            # 테이블 생성
            logger.info("데이터베이스 테이블을 생성합니다...")
            if not db_manager.create_tables():
                logger.error("데이터베이스 테이블 생성에 실패했습니다.")
                return False
            
            logger.info("데이터베이스 초기화가 완료되었습니다.")
            return True
            
        except Exception as e:
            logger.error(f"데이터베이스 초기화 중 오류 발생: {e}")
            return False

    def _save_article_to_db(self, article_data: Dict[str, Any]) -> bool:
        """기사를 데이터베이스에 저장"""
        if not DB_AVAILABLE or not db_manager:
            logger.warning("데이터베이스 매니저를 사용할 수 없어 저장을 건너뜁니다.")
            return False
        
        try:
            # published_date가 datetime 객체인지 확인하고 문자열로 변환
            if isinstance(article_data.get('published_date'), datetime):
                article_data['published_date'] = article_data['published_date'].isoformat()
            
            # DB에 저장
            if db_manager.save_article(article_data):
                logger.info(f"기사 저장 성공: {article_data.get('title', '제목 없음')}")
                return True
            else:
                logger.warning(f"기사 저장 실패 (중복 또는 오류): {article_data.get('title', '제목 없음')}")
                return False
                
        except Exception as e:
            logger.error(f"기사 저장 중 오류 발생: {e}")
            return False

    @staticmethod
    def _normalize_url(base_url: str, href: str) -> Optional[str]:
        if not href:
            return None
        href = href.strip()
        if href.startswith('javascript:') or href.startswith('#'):
            return None
        if href.startswith('http'):
            return href
        if href.startswith('/'):
            parsed = urlparse(base_url)
            return f"{parsed.scheme}://{parsed.netloc}{href}"
        return urljoin(base_url, href)

    def _make_request(self, url: str) -> Optional[requests.Response]:
        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()

                # 인코딩 추정 개선: 헤더-메타 우선, 실패 시 utf-8 시도
                if not resp.encoding or resp.encoding.lower() in ['iso-8859-1', 'ascii']:
                    try:
                        soup = BeautifulSoup(resp.content, 'html.parser')
                        meta_charset = soup.find('meta', charset=True)
                        if meta_charset and meta_charset.get('charset'):
                            resp.encoding = meta_charset['charset']
                        else:
                            meta_content = soup.find('meta', attrs={'http-equiv': 'Content-Type'})
                            if meta_content and 'charset=' in (meta_content.get('content') or ''):
                                resp.encoding = meta_content['content'].split('charset=')[-1].strip()
                        if not resp.encoding:
                            resp.encoding = 'utf-8'
                    except Exception:
                        resp.encoding = 'utf-8'

                return resp
            except requests.RequestException as e:
                logger.warning(f"요청 실패({attempt+1}/{self.max_retries}): {url} - {e}")
                time.sleep(min(2 ** attempt, 4))
        logger.error(f"요청 최종 실패: {url}")
        return None

    @staticmethod
    def _clean_text(text: str) -> str:
        if not text:
            return ''
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    @staticmethod
    def _korean_source_from_domain(netloc: str) -> str:
        mapping = {
            'www.seoul.co.kr': '서울신문',
            'seoul.co.kr': '서울신문',
            'www.hankyung.com': '한국경제',
            'hankyung.com': '한국경제',
            'www.chosun.com': '조선일보',
            'chosun.com': '조선일보',
            'news.chosun.com': '조선일보',
            'www.sisaon.co.kr': '시사오늘',
            'sisaon.co.kr': '시사오늘',
        }
        return mapping.get(netloc.lower(), netloc)

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        selectors = [
            'meta[property="og:title"]',
            'meta[name="twitter:title"]',
        ]
        for sel in selectors:
            tag = soup.select_one(sel)
            if tag and tag.get('content'):
                title = self._clean_text(tag['content'])
                if 5 < len(title) < 200:
                    return title

        for sel in ['h1', '.article-title', '.news-title', '.title']:
            tag = soup.select_one(sel)
            if tag:
                title = self._clean_text(tag.get_text())
                if 5 < len(title) < 200:
                    return title

        tag = soup.find('title')
        if tag:
            title = self._clean_text(tag.get_text())
            title = re.sub(r'\s*[-|]\s*.*$', '', title)
            if 5 < len(title) < 200:
                return title
        return None

    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        # 0) JSON-LD에서 author 탐색
        try:
            for script in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script.string or script.text or '{}')
                except Exception:
                    continue
                blocks = data if isinstance(data, list) else [data]
                for block in blocks:
                    author = None
                    if isinstance(block, dict):
                        if 'author' in block:
                            author = block.get('author')
                        elif 'creator' in block:
                            author = block.get('creator')
                    if author:
                        # author가 문자열/객체/배열 모두 대응
                        if isinstance(author, str):
                            name = self._clean_text(author)
                        elif isinstance(author, dict):
                            name = self._clean_text(author.get('name', ''))
                        elif isinstance(author, list):
                            if author and isinstance(author[0], dict):
                                name = self._clean_text(author[0].get('name', ''))
                            elif author and isinstance(author[0], str):
                                name = self._clean_text(author[0])
                            else:
                                name = ''
                        else:
                            name = ''
                        name = re.sub(r'\s*기자$', '', name)
                        if 2 <= len(name) <= 15 and re.search(r'[가-힣]', name):
                            return name
        except Exception:
            pass

        # 1) 메타 태그 우선
        for sel, attr in [
            ('meta[name="author"]', 'content'),
            ('meta[property="article:author"]', 'content'),
            ('meta[name="byline"]', 'content'),
            ('meta[name="dable:author"]', 'content'),
            ('meta[name="twitter:creator"]', 'content'),
        ]:
            tag = soup.select_one(sel)
            if tag and tag.get(attr):
                author = self._clean_text(tag.get(attr))
                author = re.sub(r'\s*기자$', '', author)
                if 2 <= len(author) <= 15 and re.search(r'[가-힣]', author):
                    return author

        # 2) mailto 기반 추정
        for a in soup.select('a[href^="mailto:"]'):
            parent_text = a.parent.get_text(' ', strip=True) if a.parent else ''
            text = a.get_text(' ', strip=True)
            blob = ' '.join([parent_text, text])
            m = re.search(r'([가-힣]{2,4})\s*기자', blob)
            if m:
                name = m.group(1)
                if 2 <= len(name) <= 15:
                    return name

        # 3) 국민일보 특화/일반 셀렉터 확장
        candidates = []
        for sel in [
            '.author', '.reporter', '.byline', '.writer', '.article-info', '.news-info',
            '.writer-name', '.article-writer', '.article_writer', 'span[class*="writer"]',
            'span[class*="name"]', 'div[class*="byline"]', 'em[class*="name"]',
            # 조선일보 특화 셀렉터
            '.reporter-name', '.reporter_name', '.journalist', '.journalist-name',
            '.byline-name', '.byline_name', '.writer-info', '.writer_info',
            '.article-meta', '.article_meta', '.news-meta', '.news_meta'
        ]:
            tag = soup.select_one(sel)
            if tag:
                candidates.append(tag.get_text(' ', strip=True))

        # 본문/헤더 인접 문단에서 검색 범위 확대
        for p in soup.find_all('p')[:12]:
            candidates.append(p.get_text(' ', strip=True))
        for span in soup.find_all('span')[:20]:
            candidates.append(span.get_text(' ', strip=True))

        # 패턴들: "홍길동 기자", "홍길동 기자 gildong@..."
        patterns = [
            r'([가-힣]{2,4})\s*기자',
            r'기자\s*([가-힣]{2,4})',
        ]
        for text in candidates:
            if not text:
                continue
            if '기자' not in text:
                continue
            for pat in patterns:
                m = re.search(pat, text)
                if m:
                    name = m.group(1)
                    if 2 <= len(name) <= 15:
                        return name
        return None

    def _extract_published_date(self, soup: BeautifulSoup) -> Optional[datetime]:
        # 메타 태그 우선
        for sel in [
            'meta[property="article:published_time"]',
            'meta[name="article:published_time"]',
            'meta[name="publish_date"]',
            'meta[name="date"]',
            'meta[property="og:published_time"]',
        ]:
            tag = soup.select_one(sel)
            if tag and tag.get('content'):
                value = tag['content']
                try:
                    return datetime.fromisoformat(value.replace('Z', '+00:00'))
                except Exception:
                    pass

        # time 태그
        t = soup.find('time')
        if t:
            for key in ['datetime', 'content']:
                if t.get(key):
                    try:
                        return datetime.fromisoformat(t.get(key).replace('Z', '+00:00'))
                    except Exception:
                        continue
        return None

    def _extract_content(self, soup: BeautifulSoup) -> Optional[str]:
        selectors = [
            # 한국경제 주요 본문 컨테이너 후보
            '#articletxt', 'div#articletxt',
            '.article-body', '.article-body__content', '.article-content',
            '.news-body', '.news_content', '.news-article',
            '.view_body', '.article_txt', '.articleText',
            'article .content', 'article .contents',
            '#articleBody', '#article-body', '#newsBody', '#CmAdContent',
            'article', '.article', '#article',
            '.article_text', '.article-text', '.content', '.news-content',
            '.story-body', '.story_body', '.post-content', '.post_content',
            '.entry-content', '.entry_content', '.main-content', '.main_content'
        ]
        for sel in selectors:
            container = soup.select_one(sel)
            if not container:
                continue

            for unwanted in container.select('script, style, .ad, .advertisement, .banner, .related-articles, .social, .tag, .recommend'):
                unwanted.decompose()

            # 첫 문단에 기자표기가 섞인 경우 제거
            ps = container.find_all('p')
            if ps:
                first = ps[0].get_text(' ', strip=True)
                if '기자' in first and ('=' in first or '·' in first or '|' in first):
                    ps[0].decompose()

            text = self._clean_text(container.get_text(' ', strip=True))
            if text and len(text) > 100:
                return text
        return None

    def extract_article_data(self, url: str) -> Optional[Dict[str, Any]]:
        resp = self._make_request(url)
        if not resp:
            return None
        soup = BeautifulSoup(resp.text, 'html.parser')

        title = self._extract_title(soup)
        content = self._extract_content(soup)
        author = self._extract_author(soup)
        published_date = self._extract_published_date(soup)

        if not title or not content:
            logger.warning(f"필수 필드 부재(title/content): {url}")
            return None

        netloc = urlparse(url).netloc
        source = self._korean_source_from_domain(netloc)

        article = {
            'title': title,
            'content': content,
            'url': url,
            'source': source,
            'author': author,
            'published_date': published_date,
            'categories': [],
            'tags': [],
            'metadata': {
                'crawled_at': datetime.now().isoformat(),
                'domain': netloc,
            },
        }
        return article

    def crawl_urls(self, urls: Iterable[str], save_db: bool = False) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        saved_count = 0
        
        # DB 저장이 필요한 경우 초기화
        if save_db:
            if not self._initialize_database():
                logger.error("데이터베이스 초기화에 실패했습니다. DB 저장을 건너뜁니다.")
                save_db = False
        
        for i, url in enumerate(urls, 1):
            url = url.strip()
            if not url:
                continue
            logger.info(f"[{i}] 크롤링: {url}")
            data = self.extract_article_data(url)
            if data:
                results.append(data)
                if save_db:
                    if self._save_article_to_db(data):
                        saved_count += 1
                        logger.info(f"DB 저장 완료 ({saved_count}건): {data.get('title', '제목 없음')}")
                    else:
                        logger.warning(f"DB 저장 실패: {data.get('title', '제목 없음')}")
            else:
                logger.warning(f"추출 실패: {url}")

            time.sleep(self.request_delay)
        
        if save_db:
            logger.info(f"총 {len(results)}건 중 {saved_count}건을 DB에 저장했습니다.")
        
        return results

    def _extract_article_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        # 한국경제 기사 링크 패턴 수집: /(section/)?article/{digits}
        patterns = [
            re.compile(r'/(?:[a-z\-]+/)?article/\d{8,}', re.I),
        ]
        links: List[str] = []
        seen = set()
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            if not href:
                continue
            
            # 한국경제 도메인 또는 상대경로 확인
            if 'hankyung.com' in href or href.startswith('/'):
                for pat in patterns:
                    if pat.search(href):
                        full = self._normalize_url(current_url, href)
                        if full and full not in seen:
                            # 한국경제 기사 URL인지 추가 검증
                            if self._is_hankyung_article_url(full):
                                links.append(full)
                                seen.add(full)
                        break
        return links

    def _is_hankyung_article_url(self, url: str) -> bool:
        """한국경제 기사 URL인지 확인"""
        try:
            parsed = urlparse(url)
            if 'hankyung.com' not in parsed.netloc:
                return False
            # 대표 기사 URL 패턴 확인: /(section/)?article/{digits}
            if re.search(r'/(?:[a-z\-]+/)?article/\d{8,}', parsed.path + ('?' + parsed.query if parsed.query else ''), re.I):
                return True
            return False
        except Exception:
            return False

    def _derive_category_name(self, category_url: str) -> Optional[str]:
        try:
            parsed = urlparse(category_url)
            path = parsed.path.lower().strip('/')

            # 0) 하드코딩된 CATEGORY_MAP이 있으면 우선 사용
            try:
                if category_url in CATEGORY_MAP:
                    return CATEGORY_MAP[category_url]
            except Exception:
                pass

            # 1) 경로 기반 분석 (한국경제 all-news-* 형태 포함)
            if path:
                # 경로 기반 카테고리 매핑 (한국경제 URL 구조)
                path_map = {
                    'all-news': '전체',
                    'all-news-opinion': '오피니언',
                    'all-news-economy': '경제',
                    'all-news-politics': '정치',
                    'all-news-society': '사회',
                    'all-news-finance': '증권',
                    'all-news-realestate': '부동산',
                    'all-news-international': '국제',
                    'all-news-it': 'IT/과학',
                    'all-news-life': '생활/문화',
                    'all-news-sports': '스포츠',
                    'all-news-entertainment': '연예',
                    # 일반 섹션 경로도 보조 매핑
                    'economy': '경제',
                    'politics': '정치',
                    'society': '사회',
                    'international': '국제',
                    'it': 'IT/과학',
                    'life': '생활/문화',
                    'sports': '스포츠',
                    'entertainment': '연예',
                    'finance': '증권',
                    'realestate': '부동산',
                    'opinion': '오피니언',
                }
                
                # 정확한 경로 매칭
                for key, value in path_map.items():
                    if path == key or path.startswith(f"{key}/"):
                        return value
                
                # 부분 경로 매칭 (fallback)
                for key, value in path_map.items():
                    if key in path:
                        return value
            
            # 2) 쿼리 파라미터 기반 매핑 (한국경제에는 거의 사용되지 않음 - 호환 유지)
            qs = parse_qs(parsed.query)
            sid1 = (qs.get('sid1') or [''])[0].lower()
            sid2 = (qs.get('sid2') or [''])[0].lower()

            if sid1 == 'ens':
                ens_map = {
                    '0005': '연예',
                    '0001': '스포츠',
                    '0004': '골프',
                }
                if sid2 in ens_map:
                    return ens_map[sid2]

            sid1_map = {
                'eco': '경제',
                'pol': '정치',
                'soc': '사회',
                'int': '국제',
                'lif': '라이프',
            }
            if sid1 in sid1_map:
                return sid1_map[sid1]

            # 3) 기타 특수 경로 처리 (필요 시 확장)
            if 'list_travel.asp' in path:
                return '여행'
            if 'list_esports.asp' in path:
                return 'e스포츠'
            if 'list_mission.asp' in path:
                return '더미션'
                
            # 4) 최종 fallback: 경로의 마지막 부분으로 매핑
            tail = os.path.basename(path).lower()
            tail_map = {
                'economy': '경제', 'eco': '경제',
                'politics': '정치', 'pol': '정치',
                'national': '사회', 'soc': '사회', 'society': '사회',
                'international': '국제', 'world': '국제', 'int': '국제',
                'medical': '건강', 'health': '건강',
                'investment': '제테크', 'jetaek': '제테크',
                'sports': '스포츠', 'sport': '스포츠', 'spo': '스포츠',
                'culture-style': '문화/연예', 'culture': '문화/연예', 'entertainment': '연예',
                'opinion': '오피니언', 'op': '오피니언',
                'life': '생활/문화', 'lif': '생활/문화',
                'ent': '연예', 'ens': '연예',
                'finance': '증권', 'realestate': '부동산', 'it': 'IT/과학',
            }
            return tail_map.get(tail)
        except Exception:
            return None

    def _extract_pagination_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        # 다음/페이지 번호 등의 페이지네이션 링크 수집
        candidates: List[str] = []
        for a in soup.find_all('a', href=True):
            text = (a.get_text() or '').strip()
            href = a.get('href')
            if not href:
                continue
            if any(token in href.lower() for token in ['page', 'pageno', 'pageindex', 'pagenum', 'pageNo']):
                full = self._normalize_url(current_url, href)
                if full:
                    candidates.append(full)
                continue
            if text in ['다음', '다음>', '>', '>>', '끝', '마지막']:
                full = self._normalize_url(current_url, href)
                if full:
                    candidates.append(full)
        # 동일 페이지 중복 제거
        unique: List[str] = []
        seen = set()
        for u in candidates:
            if u not in seen:
                unique.append(u)
                seen.add(u)
        return unique

    def collect_article_links_from_category(self, category_url: str, max_pages: int = 1) -> List[str]:
        visited_pages = set()
        queue: deque[str] = deque([category_url])
        collected: List[str] = []

        while queue and len(visited_pages) < max_pages:
            page_url = queue.popleft()
            if page_url in visited_pages:
                continue
            resp = self._make_request(page_url)
            if not resp:
                continue
            soup = BeautifulSoup(resp.text, 'html.parser')
            # 기사 링크 수집
            links = self._extract_article_links(soup, page_url)
            collected.extend(links)
            visited_pages.add(page_url)
            # 다음 페이지 후보 수집
            for next_url in self._extract_pagination_links(soup, page_url):
                if next_url not in visited_pages and next_url not in queue and urlparse(next_url).netloc == urlparse(category_url).netloc:
                    queue.append(next_url)
            time.sleep(self.request_delay)

        # 중복 제거 및 정렬
        unique = sorted(list({u for u in collected}))
        logger.info(f"카테고리에서 기사 링크 {len(unique)}건 수집: {category_url}")
        return unique

    def crawl_category_urls(self, category_urls: Iterable[str], max_pages: int = 1, save_db: bool = False) -> int:
        total_saved = 0
        total_processed = 0
        
        # DB 저장이 필요한 경우 초기화
        if save_db:
            if not self._initialize_database():
                logger.error("데이터베이스 초기화에 실패했습니다. DB 저장을 건너뜁니다.")
                save_db = False
        
        for idx, cat_url in enumerate(category_urls, 1):
            cat_url = cat_url.strip()
            if not cat_url:
                continue
            logger.info(f"[{idx}] 카테고리 크롤링 시작: {cat_url} (최대 {max_pages}페이지)")
            article_links = self.collect_article_links_from_category(cat_url, max_pages=max_pages)
            # 카테고리명 유추 (URL 파라미터 sid1 → 한글명)
            category_name = self._derive_category_name(cat_url)
            
            category_saved = 0
            for aidx, article_url in enumerate(article_links, 1):
                logger.info(f"  - 기사 {aidx}/{len(article_links)}: {article_url}")
                data = self.extract_article_data(article_url)
                if not data:
                    continue
                
                total_processed += 1
                
                # 카테고리명 설정
                if category_name:
                    data['categories'] = [category_name]
                
                if save_db:
                    if self._save_article_to_db(data):
                        total_saved += 1
                        category_saved += 1
                        logger.info(f"  - DB 저장 성공 ({category_saved}건): {data.get('title', '제목 없음')}")
                    else:
                        logger.warning(f"  - DB 저장 실패: {data.get('title', '제목 없음')}")
                else:
                    total_saved += 1  # 저장 안하지만 수집 건수 카운트
                
                time.sleep(self.request_delay)
            
            logger.info(f"카테고리 '{category_name or cat_url}' 완료: {len(article_links)}건 중 {category_saved}건 저장")
        
        if save_db:
            logger.info(f"카테고리 크롤링 완료. 총 {total_processed}건 중 {total_saved}건을 DB에 저장했습니다.")
        else:
            logger.info(f"카테고리 크롤링 완료. 총 {total_processed}건을 처리했습니다.")
        
        return total_saved


def _load_urls(args: argparse.Namespace) -> List[str]:
    urls: List[str] = []
    if args.url:
        urls.extend(args.url)
    if args.url_file and os.path.exists(args.url_file):
        with open(args.url_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    urls.append(line)
    # 중복 제거, 순서 유지
    seen = set()
    deduped: List[str] = []
    for u in urls:
        if u not in seen:
            deduped.append(u)
            seen.add(u)
    return deduped


def main():
    parser = argparse.ArgumentParser(description='URL 기반 뉴스 기사 크롤러')
    parser.add_argument('--url', action='append', help='크롤링할 URL (여러 번 지정 가능)')
    parser.add_argument('--url-file', type=str, help='URL 목록 파일 경로(줄바꿈 구분)')
    parser.add_argument('--category-url', action='append', help='카테고리 URL (여러 번 지정 가능)')
    parser.add_argument('--category-file', type=str, help='카테고리 URL 목록 파일 경로(줄바꿈 구분)')
    parser.add_argument('--category-pages', type=int, default=1, help='카테고리별 최대 탐색 페이지 수')
    parser.add_argument('--out', type=str, help='결과 저장 파일 경로(JSON Lines)')
    parser.add_argument('--save-db', action='store_true', help='DB에 저장(기본값: 저장)')
    parser.add_argument('--no-save-db', action='store_true', help='DB 저장 비활성화')
    parser.add_argument('--delay', type=float, default=0.8, help='요청 간 대기(초)')
    args = parser.parse_args()

    crawler = UrlArticleCrawler(request_delay=max(args.delay, 0.0))

    # 저장 기본값: True. --no-save-db가 있으면 False
    save_to_db = True
    if getattr(args, 'no_save_db', False):
        save_to_db = False
    elif getattr(args, 'save_db', False):
        save_to_db = True

    # DB 초기화는 크롤러 인스턴스에서 처리됩니다

    # 카테고리 우선 실행
    category_urls: List[str] = []
    if args.category_url:
        category_urls.extend(args.category_url)
    if args.category_file and os.path.exists(args.category_file):
        with open(args.category_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    category_urls.append(line)

    results: List[Dict[str, Any]] = []
    if category_urls:
        # 카테고리 크롤링은 DB에 직접 저장 중심으로 동작
        crawler.crawl_category_urls(category_urls, max_pages=max(1, args.category_pages), save_db=save_to_db)
    else:
        # 개별 URL 크롤링
        urls = _load_urls(args)
        # 인자 없고, 카테고리 하드코딩이 있으면 카테고리 우선 사용
        if not urls and DEFAULT_CATEGORY_URLS:
            logger.info('입력된 URL이 없어 하드코딩된 카테고리 URL로 실행합니다.')
            crawler.crawl_category_urls(DEFAULT_CATEGORY_URLS, max_pages=max(1, DEFAULT_CATEGORY_PAGES), save_db=save_to_db)
        else:
            if not urls:
                logger.info('입력된 URL이 없어 기본 한국경제 URL로 실행합니다.')
                urls = list(DEFAULT_URLS)
            results = crawler.crawl_urls(urls, save_db=save_to_db)

    # 출력
    if args.out and results:
        with open(args.out, 'w', encoding='utf-8') as f:
            for item in results:
                # datetime 직렬화 처리
                serializable = dict(item)
                if isinstance(serializable.get('published_date'), datetime):
                    serializable['published_date'] = serializable['published_date'].isoformat()
                f.write(json.dumps(serializable, ensure_ascii=False) + '\n')
        logger.info(f"결과 저장 완료: {args.out} ({len(results)}건)")
    elif results:
        for item in results:
            serializable = dict(item)
            if isinstance(serializable.get('published_date'), datetime):
                serializable['published_date'] = serializable['published_date'].isoformat()
            print(json.dumps(serializable, ensure_ascii=False))


if __name__ == '__main__':
    main()