#!/usr/bin/env python3
"""
URL 기반 뉴스 기사 크롤러 (동아일보 전용)
 - 동아일보 기사 페이지를 크롤링하도록 특화된 셀렉터/패턴 반영
 - 입력: 단일 URL 또는 URL 목록 파일
 - 출력: 표준출력(JSON Lines) 또는 --out 파일(JSON Lines)
 - 옵션: --save-db 사용 시 DB 저장(donga_database_manager.db_manager 활용)
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
    from C_donga_database_manager import db_manager
except Exception:
    db_manager = None  # DB가 없어도 동작 가능하게 처리


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('donga_url_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# 동아일보 기본 URL 목록 (인자를 주지 않으면 이 목록을 사용)
DEFAULT_URLS: List[str] = [
    # 필요 시 아래 URL을 원하는 기사 URL로 변경하세요
    'https://www.donga.com/',
]

# 카테고리 하드코딩 입력 (인자 없으면 이 목록을 사용)
DEFAULT_CATEGORY_URLS: List[str] = [
    'https://www.donga.com/news/Opinion', # 오피니언
    'https://www.donga.com/news/Politics', # 정치
    'https://www.donga.com/news/Economy', # 경제
    'https://www.donga.com/news/Inter', #국제
    'https://www.donga.com/news/Society', #사회
    'https://www.donga.com/news/Culture', #문화
    'https://www.donga.com/news/Entertainment', #연예
    'https://www.donga.com/news/Sports', #스포츠
    'https://www.donga.com/news/Health', #헬스동아
    'https://www.donga.com/news/TrendNews/daily' #트렌드뉴스
]

# 카테고리별 한글명 매핑 (동아일보)
CATEGORY_MAP = {
    'https://www.donga.com/news/Opinion': '오피니언',
    'https://www.donga.com/news/Politics': '정치',
    'https://www.donga.com/news/Economy': '경제',
    'https://www.donga.com/news/Inter': '국제',
    'https://www.donga.com/news/Society': '사회',
    'https://www.donga.com/news/Culture': '문화',
    'https://www.donga.com/news/Entertainment': '연예',
    'https://www.donga.com/news/Sports': '스포츠',
    'https://www.donga.com/news/Health': '헬스동아',
    'https://www.donga.com/news/TrendNews/daily': '트렌드뉴스'
}

# 카테고리 하드코딩시 최대 탐색 페이지 수
DEFAULT_CATEGORY_PAGES: int = 1


class UrlArticleCrawler:
    """동아일보 URL 기사 크롤러"""

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
            'www.donga.com': '동아일보',
            'donga.com': '동아일보',
            'news.donga.com': '동아일보',
            'www.sisaon.co.kr': '시사오늘',
            'sisaon.co.kr': '시사오늘',
        }
        return mapping.get(netloc.lower(), netloc)

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        # 동아일보 특화 셀렉터
        selectors = [
            'meta[property="og:title"]',
            'meta[name="twitter:title"]',
            '.article_title',  # 동아일보 기사 제목
            '.news_title',     # 동아일보 뉴스 제목
            '.title',          # 일반 제목
            'h1',              # H1 태그
        ]
        
        for sel in selectors:
            tag = soup.select_one(sel)
            if tag and tag.get('content'):
                title = self._clean_text(tag['content'])
                if 5 < len(title) < 200:
                    return title
            elif tag:
                title = self._clean_text(tag.get_text())
                if 5 < len(title) < 200:
                    return title

        # title 태그에서 추출
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

        # 2) 동아일보 특화 셀렉터
        candidates = []
        for sel in [
            '.reporter', '.byline', '.writer', '.article-info', '.news-info',
            '.writer-name', '.article-writer', '.article_writer', 'span[class*="writer"]',
            'span[class*="name"]', 'div[class*="byline"]', 'em[class*="name"]',
            # 동아일보 특화 셀렉터
            '.reporter-name', '.reporter_name', '.journalist', '.journalist-name',
            '.byline-name', '.byline_name', '.writer-info', '.writer_info',
            '.article-meta', '.article_meta', '.news-meta', '.news_meta',
            '.article_author', '.news_author', '.author_info'
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
        # 동아일보 특화 셀렉터
        selectors = [
            '.article_body', '.article-body', '.news_body', '.news-body',
            '.article_content', '.news_content', '.article-text', '.article_text',
            '.content', '.story-body', '.story_body', '.post-content', '.post_content',
            '.entry-content', '.entry_content', '.main-content', '.main_content',
            '.article', 'article', '#article', '.news-article', '.view_body'
        ]
        
        for sel in selectors:
            container = soup.select_one(sel)
            if not container:
                continue

            # 광고, 스크립트 등 제거
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
        for i, url in enumerate(urls, 1):
            url = url.strip()
            if not url:
                continue
            logger.info(f"[{i}] 크롤링: {url}")
            data = self.extract_article_data(url)
            if data:
                results.append(data)
                if save_db and db_manager:
                    try:
                        if not getattr(db_manager, 'connection_pool', None):
                            db_manager.initialize_pool()
                            db_manager.create_tables()
                        db_manager.save_article(data)
                        logger.info(f"DB 저장 성공: {url}")
                    except Exception as e:
                        logger.warning(f"DB 저장 실패: {e}")
            else:
                logger.warning(f"추출 실패: {url}")

            time.sleep(self.request_delay)
        return results

    def _extract_article_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        # 동아일보 기사 링크 패턴 위주로 수집
        patterns = [
            # 동아일보 기사 URL 패턴
            re.compile(r'/news/\w+/\d{4}/\d{2}/\d{2}/\d+', re.I),
            re.compile(r'/news/\w+/\d{4}/\d{2}/\d{2}/[A-Z0-9]+', re.I),
            # 기존 패턴도 유지
            re.compile(r'/article/view\.asp\?arcid=\d+', re.I),
            re.compile(r'view\.asp\?arcid=\d+', re.I),
        ]
        links: List[str] = []
        seen = set()
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            if not href:
                continue
                
            # 동아일보 도메인 확인
            if 'donga.com' in href or href.startswith('/'):
                for pat in patterns:
                    if pat.search(href):
                        full = self._normalize_url(current_url, href)
                        if full and full not in seen:
                            # 동아일보 기사 URL인지 추가 검증
                            if self._is_donga_article_url(full):
                                links.append(full)
                                seen.add(full)
                        break
        return links

    def _is_donga_article_url(self, url: str) -> bool:
        """동아일보 기사 URL인지 확인"""
        try:
            parsed = urlparse(url)
            if 'donga.com' not in parsed.netloc:
                return False
            
            path = parsed.path.strip('/')
            if not path:
                return False
                
            # 동아일보 기사 URL 패턴: news/카테고리/년/월/일/고유ID
            if path.startswith('news/'):
                parts = path.split('/')
                if len(parts) >= 6:
                    # 년/월/일 형식 확인
                    if (re.match(r'^\d{4}$', parts[-3]) and 
                        re.match(r'^\d{2}$', parts[-2]) and 
                        re.match(r'^\d{2}$', parts[-1])):
                        return True
                    # 또는 마지막 부분이 고유ID인지 확인
                    elif (re.match(r'^\d{4}$', parts[-4]) and 
                          re.match(r'^\d{2}$', parts[-3]) and 
                          re.match(r'^\d{2}$', parts[-2]) and
                          re.match(r'^[A-Z0-9]+$', parts[-1])):
                        return True
            
            return False
        except Exception:
            return False

    def _derive_category_name(self, category_url: str) -> Optional[str]:
        try:
            parsed = urlparse(category_url)
            path = parsed.path.lower().strip('/')
            
            # 동아일보는 경로 기반으로 카테고리를 구분하므로 경로를 우선 분석
            if path:
                # 경로 기반 카테고리 매핑 (동아일보 URL 구조)
                path_map = {
                    'news/opinion': '오피니언',
                    'news/politics': '정치', 
                    'news/economy': '경제',
                    'news/inter': '국제',
                    'news/society': '사회',
                    'news/culture': '문화',
                    'news/entertainment': '연예',
                    'news/sports': '스포츠',
                    'news/health': '헬스동아',
                    'news/trendnews/daily': '트렌드뉴스'
                }
                
                # 정확한 경로 매칭
                for key, value in path_map.items():
                    if path == key or path.startswith(f"{key}/"):
                        return value
                
                # 부분 경로 매칭 (fallback)
                for key, value in path_map.items():
                    if key in path:
                        return value
            
            # 쿼리 파라미터 기반 매핑 (기존 로직 유지)
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
                
            # 최종 fallback: 경로의 마지막 부분으로 매핑
            tail = os.path.basename(path).lower()
            tail_map = {
                'economy': '경제', 'eco': '경제',
                'politics': '정치', 'pol': '정치',
                'national': '사회', 'soc': '사회',
                'international': '국제', 'world': '국제', 'int': '국제',
                'medical': '건강', 'health': '건강',
                'investment': '제테크', 'jetaek': '제테크',
                'sports': '스포츠', 'sport': '스포츠', 'spo': '스포츠',
                'culture': '문화', 'entertainment': '연예',
                'opinion': '오피니언', 'op': '오피니언',
                'life': '라이프', 'lif': '라이프',
                'entertainment': '연예', 'ent': '연예', 'ens': '연예',
                'golf': '골프',
                'travel': '여행',
                'esports': 'e스포츠', 'e-sports': 'e스포츠', 'esport': 'e스포츠',
                'mission': '더미션',
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
        for idx, cat_url in enumerate(category_urls, 1):
            cat_url = cat_url.strip()
            if not cat_url:
                continue
            logger.info(f"[{idx}] 카테고리 크롤링 시작: {cat_url} (최대 {max_pages}페이지)")
            article_links = self.collect_article_links_from_category(cat_url, max_pages=max_pages)
            # 카테고리명 유추 (URL 파라미터 sid1 → 한글명)
            category_name = self._derive_category_name(cat_url)
            for aidx, article_url in enumerate(article_links, 1):
                logger.info(f"  - 기사 {aidx}/{len(article_links)}: {article_url}")
                data = self.extract_article_data(article_url)
                if not data:
                    continue
                # 카테고리명 설정
                if category_name:
                    data['categories'] = [category_name]
                if save_db and db_manager:
                    try:
                        if not getattr(db_manager, 'connection_pool', None):
                            db_manager.initialize_pool()
                            db_manager.create_tables()
                        if db_manager.save_article(data):
                            total_saved += 1
                            logger.info(f"  - DB 저장 성공: {article_url}")
                        else:
                            logger.warning(f"  - DB 저장 실패: {article_url}")
                    except Exception as e:
                        logger.warning(f"DB 저장 실패: {e}")
                else:
                    total_saved += 1  # 저장 안하지만 수집 건수 카운트
                time.sleep(self.request_delay)
        logger.info(f"카테고리 크롤링 완료. 처리 기사 수: {total_saved}")
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
    parser = argparse.ArgumentParser(description='동아일보 URL 기반 뉴스 기사 크롤러')
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

    # DB 초기화(필요 시)
    if save_to_db and db_manager:
        try:
            if not getattr(db_manager, 'connection_pool', None):
                db_manager.initialize_pool()
                db_manager.create_tables()
                logger.info("데이터베이스 연결 및 테이블 생성 완료")
        except Exception as e:
            logger.warning(f'DB 초기화 실패(계속 진행): {e}')

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
                logger.info('입력된 URL이 없어 기본 동아일보 URL로 실행합니다.')
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