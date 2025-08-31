#!/usr/bin/env python3
"""
RSS 기반 뉴스 기사 크롤러
 - 조선일보 RSS 피드를 파싱하여 기사 정보를 추출하고 DB에 저장
 - 입력: RSS URL 또는 RSS URL 목록 파일
 - 출력: 표준출력(JSON Lines) 또는 --out 파일(JSON Lines)
 - 옵션: --save-db 사용 시 DB 저장(chosun_database_manager.db_manager 활용)
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
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup

try:
    from chosun_database_manager import db_manager
except Exception:
    db_manager = None  # DB가 없어도 동작 가능하게 처리


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('chosun_rss_crawler.log', encoding='utf-8', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 로그 레벨 설정 (디버깅 시 더 자세한 정보를 위해)
logger.setLevel(logging.INFO)


# 조선일보 RSS URL 목록
DEFAULT_RSS_URLS: List[str] = [
    'https://www.chosun.com/arc/outboundfeeds/rss/category/politics/?outputType=xml', # 정치
    'https://www.chosun.com/arc/outboundfeeds/rss/category/economy/?outputType=xml', # 경제
    'https://www.chosun.com/arc/outboundfeeds/rss/category/national/?outputType=xml', # 사회
    'https://www.chosun.com/arc/outboundfeeds/rss/category/international/?outputType=xml', # 국제
    'https://www.chosun.com/arc/outboundfeeds/rss/category/culture-life/?outputType=xml', # 문화/라이프
    'https://www.chosun.com/arc/outboundfeeds/rss/category/opinion/?outputType=xml', # 오피니언
    'https://www.chosun.com/arc/outboundfeeds/rss/category/sports/?outputType=xml', # 스포츠
    'https://www.chosun.com/arc/outboundfeeds/rss/category/entertainments/?outputType=xml', # 연예
]

# 카테고리별 한글명 매핑 (조선일보 RSS용)
RSS_CATEGORY_MAP = {
    'politics': '정치',
    'economy': '경제',
    'national': '사회',
    'international': '국제',
    'culture-life': '문화/라이프',
    'opinion': '오피니언',
    'sports': '스포츠',
    'entertainments': '연예'
}


class RSSArticleCrawler:
    """RSS 피드 기사 크롤러"""

    def __init__(self, timeout: int = 15, max_retries: int = 3, request_delay: float = 0.8):
        self.timeout = timeout
        self.max_retries = max_retries
        self.request_delay = request_delay

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _make_request(self, url: str) -> Optional[requests.Response]:
        for attempt in range(self.max_retries):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                resp.raise_for_status()
                
                # RSS 피드 인코딩 처리 개선
                if 'xml' in resp.headers.get('content-type', '').lower():
                    # XML/RSS 피드의 경우 인코딩을 명시적으로 처리
                    try:
                        # 응답 내용을 바이트로 가져와서 인코딩 추정
                        content = resp.content
                        
                        # XML 선언에서 인코딩 확인
                        if content.startswith(b'<?xml'):
                            xml_declaration = content[:100].decode('utf-8', errors='ignore')
                            encoding_match = re.search(r'encoding=["\']([^"\']+)["\']', xml_declaration)
                            if encoding_match:
                                detected_encoding = encoding_match.group(1).lower()
                                if detected_encoding in ['utf-8', 'utf8']:
                                    resp.encoding = 'utf-8'
                                elif detected_encoding in ['euc-kr', 'cp949', 'ks_c_5601']:
                                    resp.encoding = 'cp949'
                                else:
                                    resp.encoding = detected_encoding
                                logger.info(f"XML 선언에서 인코딩 감지: {detected_encoding}")
                            else:
                                # XML 선언에 인코딩이 없으면 UTF-8로 가정
                                resp.encoding = 'utf-8'
                        else:
                            # XML 선언이 없으면 응답 헤더의 인코딩 사용
                            if resp.encoding and resp.encoding.lower() in ['utf-8', 'utf8']:
                                resp.encoding = 'utf-8'
                            elif resp.encoding and resp.encoding.lower() in ['euc-kr', 'cp949', 'ks_c_5601']:
                                resp.encoding = 'cp949'
                            else:
                                # 기본값으로 UTF-8 사용
                                resp.encoding = 'utf-8'
                        
                        # 인코딩 테스트
                        test_text = resp.text[:200]
                        if not test_text or '' in test_text or '?' in test_text:
                            # 인코딩 문제가 있으면 다른 인코딩 시도
                            for test_encoding in ['cp949', 'euc-kr', 'iso-8859-1']:
                                try:
                                    test_text = content.decode(test_encoding)
                                    if test_text and '' not in test_text and '?' not in test_text:
                                        resp.encoding = test_encoding
                                        logger.info(f"인코딩 재설정: {test_encoding}")
                                        break
                                except UnicodeDecodeError:
                                    continue
                    
                    except Exception as e:
                        logger.warning(f"인코딩 처리 중 오류: {e}")
                        # 기본값으로 UTF-8 사용
                        resp.encoding = 'utf-8'
                
                return resp
            except requests.RequestException as e:
                logger.warning(f"요청 실패({attempt+1}/{self.max_retries}): {url} - {e}")
                time.sleep(min(2 ** attempt, 4))
        logger.error(f"요청 최종 실패: {url}")
        return None

    def _parse_rss_feed(self, rss_content: str, rss_url: str) -> List[Dict[str, Any]]:
        """RSS 피드를 파싱하여 기사 정보 추출"""
        articles = []
        
        try:
            # 인코딩 문제가 있는 경우를 대비해 여러 방법으로 파싱 시도
            root = None
            
            # 1차 시도: 원본 내용으로 파싱
            try:
                root = ET.fromstring(rss_content)
            except ET.ParseError as e:
                logger.warning(f"1차 XML 파싱 실패: {e}")
                
                # 2차 시도: 인코딩 문제 해결 후 파싱
                try:
                    # 바이트로 변환하여 다른 인코딩 시도
                    if isinstance(rss_content, str):
                        # 이미 디코딩된 문자열인 경우, 바이트로 다시 인코딩 시도
                        for encoding in ['utf-8', 'cp949', 'euc-kr', 'iso-8859-1']:
                            try:
                                # 문자열을 바이트로 인코딩한 후 다시 디코딩
                                test_bytes = rss_content.encode(encoding, errors='ignore')
                                test_content = test_bytes.decode(encoding, errors='ignore')
                                root = ET.fromstring(test_content)
                                logger.info(f"인코딩 {encoding}으로 재파싱 성공")
                                break
                            except (ET.ParseError, UnicodeError):
                                continue
                    
                    if root is None:
                        # 3차 시도: HTML 엔티티 디코딩 후 파싱
                        import html
                        decoded_content = html.unescape(rss_content)
                        root = ET.fromstring(decoded_content)
                        logger.info("HTML 엔티티 디코딩 후 파싱 성공")
                        
                except ET.ParseError as e2:
                    logger.error(f"모든 XML 파싱 시도 실패: {e2}")
                    return articles
            
            if root is None:
                logger.error(f"RSS 피드 파싱 불가: {rss_url}")
                return articles
            
            # RSS 네임스페이스 처리
            namespaces = {
                'rss': 'http://purl.org/rss/1.0/',
                'atom': 'http://www.w3.org/2005/Atom',
                'dc': 'http://purl.org/dc/elements/1.1/',
                'content': 'http://purl.org/rss/1.0/modules/content/'
            }
            
            # item 태그 찾기 (RSS 2.0)
            items = root.findall('.//item')
            if not items:
                # RSS 1.0 또는 다른 형식 시도
                items = root.findall('.//rss:item', namespaces)
            
            if not items:
                logger.warning(f"RSS 피드에서 item을 찾을 수 없음: {rss_url}")
                return articles
            
            # 카테고리명 추출
            category_name = self._extract_category_from_url(rss_url)
            
            for item in items:
                try:
                    article = self._parse_rss_item(item, rss_url, category_name, namespaces)
                    if article:
                        articles.append(article)
                except Exception as e:
                    logger.warning(f"RSS item 파싱 실패: {e}")
                    continue
                    
        except ET.ParseError as e:
            logger.error(f"RSS XML 파싱 실패: {rss_url} - {e}")
        except Exception as e:
            logger.error(f"RSS 파싱 중 오류: {rss_url} - {e}")
            
        return articles

    def _parse_rss_item(self, item: ET.Element, rss_url: str, category_name: str, namespaces: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """개별 RSS item을 파싱하여 기사 정보 추출"""
        
        # 제목 추출 및 정리
        title_elem = item.find('title')
        if title_elem is None or not title_elem.text:
            return None
        title = self._clean_text(title_elem.text.strip())
        if not title:
            return None
        
        # 링크 추출
        link_elem = item.find('link')
        if link_elem is None or not link_elem.text:
            return None
        link = link_elem.text.strip()
        
        # 설명/내용 추출 및 정리
        description = ""
        desc_elem = item.find('description')
        if desc_elem is not None and desc_elem.text:
            description = self._clean_text(desc_elem.text.strip())
        
        # content:encoded 확인 (더 자세한 내용)
        content_elem = item.find('content:encoded', namespaces)
        if content_elem is not None and content_elem.text:
            content = content_elem.text.strip()
            # HTML 태그 제거 및 텍스트 정리
            soup = BeautifulSoup(content, 'html.parser')
            content = self._clean_text(soup.get_text(' ', strip=True))
            if content and len(content) > len(description):
                description = content
        
        # 발행일 추출
        pub_date = None
        date_elem = item.find('pubDate')
        if date_elem is not None and date_elem.text:
            try:
                # 다양한 날짜 형식 처리
                date_str = date_elem.text.strip()
                # RFC 822 형식 (예: "Wed, 02 Oct 2002 15:00:00 +0200")
                pub_date = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
            except ValueError:
                try:
                    # ISO 형식 시도
                    pub_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                except ValueError:
                    logger.warning(f"날짜 파싱 실패: {date_str}")
        
        # 작성자 추출 및 정리
        author = None
        author_elem = item.find('author') or item.find('dc:creator', namespaces)
        if author_elem is not None and author_elem.text:
            author = self._clean_text(author_elem.text.strip())
            # 이메일 제거 (예: "홍길동 <hong@example.com>")
            if '<' in author and '>' in author:
                author = author.split('<')[0].strip()
        
        # 필수 필드 검증
        if not title or not link:
            return None
        
        # 기사 URL이 상대 경로인 경우 절대 경로로 변환
        if not link.startswith('http'):
            parsed_rss = urlparse(rss_url)
            link = f"{parsed_rss.scheme}://{parsed_rss.netloc}{link}"
        
        article = {
            'title': title,
            'content': description,
            'url': link,
            'source': '조선일보',
            'author': author,
            'published_date': pub_date,
            'categories': [category_name] if category_name else [],
            'tags': [],
            'metadata': {
                'crawled_at': datetime.now().isoformat(),
                'rss_url': rss_url,
                'domain': urlparse(link).netloc,
                'rss_source': 'rss'
            },
        }
        
        return article

    def _clean_text(self, text: str) -> str:
        """텍스트 정리 및 인코딩 문제 해결"""
        if not text:
            return ''
        
        # HTML 엔티티 디코딩
        import html
        text = html.unescape(text)
        
        # 특수 문자 및 제어 문자 정리
        text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', text)
        
        # 연속된 공백 정리
        text = re.sub(r'\s+', ' ', text)
        
        # 앞뒤 공백 제거
        text = text.strip()
        
        # 인코딩 문제가 있는 문자 처리
        if '' in text or '?' in text:
            # 문제가 있는 문자를 제거하거나 대체
            text = text.replace('', '').replace('?', '')
        
        return text

    def _extract_category_from_url(self, rss_url: str) -> Optional[str]:
        """RSS URL에서 카테고리명 추출"""
        filename = os.path.basename(rss_url)
        return RSS_CATEGORY_MAP.get(filename)

    def crawl_rss_feed(self, rss_url: str, save_db: bool = False) -> List[Dict[str, Any]]:
        """단일 RSS 피드를 크롤링"""
        logger.info(f"RSS 피드 크롤링 시작: {rss_url}")
        
        resp = self._make_request(rss_url)
        if not resp:
            logger.error(f"RSS 피드 요청 실패: {rss_url}")
            return []
        
        # RSS 내용 파싱
        articles = self._parse_rss_feed(resp.text, rss_url)
        logger.info(f"RSS 피드에서 {len(articles)}개 기사 추출: {rss_url}")
        
        # DB 저장
        if save_db and db_manager and articles:
            saved_count = 0
            for article in articles:
                try:
                    if not getattr(db_manager, 'connection_pool', None):
                        db_manager.initialize_pool()
                        db_manager.create_tables()
                    
                    if db_manager.save_article(article):
                        saved_count += 1
                        logger.info(f"기사 저장 성공: {article['title'][:50]}...")
                    else:
                        logger.info(f"기사 이미 존재: {article['title'][:50]}...")
                        
                except Exception as e:
                    logger.error(f"기사 저장 실패: {article['title'][:50]}... - {e}")
            
            logger.info(f"RSS 피드 처리 완료: {rss_url} - {saved_count}/{len(articles)} 기사 저장")
        
        return articles

    def crawl_rss_feeds(self, rss_urls: Iterable[str], save_db: bool = False) -> List[Dict[str, Any]]:
        """여러 RSS 피드를 순차적으로 크롤링"""
        all_articles = []
        
        for i, rss_url in enumerate(rss_urls, 1):
            rss_url = rss_url.strip()
            if not rss_url:
                continue
                
            logger.info(f"[{i}] RSS 피드 크롤링: {rss_url}")
            articles = self.crawl_rss_feed(rss_url, save_db=save_db)
            all_articles.extend(articles)
            
            # 요청 간 대기
            if i < len(list(rss_urls)):
                time.sleep(self.request_delay)
        
        logger.info(f"전체 RSS 피드 크롤링 완료: {len(all_articles)}개 기사")
        return all_articles


def _load_rss_urls(args: argparse.Namespace) -> List[str]:
    """RSS URL 목록 로드"""
    urls: List[str] = []
    
    if args.rss_url:
        urls.extend(args.rss_url)
    
    if args.rss_file and os.path.exists(args.rss_file):
        with open(args.rss_file, 'r', encoding='utf-8') as f:
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
    parser = argparse.ArgumentParser(description='조선일보 RSS 피드 크롤러')
    parser.add_argument('--rss-url', action='append', help='크롤링할 RSS URL (여러 번 지정 가능)')
    parser.add_argument('--rss-file', type=str, help='RSS URL 목록 파일 경로(줄바꿈 구분)')
    parser.add_argument('--out', type=str, help='결과 저장 파일 경로(JSON Lines)')
    parser.add_argument('--save-db', action='store_true', help='DB에 저장(기본값: 저장)')
    parser.add_argument('--no-save-db', action='store_true', help='DB 저장 비활성화')
    parser.add_argument('--delay', type=float, default=0.8, help='요청 간 대기(초)')
    args = parser.parse_args()

    crawler = RSSArticleCrawler(request_delay=max(args.delay, 0.0))

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
                logger.info("DB 초기화 완료")
        except Exception as e:
            logger.error(f'DB 초기화 실패: {e}')
            return

    # RSS URL 목록 로드
    rss_urls = _load_rss_urls(args)
    
    # 입력된 URL이 없으면 기본 RSS URL 사용
    if not rss_urls:
        logger.info('입력된 RSS URL이 없어 기본 조선일보 RSS URL로 실행합니다.')
        rss_urls = DEFAULT_RSS_URLS

    # RSS 피드 크롤링 실행
    results = crawler.crawl_rss_feeds(rss_urls, save_db=save_to_db)

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