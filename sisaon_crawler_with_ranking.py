#!/usr/bin/env python3
"""
시사오늘 뉴스 섹션 크롤링 및 기자 순위 시스템
개선된 버전 - 안정성 및 성능 최적화
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from database_manager import db_manager
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sisaon_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SisaonCrawler:
    """시사오늘 뉴스 크롤러 (개선된 버전)"""
    
    def __init__(self, max_workers: int = 3):
        self.base_url = "http://www.sisaon.co.kr"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        
        # 뉴스 섹션 메인 카테고리
        self.categories = {
            '정치': 'S2N29',
            '경제': 'S2N30',
            '산업': 'S2N31',
            '건설·부동산': 'S2N32',
            'IT': 'S2N33',
            '유통·바이오': 'S2N34',
            '사회': 'S2N35',
            '자동차': 'S2N55'
        }
        
        # 크롤링 통계
        self.stats = {
            'total_articles': 0,
            'total_journalists': 0,
            'category_stats': {},
            'errors': 0,
            'duplicates': 0,
            'start_time': None,
            'end_time': None
        }
        
        # 성능 설정
        self.max_workers = max_workers
        self.request_delay = (1.0, 2.0)  # 랜덤 지연
        self.timeout = 15
        self.max_retries = 3
        
        # 세션 관리
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def _random_delay(self):
        """랜덤 지연으로 서버 부하 방지"""
        delay = random.uniform(*self.request_delay)
        time.sleep(delay)
    
    def _fix_encoding_issues(self, text: str) -> str:
        """인코딩 문제 해결 (한글 깨짐 수정) - 개선된 버전"""
        if not text:
            return text
        
        # 1. 일반적인 깨진 문자 패턴 수정
        encoding_fixes = {
            '': '가',  # 깨진 한글 자음
            '': '나',  # 깨진 한글 자음
            '': '다',  # 깨진 한글 자음
            '': '라',  # 깨진 한글 자음
            '': '마',  # 깨진 한글 자음
            '': '바',  # 깨진 한글 자음
            '': '사',  # 깨진 한글 자음
            '': '아',  # 깨진 한글 자음
            '': '자',  # 깨진 한글 자음
            '': '차',  # 깨진 한글 자음
            '': '카',  # 깨진 한글 자음
            '': '타',  # 깨진 한글 자음
            '': '파',  # 깨진 한글 자음
            '': '하',  # 깨진 한글 자음
            '': '기',  # 깨진 한글 자음
            '': '니',  # 깨진 한글 자음
            '': '디',  # 깨진 한글 자음
            '': '리',  # 깨진 한글 자음
            '': '미',  # 깨진 한글 자음
            '': '비',  # 깨진 한글 자음
            '': '시',  # 깨진 한글 자음
            '': '이',  # 깨진 한글 자음
            '': '지',  # 깨진 한글 자음
            '': '치',  # 깨진 한글 자음
            '': '키',  # 깨진 한글 자음
            '': '티',  # 깨진 한글 자음
            '': '피',  # 깨진 한글 자음
            '': '히',  # 깨진 한글 자음
        }
        
        # 2. 깨진 문자 교체
        for broken_char, fixed_char in encoding_fixes.items():
            text = text.replace(broken_char, fixed_char)
        
        # 3. 시사오늘 특화 깨진 문자 패턴 수정
        sisaon_fixes = {
            '히': '',  # 시사오늘에서 자주 보이는 패턴
            '히관히련히기히사히': '관련기사',
            '히정히치히': '정치',
            '히경히제': '경제',
            '히사히회': '사회',
            '히자히동히차': '자동차',
            '히유히통히바히오': '유통바이오',
            '히건히설히부히동히산': '건설부동산',
            '히산히업': '산업',
        }
        
        for broken_pattern, fixed_pattern in sisaon_fixes.items():
            text = text.replace(broken_pattern, fixed_pattern)
        
        # 4. 추가 인코딩 패턴 수정 (새로 추가)
        additional_fixes = {
            'м мнҳё': '기자명',  # 깨진 기자명 패턴
            'кҙҖл ЁкёмӮ': '기사제목',  # 깨진 제목 패턴
            'лм ңмқҖ': '기자명',  # 깨진 기자명 패턴
        }
        
        for broken_pattern, fixed_pattern in additional_fixes.items():
            text = text.replace(broken_pattern, fixed_pattern)
        
        # 5. 연속된 깨진 문자 제거 (더 강화된 버전)
        text = re.sub(r'[^\w\s가-힣.,!?()[\]{}"\'-]', '', text)
        
        # 6. 과도한 공백 정리
        text = re.sub(r'\s+', ' ', text)
        
        # 7. 빈 문자열이나 의미없는 텍스트 제거
        if len(text.strip()) < 3:
            return ""
        
        # 8. 한글이 전혀 없는 경우 필터링
        if not re.search(r'[가-힣]', text):
            return ""
        
        return text.strip()
    
    def _make_request(self, url: str, retries: int = None) -> Optional[requests.Response]:
        """안정적인 HTTP 요청 (인코딩 강화 버전)"""
        if retries is None:
            retries = self.max_retries
        
        for attempt in range(retries + 1):
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                
                # 강화된 인코딩 처리
                try:
                    # 1. 응답 헤더에서 인코딩 확인
                    if 'charset' in response.headers.get('content-type', '').lower():
                        content_type = response.headers['content-type'].lower()
                        if 'charset=utf-8' in content_type:
                            response.encoding = 'utf-8'
                        elif 'charset=euc-kr' in content_type:
                            response.encoding = 'euc-kr'
                        elif 'charset=cp949' in content_type:
                            response.encoding = 'cp949'
                    
                    # 2. HTML 메타 태그에서 인코딩 확인
                    if response.encoding in ['ISO-8859-1', 'ascii']:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        meta_charset = soup.find('meta', charset=True)
                        if meta_charset:
                            response.encoding = meta_charset['charset']
                        else:
                            meta_content = soup.find('meta', attrs={'http-equiv': 'Content-Type'})
                            if meta_content and 'charset=' in meta_content.get('content', ''):
                                charset = meta_content['content'].split('charset=')[-1].strip()
                                response.encoding = charset
                    
                    # 3. 일반적인 한글 사이트 인코딩 처리
                    if response.encoding in ['ISO-8859-1', 'ascii', 'cp949']:
                        # 한글이 포함되어 있는지 확인
                        content_text = response.text
                        if any(ord(char) > 127 for char in content_text[:1000]):
                            # 한글이 있으면 UTF-8로 강제 설정
                            response.encoding = 'utf-8'
                    
                    # 4. 최종 검증 - 한글 깨짐 확인
                    test_text = response.text[:200]
                    if '' in test_text or '?' in test_text:
                        # 깨진 문자가 있으면 다른 인코딩 시도
                        for encoding in ['utf-8', 'euc-kr', 'cp949']:
                            try:
                                test_content = response.content.decode(encoding)
                                if '' not in test_content[:200] and '?' not in test_content[:200]:
                                    response.encoding = encoding
                                    break
                            except UnicodeDecodeError:
                                continue
                    
                    # 5. 시사오늘 사이트 특화 인코딩 처리
                    if 'sisaon.co.kr' in url:
                        # 시사오늘은 보통 UTF-8을 사용하지만 가끔 깨짐
                        if response.encoding not in ['utf-8', 'euc-kr']:
                            response.encoding = 'utf-8'
                
                except Exception as encoding_error:
                    logger.warning(f"인코딩 처리 중 오류: {encoding_error}")
                    # 기본값으로 UTF-8 설정
                    response.encoding = 'utf-8'
                
                return response
            except requests.exceptions.RequestException as e:
                if attempt == retries:
                    logger.error(f"요청 실패 (최종): {url} - {e}")
                    return None
                else:
                    logger.warning(f"요청 실패 (재시도 {attempt + 1}/{retries}): {url} - {e}")
                    time.sleep(2 ** attempt)  # 지수 백오프
        
        return None
    
    def get_article_links_from_page(self, category_url: str) -> List[str]:
        """페이지에서 기사 링크 추출 (시사오늘 특화 개선 버전)"""
        try:
            response = self._make_request(category_url)
            if not response:
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 시사오늘 특화 기사 링크 패턴
            link_patterns = [
                r'/news/articleView\.html\?idxno=\d+',
                r'/news/articleView\.html\?idxno=\d+&.*',
                r'/articleView\.html\?idxno=\d+',
                r'/news/view\.html\?idxno=\d+',
                r'/article/view\.html\?idxno=\d+'
            ]
            
            article_links = []
            seen_links = set()
            
            # 방법 1: 정규식 패턴으로 링크 찾기
            for pattern in link_patterns:
                links = soup.find_all('a', href=re.compile(pattern))
                for link in links:
                    href = link.get('href')
                    if href and href not in seen_links:
                        # URL 정규화
                        if href.startswith('/'):
                            full_url = self.base_url + href
                        elif href.startswith('http'):
                            full_url = href
                        else:
                            continue
                        
                        # URL 파라미터 정리 (idxno만 유지)
                        if '?' in full_url:
                            base_url = full_url.split('?')[0]
                            params = full_url.split('?')[1]
                            idxno_match = re.search(r'idxno=(\d+)', params)
                            if idxno_match:
                                full_url = f"{base_url}?idxno={idxno_match.group(1)}"
                        
                        article_links.append(full_url)
                        seen_links.add(href)
            
            # 방법 2: 시사오늘 특화 클래스로 링크 찾기
            specific_selectors = [
                '.article-list a',
                '.news-list a', 
                '.list-article a',
                '.article-item a',
                '.news-item a'
            ]
            
            for selector in specific_selectors:
                links = soup.select(selector)
                for link in links:
                    href = link.get('href')
                    if href and href not in seen_links:
                        # 기사 링크인지 확인
                        if any(pattern in href for pattern in ['articleView', 'view.html']):
                            if href.startswith('/'):
                                full_url = self.base_url + href
                            elif href.startswith('http'):
                                full_url = href
                            else:
                                continue
                            
                            # URL 파라미터 정리
                            if '?' in full_url:
                                base_url = full_url.split('?')[0]
                                params = full_url.split('?')[1]
                                idxno_match = re.search(r'idxno=(\d+)', params)
                                if idxno_match:
                                    full_url = f"{base_url}?idxno={idxno_match.group(1)}"
                            
                            article_links.append(full_url)
                            seen_links.add(href)
            
            # 중복 제거 및 정렬
            unique_links = list(set(article_links))
            unique_links.sort()
            
            logger.info(f"페이지에서 {len(unique_links)}개 기사 링크 발견")
            return unique_links
            
        except Exception as e:
            logger.error(f"페이지 링크 추출 실패: {e}")
            return []
    
    def get_total_pages(self, category_url: str) -> int:
        """카테고리의 총 페이지 수 확인 (개선된 버전)"""
        try:
            response = self._make_request(category_url)
            if not response:
                return 1
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 방법 1: 페이지네이션 링크에서 찾기
            pagination_links = soup.find_all('a', href=re.compile(r'page=\d+'))
            
            if pagination_links:
                page_numbers = []
                for link in pagination_links:
                    href = link.get('href', '')
                    page_match = re.search(r'page=(\d+)', href)
                    if page_match:
                        page_numbers.append(int(page_match.group(1)))
                
                if page_numbers:
                    max_page = max(page_numbers)
                    logger.info(f"페이지네이션에서 총 {max_page}페이지 발견")
                    return max_page
            
            # 방법 2: 페이지 번호 텍스트에서 찾기
            page_texts = soup.find_all(text=re.compile(r'\d+'))
            for text in page_texts:
                if '페이지' in text or 'page' in text.lower():
                    numbers = re.findall(r'\d+', text)
                    if numbers:
                        max_page = max(int(num) for num in numbers)
                        logger.info(f"텍스트에서 총 {max_page}페이지 발견")
                        return max_page
            
            # 방법 3: "다음" 버튼이나 "마지막" 버튼 찾기
            next_links = soup.find_all('a', string=re.compile(r'다음|마지막|>>|>'))
            if next_links:
                for link in next_links:
                    href = link.get('href', '')
                    page_match = re.search(r'page=(\d+)', href)
                    if page_match:
                        max_page = int(page_match.group(1))
                        logger.info(f"다음/마지막 버튼에서 총 {max_page}페이지 발견")
                        return max_page
            
            logger.info("페이지 수를 확인할 수 없어 1페이지로 설정")
            return 1
                
        except Exception as e:
            logger.error(f"페이지 수 확인 실패: {e}")
            return 1
    
    def extract_article_data(self, article_url: str, category: str) -> Optional[Dict[str, Any]]:
        """기사 페이지에서 데이터 추출 (개선된 버전)"""
        try:
            response = self._make_request(article_url)
            if not response:
                return None
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # 제목 추출 (개선된 방법)
            title = self._extract_title(soup)
            if not title:
                logger.warning(f"제목 추출 실패: {article_url}")
                return None
            
            # 기자 정보 추출 (개선된 방법)
            author = self._extract_author(soup)
            if not author:
                logger.warning(f"기자 정보 추출 실패: {article_url}")
                return None
            
            # 본문 추출 (개선된 방법)
            content = self._extract_content(soup)
            if not content:
                logger.warning(f"본문 추출 실패: {article_url}")
                return None
            
            # 발행일 추출 (개선된 방법)
            published_date = self._extract_published_date(soup)
            
            return {
                'title': title,
                'content': content,
                'url': article_url,
                'source': '시사오늘',
                'author': author,
                'published_date': published_date,
                'categories': [category],
                'tags': [],
                'metadata': {
                    'crawled_at': datetime.now().isoformat(),
                    'category': category,
                    'word_count': len(content.split())
                }
            }
            
        except Exception as e:
            logger.error(f"기사 데이터 추출 실패 {article_url}: {e}")
            return None
    
    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        """제목 추출 (시사오늘 특화 강화 버전)"""
        # 시사오늘 특화 제목 선택자 (우선순위 순)
        title_selectors = [
            '.aht-title',
            '.article-head-title', 
            '.aht-title-view',
            '.article-title',
            '.news-title',
            '.title',
            'h1',
            'h2',
            'h3',
            '[class*="title"]',
            '[class*="headline"]'
        ]
        
        for selector in title_selectors:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = title_elem.get_text().strip()
                
                # 인코딩 문제 해결
                title = self._fix_encoding_issues(title)
                
                # 제목 유효성 검사
                if (title and 
                    len(title) > 5 and 
                    len(title) < 200 and  # 너무 긴 제목 제외
                    '[' not in title and  # 광고성 제목 제외
                    'http' not in title.lower() and  # URL 제외
                    not title.startswith('광고') and  # 광고 제외
                    not title.startswith('PR') and   # PR 제외
                    not re.match(r'^\d+$', title)):  # 숫자만 있는 제목 제외
                    return title
        
        # 방법 2: 메타 태그에서 제목 찾기
        meta_title = soup.find('meta', property='og:title')
        if meta_title:
            title = meta_title.get('content', '').strip()
            if title and len(title) > 5 and len(title) < 200:
                return title
        
        # 방법 3: title 태그에서 찾기
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text().strip()
            if title and len(title) > 5 and len(title) < 200:
                # 불필요한 접미사 제거
                title = re.sub(r'\s*[-|]\s*시사오늘.*$', '', title)
                title = re.sub(r'\s*[-|]\s*시사ON.*$', '', title)
                if len(title) > 5:
                    return title
        
        # 방법 4: 더 넓은 범위에서 제목 찾기
        for elem in soup.find_all(['h1', 'h2', 'h3', 'h4']):
            title = elem.get_text().strip()
            if (title and 
                len(title) > 5 and 
                len(title) < 200 and
                not title.startswith('광고') and
                not title.startswith('PR')):
                return title
        
        return None
    
    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """기자 정보 추출 (시사오늘 특화 개선 버전)"""
        # 방법 1: 시사오늘 특화 - 기자 프로필 섹션
        profile_selectors = [
            '#wrProfile .name strong',
            '.writer-info .name',
            '.author-info .name',
            '.reporter-info .name'
        ]
        
        for selector in profile_selectors:
            profile_name = soup.select_one(selector)
            if profile_name:
                author = profile_name.get_text().strip()
                # 인코딩 문제 해결
                author = self._fix_encoding_issues(author)
                if author and len(author) <= 10 and '기자' not in author:
                    return author
        
        # 방법 2: 메타 태그에서 추출
        meta_author = soup.find('meta', property='og:article:author')
        if meta_author:
            author = meta_author.get('content', '').strip()
            if author and '기자' in author:
                return author.split('기자')[0].strip()
        
        # 방법 3: twitter creator 메타 태그
        twitter_author = soup.find('meta', attrs={'name': 'twitter:creator'})
        if twitter_author:
            author = twitter_author.get('content', '').strip()
            if author and '기자' in author:
                return author.split('기자')[0].strip()
        
        # 방법 4: dable author 메타 태그
        dable_author = soup.find('meta', property='dable:author')
        if dable_author:
            author = dable_author.get('content', '').strip()
            if author and '기자' in author:
                return author.split('기자')[0].strip()
        
        # 방법 5: 기자 정보 섹션에서 추출 (시사오늘 특화)
        info_selectors = [
            '.info-text li',
            '.article-info .reporter',
            '.article-meta .author',
            '.byline'
        ]
        
        for selector in info_selectors:
            info_text = soup.select_one(selector)
            if info_text:
                info_content = info_text.get_text().strip()
                if '기자' in info_content:
                    # 다양한 패턴 매칭
                    patterns = [
                        r'([가-힣]{2,4})\s*기자',
                        r'기자\s*([가-힣]{2,4})',
                        r'([가-힣]{2,4})\s*기자\s*[가-힣]*',
                        r'=\s*([가-힣]+)\s*기자'
                    ]
                    
                    for pattern in patterns:
                        author_match = re.search(pattern, info_content)
                        if author_match:
                            author = author_match.group(1).strip()
                            if len(author) >= 2 and len(author) <= 4:
                                return author
        
        # 방법 6: 본문 첫 부분에서 추출 (시사오늘 특화)
        p_elements = soup.find_all('p')
        for p_elem in p_elements[:5]:  # 처음 5개 문단만 확인
            p_text = p_elem.get_text().strip()
            if '기자' in p_text and ('=' in p_text or '·' in p_text):
                # 시사오늘 특화 패턴
                patterns = [
                    r'=\s*([가-힣]+)\s*기자',
                    r'·\s*([가-힣]+)\s*기자',
                    r'([가-힣]{2,4})\s*기자',
                    r'기자\s*([가-힣]{2,4})'
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, p_text)
                    if match:
                        author = match.group(1).strip()
                        if len(author) >= 2 and len(author) <= 4:
                            return author
        
        # 방법 7: 제목 근처에서 기자 정보 찾기
        title_area = soup.find('h1') or soup.find('h2') or soup.find('.title')
        if title_area:
            title_parent = title_area.parent
            if title_parent:
                for elem in title_parent.find_all(['span', 'div', 'p']):
                    text = elem.get_text().strip()
                    if '기자' in text:
                        author_match = re.search(r'([가-힣]{2,4})\s*기자', text)
                        if author_match:
                            return author_match.group(1).strip()
        
        return None
    
    def _extract_content(self, soup: BeautifulSoup) -> Optional[str]:
        """본문 추출 (시사오늘 특화 개선 버전)"""
        # 시사오늘 특화 콘텐츠 선택자
        content_selectors = [
            'article.article-veiw-body',
            '.user-content',
            '.article-content',
            '.content-body',
            '.article-body',
            '.news-content',
            '.content',
            '.body',
            'article',
            '.article'
        ]
        
        for selector in content_selectors:
            content_elem = soup.select_one(selector)
            if content_elem:
                # 불필요한 요소 제거
                unwanted_selectors = [
                    '.related-articles', '.comments', '.advertisement', 
                    '.ad', '.banner', '.social-share', '.article-footer',
                    'script', 'style', '.recommend', '.news-recommend',
                    '.article-recommend', '.related-news', '.more-news'
                ]
                
                for unwanted_selector in unwanted_selectors:
                    for unwanted in content_elem.select(unwanted_selector):
                        unwanted.decompose()
                
                # 기자 정보가 포함된 첫 문단 제거
                p_elements = content_elem.find_all('p')
                if p_elements:
                    first_p = p_elements[0]
                    first_text = first_p.get_text().strip()
                    if '기자' in first_text and ('=' in first_text or '·' in first_text):
                        first_p.decompose()
                
                content_text = content_elem.get_text().strip()
                if content_text and len(content_text) > 100:
                    # 인코딩 문제 해결
                    content_text = self._fix_encoding_issues(content_text)
                    # 관련기사나 댓글 부분 제거
                    if '관련기사' not in content_text and '댓글' not in content_text:
                        # 불필요한 공백 정리
                        content_text = re.sub(r'\s+', ' ', content_text)
                        content_text = re.sub(r'\n\s*\n', '\n', content_text)
                        return content_text.strip()
        
        return None
    
    def _extract_published_date(self, soup: BeautifulSoup) -> datetime:
        """발행일 추출 (개선된 방법)"""
        try:
            # 메타 태그에서 날짜 찾기
            date_selectors = [
                'meta[property="og:published_time"]',
                'meta[name="publish_date"]',
                'meta[name="article:published_time"]'
            ]
            
            for selector in date_selectors:
                date_elem = soup.select_one(selector)
                if date_elem:
                    date_str = date_elem.get('content', '')
                    if date_str:
                        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            
            # 현재 시간으로 대체
            return datetime.now()
            
        except Exception:
            return datetime.now()
    
    def crawl_category(self, category: str, category_code: str, max_pages: int = None) -> int:
        """특정 카테고리 크롤링 (시사오늘 특화 개선 버전)"""
        logger.info(f"카테고리 '{category}' 크롤링 시작...")
        
        category_url = f"{self.base_url}/news/articleList.html?sc_sub_section_code={category_code}&view_type=sm"
        
        # 총 페이지 수 확인
        total_pages = self.get_total_pages(category_url)
        if max_pages:
            total_pages = min(total_pages, max_pages)
        
        logger.info(f"카테고리 '{category}': 총 {total_pages}페이지 크롤링 예정")
        
        category_articles = 0
        
        for page in range(1, total_pages + 1):
            try:
                page_url = f"{category_url}&page={page}"
                logger.info(f"페이지 {page}/{total_pages} 크롤링 중... ({page/total_pages*100:.1f}%)")
                
                # 페이지에서 기사 링크 추출
                article_links = self.get_article_links_from_page(page_url)
                
                if not article_links:
                    logger.warning(f"페이지 {page}에서 기사 링크를 찾을 수 없습니다.")
                    continue
                
                logger.info(f"페이지 {page}에서 {len(article_links)}개 기사 링크 발견")
                
                # 병렬 처리로 기사 크롤링 (안정성 개선)
                successful_articles = 0
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # 작업 제출
                    future_to_url = {
                        executor.submit(self._process_article, article_url, category): article_url 
                        for article_url in article_links
                    }
                    
                    # 결과 수집
                    for future in as_completed(future_to_url):
                        article_url = future_to_url[future]
                        try:
                            result = future.result(timeout=60)  # 60초 타임아웃
                            if result:
                                successful_articles += 1
                                category_articles += 1
                                self.stats['total_articles'] += 1
                        except Exception as e:
                            logger.error(f"기사 처리 실패 {article_url}: {e}")
                            self.stats['errors'] += 1
                
                logger.info(f"페이지 {page} 완료: {successful_articles}/{len(article_links)} 기사 성공")
                
                # 페이지 간격 조절
                self._random_delay()
                
            except Exception as e:
                logger.error(f"페이지 {page} 크롤링 실패: {e}")
                self.stats['errors'] += 1
                continue
        
        self.stats['category_stats'][category] = category_articles
        logger.info(f"카테고리 '{category}' 크롤링 완료: {category_articles}개 기사")
        
        return category_articles
    
    def _process_article(self, article_url: str, category: str) -> bool:
        """개별 기사 처리 (journalists 테이블에만 저장)"""
        try:
            # 기사 데이터 추출 (재시도 포함)
            article_data = None
            for retry in range(3):
                try:
                    article_data = self.extract_article_data(article_url, category)
                    if article_data:
                        break
                    time.sleep(2)  # 재시도 전 대기
                except Exception as retry_e:
                    logger.warning(f"기사 추출 재시도 {retry+1}/3: {retry_e}")
                    if retry < 2:
                        time.sleep(3)
            
            if article_data and article_data.get('author'):
                # journalists 테이블에 기자 통계와 기사 내용 업데이트
                saved = False
                if hasattr(db_manager, 'connection_pool') and db_manager.connection_pool:
                    try:
                        if db_manager.update_journalist_stats(
                            journalist_name=article_data['author'],
                            category=category,
                            increment=1,
                            article_data=article_data
                        ):
                            saved = True
                    except Exception as e:
                        logger.warning(f"기자 통계 업데이트 실패: {e}")
                
                if saved:
                    logger.info(f"기자 통계 및 기사 내용 저장 완료: {article_data['title'][:50]}... (기자: {article_data.get('author', 'N/A')})")
                else:
                    logger.info(f"기사 크롤링 완료: {article_data['title'][:50]}... (기자: {article_data.get('author', 'N/A')}) - 저장 안됨")
                
                return True
            else:
                if not article_data:
                    self.stats['errors'] += 1
                    logger.warning(f"기사 데이터 추출 실패: {article_url}")
                else:
                    logger.warning(f"기자 정보 없음: {article_url}")
                return False
                
        except Exception as e:
            logger.error(f"기사 처리 실패 {article_url}: {e}")
            self.stats['errors'] += 1
            return False
    
    def crawl_all_categories(self, max_pages_per_category: int = None):
        """모든 카테고리 크롤링 (개선된 버전)"""
        logger.info("전체 카테고리 크롤링 시작...")
        
        self.stats['start_time'] = datetime.now()
        
        for category, code in self.categories.items():
            try:
                self.crawl_category(category, code, max_pages_per_category)
                
                # 카테고리 간격 조절
                time.sleep(3)
                
            except Exception as e:
                logger.error(f"카테고리 '{category}' 크롤링 실패: {e}")
                self.stats['errors'] += 1
                continue
        
        self.stats['end_time'] = datetime.now()
        self._print_final_stats()
    
    def _print_final_stats(self):
        """최종 통계 출력"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        logger.info(f"전체 크롤링 완료!")
        logger.info(f"총 기사 수: {self.stats['total_articles']}개")
        logger.info(f"중복 기사 수: {self.stats['duplicates']}개")
        logger.info(f"총 오류 수: {self.stats['errors']}개")
        logger.info(f"소요 시간: {duration:.2f}초")
        
        # 카테고리별 통계 출력
        print("\n📊 카테고리별 크롤링 결과:")
        print("-" * 50)
        for category, count in self.stats['category_stats'].items():
            print(f"{category:<15}: {count:4d}개 기사")
        
        # 성공률 계산
        total_attempts = self.stats['total_articles'] + self.stats['errors'] + self.stats['duplicates']
        if total_attempts > 0:
            success_rate = (self.stats['total_articles'] / total_attempts) * 100
            print(f"\n✅ 성공률: {success_rate:.1f}%")
        
        print(f"⏱️  평균 처리 속도: {self.stats['total_articles']/duration*60:.1f}개/분")
        
        # 크롤링 로그 저장
        if hasattr(db_manager, 'connection_pool') and db_manager.connection_pool:
            db_manager.log_crawling_job(
                job_type='sisaon_news_crawling',
                source_key='시사오늘',
                status='completed',
                articles_count=self.stats['total_articles'],
                duration_seconds=duration,
                error_message=f"오류 {self.stats['errors']}개, 중복 {self.stats['duplicates']}개" if self.stats['errors'] > 0 or self.stats['duplicates'] > 0 else None
            )

class JournalistRankingSystem:
    """기자 순위 시스템 (개선된 버전)"""
    
    def __init__(self):
        self.categories = {
            '정치': 'S2N29',
            '경제': 'S2N30',
            '산업': 'S2N31',
            '건설·부동산': 'S2N32',
            'IT': 'S2N33',
            '유통·바이오': 'S2N34',
            '사회': 'S2N35',
            '자동차': 'S2N55'
        }
        
        # 통계 캐시
        self._stats_cache = {}
        self._cache_expiry = {}
        self.cache_duration = 300  # 5분
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """캐시 유효성 확인"""
        if cache_key not in self._cache_expiry:
            return False
        return datetime.now() < self._cache_expiry[cache_key]
    
    def _set_cache(self, cache_key: str, data: Any):
        """캐시 설정"""
        self._stats_cache[cache_key] = data
        self._cache_expiry[cache_key] = datetime.now() + timedelta(seconds=self.cache_duration)
    
    def _get_cache(self, cache_key: str) -> Any:
        """캐시 조회"""
        if self._is_cache_valid(cache_key):
            return self._stats_cache.get(cache_key)
        return None
    
    def generate_journalist_stats(self, force_refresh: bool = False) -> bool:
        """기자 통계 생성 (journalists 테이블 기반)"""
        logger.info("기자 통계 생성을 시작합니다...")
        
        cache_key = "journalist_stats"
        if not force_refresh:
            cached_data = self._get_cache(cache_key)
            if cached_data:
                logger.info("캐시된 기자 통계를 사용합니다.")
                return True
        
        try:
            # journalists 테이블에서 기자 통계 조회
            all_journalists = db_manager.get_all_journalists(source='시사오늘', limit=1000)
            
            if all_journalists:
                # 기자별 총 기사 수 및 카테고리 분포 계산
                journalist_totals = {}
                journalist_categories = {}
                
                for journalist in all_journalists:
                    name = journalist['name']
                    total_articles = journalist['total_articles']
                    categories = journalist.get('categories', [])
                    
                    # 총 기사 수 저장
                    journalist_totals[name] = total_articles
                    
                    # 카테고리별 분포는 news_articles에서 계산
                    journalist_stats = db_manager.get_journalist_stats_by_journalist(name)
                    if journalist_stats:
                        journalist_categories[name] = {
                            stat['category']: stat['article_count'] 
                            for stat in journalist_stats
                        }
                    else:
                        journalist_categories[name] = {}
                
                # 총 기사 수로 정렬
                sorted_journalists = sorted(journalist_totals.items(), key=lambda x: x[1], reverse=True)
                
                # 캐시에 저장
                stats_data = {
                    'journalist_totals': journalist_totals,
                    'journalist_categories': journalist_categories,
                    'sorted_journalists': sorted_journalists,
                    'total_journalists': len(sorted_journalists),
                    'total_articles': sum(journalist_totals.values())
                }
                self._set_cache(cache_key, stats_data)
                
                logger.info(f"기자 통계 생성 완료:")
                logger.info(f"  - 총 기자 수: {len(sorted_journalists)}명")
                logger.info(f"  - 총 기사 수: {sum(journalist_totals.values())}개")
                
                # 기자별 상세 정보 출력
                self._print_journalist_summary(sorted_journalists)
                
                return True
            else:
                logger.warning("시사오늘 기자 데이터가 없습니다.")
                return False
                
        except Exception as e:
            logger.error(f"기자 통계 생성 실패: {e}")
            return False
    
    def _print_journalist_summary(self, sorted_journalists: List[Tuple[str, int]]):
        """기자 요약 정보 출력"""
        print(f"\n📊 시사오늘 기자 현황:")
        print("-" * 60)
        print(f"{'순위':<4} {'기자명':<15} {'총 기사수':<10} {'주요 카테고리':<20}")
        print("-" * 60)
        
        for i, (name, total) in enumerate(sorted_journalists[:15], 1):  # 상위 15명 출력
            # 주요 카테고리 찾기
            categories = self._get_cache("journalist_stats")
            if categories and 'journalist_categories' in categories:
                journalist_cats = categories['journalist_categories'].get(name, {})
                if journalist_cats:
                    top_category = max(journalist_cats.items(), key=lambda x: x[1])
                    main_category = f"{top_category[0]}({top_category[1]})"
                else:
                    main_category = "N/A"
            else:
                main_category = "N/A"
            
            # 상위 3명은 특별 표시
            if i == 1:
                print(f"🥇 {i:<2} {name:<15} {total:<10} {main_category}")
            elif i == 2:
                print(f"🥈 {i:<2} {name:<15} {total:<10} {main_category}")
            elif i == 3:
                print(f"🥉 {i:<2} {name:<15} {total:<10} {main_category}")
            else:
                print(f"   {i:<2} {name:<15} {total:<10} {main_category}")
        
        if len(sorted_journalists) > 15:
            print(f"... 외 {len(sorted_journalists) - 15}명")
    
    def get_journalist_rankings_by_category(self, category: str, limit: int = 20) -> list:
        """특정 카테고리의 기자 순위 조회 (journalists 테이블 기반)"""
        cache_key = f"category_rankings_{category}_{limit}"
        cached_data = self._get_cache(cache_key)
        if cached_data:
            return cached_data
        
        try:
            # journalists 테이블에서 해당 카테고리에 기사를 쓴 기자들 조회
            all_journalists = db_manager.get_all_journalists(source='시사오늘', limit=1000)
            
            # 해당 카테고리에 기사를 쓴 기자들 필터링
            category_journalists = []
            for journalist in all_journalists:
                categories = journalist.get('categories', [])
                if category in categories:
                    category_journalists.append({
                        'journalist_name': journalist['name'],
                        'article_count': journalist['total_articles'],
                        'category': category,
                        'updated_at': journalist['updated_at'],
                        'total_articles': journalist['total_articles'],
                        'category_count': len(categories)
                    })
            
            # 기사 수로 정렬
            category_journalists.sort(key=lambda x: x['article_count'], reverse=True)
            
            # 순위 추가
            rankings = []
            for i, ranking in enumerate(category_journalists[:limit], 1):
                ranking['rank'] = i
                rankings.append(ranking)
            
            self._set_cache(cache_key, rankings)
            return rankings
            
        except Exception as e:
            logger.error(f"{category} 카테고리 순위 조회 실패: {e}")
            return []
    
    def print_category_rankings(self, category: str, limit: int = 10):
        """카테고리별 순위 출력 (개선된 버전)"""
        try:
            category_rankings = self.get_journalist_rankings_by_category(category, limit)
            
            if category_rankings:
                print(f"\n🏆 {category} 카테고리 기자 순위 (상위 {len(category_rankings)}명)")
                print("=" * 80)
                print(f"{'순위':<4} {'기자명':<15} {'기사수':<8} {'전체기사':<10} {'카테고리수':<10} {'마지막 업데이트':<20}")
                print("-" * 80)
                
                for ranking in category_rankings:
                    name = ranking['journalist_name'][:14]
                    count = ranking['article_count']
                    total = ranking.get('total_articles', 0)
                    cat_count = ranking.get('category_count', 0)
                    updated = ranking.get('updated_at', 'N/A')
                    if updated != 'N/A':
                        updated = updated.strftime('%Y-%m-%d %H:%M')
                    
                    rank = ranking['rank']
                    # 상위 3명은 특별 표시
                    if rank == 1:
                        print(f"🥇 {rank:<2} {name:<15} {count:<8} {total:<10} {cat_count:<10} {updated}")
                    elif rank == 2:
                        print(f"🥈 {rank:<2} {name:<15} {count:<8} {total:<10} {cat_count:<10} {updated}")
                    elif rank == 3:
                        print(f"🥉 {rank:<2} {name:<15} {count:<8} {total:<10} {cat_count:<10} {updated}")
                    else:
                        print(f"   {rank:<2} {name:<15} {count:<8} {total:<10} {cat_count:<10} {updated}")
            else:
                print(f"\n❌ {category} 카테고리에 기자 데이터가 없습니다.")
                
        except Exception as e:
            logger.error(f"{category} 순위 출력 실패: {e}")
    
    def print_all_rankings(self, limit: int = 10):
        """모든 카테고리 순위 출력 (개선된 버전)"""
        try:
            print("\n" + "="*100)
            print("📊 전체 카테고리 기자 순위 현황")
            print("="*100)
            
            # 전체 순위 조회 (journalists 테이블 기반)
            all_journalists = db_manager.get_all_journalists(source='시사오늘', limit=limit)
            if all_journalists:
                print("\n🏆 전체 기사 수 기준 상위 기자")
                print("-" * 90)
                print(f"{'순위':<4} {'기자명':<20} {'총 기사수':<10} {'카테고리 수':<12} {'주요 카테고리':<20} {'마지막 업데이트':<20}")
                print("-" * 90)
                
                for i, journalist in enumerate(all_journalists[:10], 1):
                    name = journalist['name'][:19]
                    total = journalist['total_articles']
                    categories = len(journalist.get('categories', []))
                    
                    # 주요 카테고리 찾기
                    categories_list = journalist.get('categories', [])
                    if categories_list:
                        main_category = f"{categories_list[0]}({total})"
                    else:
                        main_category = "N/A"
                    
                    updated = journalist.get('updated_at', 'N/A')
                    if updated != 'N/A':
                        updated = updated.strftime('%Y-%m-%d %H:%M')
                    
                    # 상위 3명은 특별 표시
                    if i == 1:
                        print(f"🥇 {i:<2} {name:<20} {total:<10} {categories:<12} {main_category:<20} {updated}")
                    elif i == 2:
                        print(f"🥈 {i:<2} {name:<20} {total:<10} {categories:<12} {main_category:<20} {updated}")
                    elif i == 3:
                        print(f"🥉 {i:<2} {name:<20} {total:<10} {categories:<12} {main_category:<20} {updated}")
                    else:
                        print(f"   {i:<2} {name:<20} {total:<10} {categories:<12} {main_category:<20} {updated}")
            
            # 카테고리별 순위 요약
            print(f"\n📰 카테고리별 TOP 3 요약:")
            print("-" * 80)
            for category in self.categories.keys():
                category_rankings = self.get_journalist_rankings_by_category(category, limit=3)
                
                if category_rankings:
                    top_3 = []
                    for ranking in category_rankings[:3]:
                        name = ranking['journalist_name'][:12]
                        count = ranking['article_count']
                        rank = ranking['rank']
                        
                        if rank == 1:
                            top_3.append(f"🥇{name}({count})")
                        elif rank == 2:
                            top_3.append(f"🥈{name}({count})")
                        elif rank == 3:
                            top_3.append(f"🥉{name}({count})")
                    
                    print(f"{category:<15}: {' | '.join(top_3)}")
                else:
                    print(f"{category:<15}: 데이터 없음")
                    
        except Exception as e:
            logger.error(f"전체 순위 출력 실패: {e}")
    
    def analyze_journalist_trends(self, days: int = 7) -> Dict[str, Any]:
        """기자 활동 트렌드 분석"""
        try:
            logger.info(f"최근 {days}일간 기자 활동 트렌드 분석 중...")
            
            # 최근 기사 데이터 조회
            recent_articles = db_manager.get_articles(
                limit=1000,
                source='시사오늘'
            )
            
            # 날짜별 기자 활동 분석
            journalist_activity = {}
            cutoff_date = datetime.now() - timedelta(days=days)
            
            for article in recent_articles:
                if not article.get('author') or not article.get('published_date'):
                    continue
                
                published_date = article['published_date']
                if isinstance(published_date, str):
                    try:
                        published_date = datetime.fromisoformat(published_date.replace('Z', '+00:00'))
                    except:
                        continue
                
                if published_date < cutoff_date:
                    continue
                
                author = article['author']
                date_key = published_date.strftime('%Y-%m-%d')
                
                if author not in journalist_activity:
                    journalist_activity[author] = {}
                
                if date_key not in journalist_activity[author]:
                    journalist_activity[author][date_key] = 0
                
                journalist_activity[author][date_key] += 1
            
            # 가장 활발한 기자들 찾기
            total_activity = {author: sum(days.values()) for author, days in journalist_activity.items()}
            top_active = sorted(total_activity.items(), key=lambda x: x[1], reverse=True)[:10]
            
            return {
                'journalist_activity': journalist_activity,
                'top_active_journalists': top_active,
                'analysis_period': f"{days}일",
                'total_journalists_analyzed': len(journalist_activity)
            }
            
        except Exception as e:
            logger.error(f"기자 트렌드 분석 실패: {e}")
            return {}
    
    def print_trend_analysis(self, days: int = 7):
        """트렌드 분석 결과 출력"""
        trends = self.analyze_journalist_trends(days)
        
        if not trends:
            print(f"\n❌ 최근 {days}일간 트렌드 데이터가 없습니다.")
            return
        
        print(f"\n📈 최근 {days}일간 기자 활동 트렌드")
        print("=" * 60)
        
        top_active = trends.get('top_active_journalists', [])
        if top_active:
            print(f"\n🔥 가장 활발한 기자 TOP 10:")
            print("-" * 40)
            print(f"{'순위':<4} {'기자명':<15} {'기사수':<8}")
            print("-" * 40)
            
            for i, (name, count) in enumerate(top_active, 1):
                if i == 1:
                    print(f"🥇 {i:<2} {name:<15} {count:<8}")
                elif i == 2:
                    print(f"🥈 {i:<2} {name:<15} {count:<8}")
                elif i == 3:
                    print(f"🥉 {i:<2} {name:<15} {count:<8}")
                else:
                    print(f"   {i:<2} {name:<15} {count:<8}")
        
        print(f"\n📊 분석 결과:")
        print(f"  - 분석 기간: {trends.get('analysis_period', 'N/A')}")
        print(f"  - 분석된 기자 수: {trends.get('total_journalists_analyzed', 0)}명")
    
    def get_journalist_insights(self, journalist_name: str) -> Dict[str, Any]:
        """특정 기자에 대한 상세 인사이트"""
        try:
            # 기자별 통계 조회
            journalist_stats = db_manager.get_journalist_stats_by_journalist(journalist_name)
            
            if not journalist_stats:
                return {}
            
            # 카테고리별 기사 수 계산
            category_distribution = {}
            total_articles = 0
            
            for stat in journalist_stats:
                category = stat['category']
                count = stat['article_count']
                category_distribution[category] = count
                total_articles += count
            
            # 주요 카테고리 찾기
            if category_distribution:
                main_category = max(category_distribution.items(), key=lambda x: x[1])
                main_category_name = main_category[0]
                main_category_count = main_category[1]
                main_category_ratio = (main_category_count / total_articles) * 100
            else:
                main_category_name = "N/A"
                main_category_count = 0
                main_category_ratio = 0
            
            # 기자 정보 조회
            journalist_info = db_manager.get_journalist_info(journalist_name, source='시사오늘')
            
            return {
                'journalist_name': journalist_name,
                'total_articles': total_articles,
                'category_distribution': category_distribution,
                'main_category': {
                    'name': main_category_name,
                    'count': main_category_count,
                    'ratio': main_category_ratio
                },
                'category_count': len(category_distribution),
                'journalist_info': journalist_info,
                'stats': journalist_stats
            }
            
        except Exception as e:
            logger.error(f"기자 인사이트 조회 실패: {e}")
            return {}
    
    def print_journalist_insights(self, journalist_name: str):
        """기자 인사이트 출력"""
        insights = self.get_journalist_insights(journalist_name)
        
        if not insights:
            print(f"\n❌ '{journalist_name}' 기자의 데이터를 찾을 수 없습니다.")
            return
        
        print(f"\n👤 {journalist_name} 기자 상세 분석")
        print("=" * 60)
        
        print(f"📊 기본 정보:")
        print(f"  - 총 기사 수: {insights['total_articles']}개")
        print(f"  - 활동 카테고리: {insights['category_count']}개")
        print(f"  - 주요 카테고리: {insights['main_category']['name']} ({insights['main_category']['count']}개, {insights['main_category']['ratio']:.1f}%)")
        
        print(f"\n📰 카테고리별 기사 분포:")
        category_dist = insights['category_distribution']
        if category_dist:
            sorted_categories = sorted(category_dist.items(), key=lambda x: x[1], reverse=True)
            for category, count in sorted_categories:
                ratio = (count / insights['total_articles']) * 100
                print(f"  - {category}: {count}개 ({ratio:.1f}%)")
        
        # 최근 활동 정보
        if insights.get('journalist_info'):
            info = insights['journalist_info']
            if info.get('last_article_date'):
                print(f"\n⏰ 최근 활동:")
                print(f"  - 마지막 기사: {info['last_article_date'].strftime('%Y-%m-%d %H:%M')}")
            if info.get('first_article_date'):
                print(f"  - 첫 기사: {info['first_article_date'].strftime('%Y-%m-%d %H:%M')}")

def main():
    """메인 실행 함수 (개선된 버전)"""
    import argparse
    
    parser = argparse.ArgumentParser(description='시사오늘 뉴스 크롤링 및 기자 순위 시스템')
    parser.add_argument('--mode', choices=['crawl', 'rank', 'trend', 'insight', 'all'], 
                       default='all', help='실행 모드 선택')
    parser.add_argument('--pages', type=int, default=1, 
                       help='카테고리당 크롤링할 페이지 수 (기본값: 1)')
    parser.add_argument('--workers', type=int, default=3, 
                       help='병렬 처리 워커 수 (기본값: 3)')
    parser.add_argument('--category', choices=['정치', '경제', '산업', '건설·부동산', 'IT', '유통·바이오', '사회', '자동차'], 
                       help='특정 카테고리만 크롤링')
    parser.add_argument('--journalist', type=str, 
                       help='특정 기자 인사이트 조회')
    parser.add_argument('--trend-days', type=int, default=7, 
                       help='트렌드 분석 기간 (일, 기본값: 7)')
    parser.add_argument('--force-refresh', action='store_true', 
                       help='캐시 무시하고 강제 새로고침')
    
    args = parser.parse_args()
    
    logger.info("시사오늘 크롤링 및 순위 시스템을 시작합니다...")
    
    # 데이터베이스 연결
    db_connected = db_manager.initialize_pool()
    if not db_connected:
        logger.warning("데이터베이스 연결 실패 - 크롤링만 진행합니다")
    
    # 테이블 생성 (필요한 경우)
    if db_connected:
        db_manager.create_tables()
    
    ranking_system = JournalistRankingSystem()
    
    if args.mode in ['crawl', 'all']:
        print("🚀 1단계: 시사오늘 뉴스 크롤링 시작")
        crawler = SisaonCrawler(max_workers=args.workers)
        
        if args.category:
            # 특정 카테고리만 크롤링
            if args.category in crawler.categories:
                category_code = crawler.categories[args.category]
                crawler.crawl_category(args.category, category_code, args.pages)
            else:
                print(f"❌ 잘못된 카테고리: {args.category}")
        else:
            # 모든 카테고리 크롤링
            crawler.crawl_all_categories(max_pages_per_category=args.pages)
    
    if args.mode in ['rank', 'all'] and db_connected:
        print("\n📊 2단계: 기자 통계 생성")
        if ranking_system.generate_journalist_stats(force_refresh=args.force_refresh):
            print("✅ 기자 통계 생성 완료")
            
            # 3단계: 순위 출력
            print("\n🏆 3단계: 기자 순위 출력")
            ranking_system.print_all_rankings(limit=10)
            
            # 특정 카테고리 순위 출력
            if args.category:
                ranking_system.print_category_rankings(args.category, limit=10)
            else:
                ranking_system.print_category_rankings('정치', limit=10)
                ranking_system.print_category_rankings('경제', limit=10)
        else:
            print("❌ 기자 통계 생성 실패")
    
    if args.mode in ['trend', 'all'] and db_connected:
        print(f"\n📈 4단계: 기자 활동 트렌드 분석 (최근 {args.trend_days}일)")
        ranking_system.print_trend_analysis(days=args.trend_days)
    
    if args.mode == 'insight' and args.journalist and db_connected:
        print(f"\n👤 5단계: {args.journalist} 기자 상세 분석")
        ranking_system.print_journalist_insights(args.journalist)
    
    # 추가 통계 정보 출력
    if db_connected and args.mode in ['rank', 'all']:
        print("\n📋 추가 통계 정보:")
        print("-" * 40)
        
        # 전체 통계
        stats = db_manager.get_crawling_statistics()
        if stats:
            print(f"📰 전체 기사 수: {stats.get('total_articles', 0)}개")
            print(f"📅 오늘 수집된 기사: {stats.get('today_articles', 0)}개")
            
            # 소스별 통계
            articles_by_source = stats.get('articles_by_source', {})
            if articles_by_source:
                print(f"📊 소스별 기사 분포:")
                for source, count in articles_by_source.items():
                    print(f"  - {source}: {count}개")
        
        # 카테고리 분포도
        category_dist = db_manager.get_category_distribution()
        if category_dist and category_dist.get('category_stats'):
            print(f"\n📂 카테고리별 분포:")
            for cat_stat in category_dist['category_stats'][:5]:  # 상위 5개만
                print(f"  - {cat_stat['category']}: {cat_stat['total_articles']}개 ({cat_stat['journalist_count']}명 기자)")
    
    logger.info("시사오늘 크롤링 및 순위 시스템이 완료되었습니다!")

if __name__ == "__main__":
    main() 