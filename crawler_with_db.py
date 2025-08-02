#!/usr/bin/env python3
"""
데이터베이스와 연결된 크롤링 시스템 예제
"""

import asyncio
import aiohttp
import feedparser
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import List, Dict, Any
import time

# 데이터베이스 매니저 import
from database_manager import db_manager

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NewsCrawler:
    """뉴스 크롤러 (데이터베이스 연동)"""
    
    def __init__(self):
        self.session = None
        # 데이터베이스 초기화
        self.db = db_manager
        self.db.initialize_pool()
        self.db.create_tables()
    
    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        if self.session:
            await self.session.close()
        # 데이터베이스 연결 풀 종료
        self.db.close_pool()
    
    async def fetch_rss_feed(self, rss_url: str) -> List[Dict[str, Any]]:
        """RSS 피드에서 기사 목록 가져오기"""
        try:
            async with self.session.get(rss_url) as response:
                if response.status == 200:
                    content = await response.text()
                    feed = feedparser.parse(content)
                    
                    articles = []
                    for entry in feed.entries:
                        article = {
                            'title': entry.get('title', ''),
                            'url': entry.get('link', ''),
                            'description': entry.get('summary', ''),
                            'published_date': datetime.now(),  # RSS에서 파싱 가능
                            'source': 'RSS Feed',
                            'categories': [],
                            'tags': [],
                            'metadata': {
                                'feed_url': rss_url,
                                'entry_id': entry.get('id', '')
                            }
                        }
                        articles.append(article)
                    
                    logger.info(f"RSS 피드에서 {len(articles)}개 기사 수집: {rss_url}")
                    return articles
                else:
                    logger.error(f"RSS 피드 접근 실패: {rss_url}, 상태: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"RSS 피드 처리 오류: {e}")
            return []
    
    async def scrape_article_content(self, article: Dict[str, Any]) -> Dict[str, Any]:
        """기사 URL에서 본문 내용 스크래핑"""
        try:
            async with self.session.get(article['url']) as response:
                if response.status == 200:
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # 본문 추출 (사이트별로 선택자 조정 필요)
                    content_selectors = [
                        'article',
                        '.article-content',
                        '.post-content',
                        '.entry-content',
                        '.content',
                        '#content'
                    ]
                    
                    content = None
                    for selector in content_selectors:
                        element = soup.select_one(selector)
                        if element:
                            content = element.get_text(strip=True)
                            break
                    
                    if content:
                        article['content'] = content
                        logger.info(f"기사 본문 스크래핑 완료: {article['title']}")
                    else:
                        article['content'] = article.get('description', '')
                        logger.warning(f"본문 추출 실패, 설명 사용: {article['title']}")
                    
                    return article
                else:
                    logger.error(f"기사 페이지 접근 실패: {article['url']}")
                    return article
                    
        except Exception as e:
            logger.error(f"기사 스크래핑 오류: {e}")
            return article
    
    async def crawl_and_save(self, rss_urls: List[str], source_name: str):
        """크롤링하고 데이터베이스에 저장"""
        start_time = time.time()
        
        try:
            logger.info(f"크롤링 시작: {source_name}")
            
            all_articles = []
            
            # RSS 피드에서 기사 목록 수집
            for rss_url in rss_urls:
                articles = await self.fetch_rss_feed(rss_url)
                all_articles.extend(articles)
            
            # 중복 제거 (URL 기준)
            unique_articles = {}
            for article in all_articles:
                if article['url'] not in unique_articles:
                    unique_articles[article['url']] = article
            
            articles_list = list(unique_articles.values())
            logger.info(f"중복 제거 후 {len(articles_list)}개 기사")
            
            # 기사 본문 스크래핑
            scraped_articles = []
            for article in articles_list:
                # 데이터베이스에 이미 존재하는지 확인
                if not self.db.save_article(article):
                    continue  # 이미 존재하면 스킵
                
                # 본문 스크래핑
                scraped_article = await self.scrape_article_content(article)
                scraped_articles.append(scraped_article)
                
                # 스크래핑된 기사 업데이트
                self.db.save_article(scraped_article)
                
                # 요청 간격 조절
                await asyncio.sleep(1)
            
            # 크롤링 로그 저장
            duration = time.time() - start_time
            self.db.log_crawling_job(
                job_type='rss_crawling',
                source_key=source_name,
                status='completed',
                articles_count=len(scraped_articles),
                duration_seconds=duration
            )
            
            logger.info(f"크롤링 완료: {len(scraped_articles)}개 기사, {duration:.2f}초")
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"크롤링 실패: {e}")
            
            # 에러 로그 저장
            self.db.log_crawling_job(
                job_type='rss_crawling',
                source_key=source_name,
                status='failed',
                duration_seconds=duration,
                error_message=str(e)
            )

async def main():
    """메인 함수"""
    # RSS 피드 URL들 (예시)
    rss_feeds = {
        '연합뉴스': [
            'https://www.yonhapnews.co.kr/feed/headline.xml',
            'https://www.yonhapnews.co.kr/feed/politics.xml'
        ],
        '시사오늘': [
            'https://www.sisaoneul.com/rss.xml'
        ]
    }
    
    async with NewsCrawler() as crawler:
        # 각 소스별로 크롤링 실행
        for source_name, urls in rss_feeds.items():
            await crawler.crawl_and_save(urls, source_name)
        
        # 크롤링 통계 출력
        stats = db_manager.get_crawling_statistics()
        print("\n" + "=" * 50)
        print("크롤링 통계")
        print("=" * 50)
        print(f"전체 기사 수: {stats.get('total_articles', 0)}")
        print(f"오늘 수집된 기사: {stats.get('today_articles', 0)}")
        print(f"소스별 기사 수: {stats.get('articles_by_source', {})}")
        
        # 최근 기사 목록 출력
        recent_articles = db_manager.get_articles(limit=5)
        print(f"\n최근 수집된 기사:")
        for article in recent_articles:
            print(f"- {article['title']} ({article['source']})")

if __name__ == "__main__":
    # 환경변수 설정 (실제 값으로 변경)
    import os
    os.environ['DB_HOST'] = 'localhost'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_NAME'] = 'newspaper_db'
    os.environ['DB_USER'] = 'EZ0320'
    os.environ['DB_PASSWORD'] = 'Excel*2002'
    
    # 크롤링 실행
    asyncio.run(main()) 