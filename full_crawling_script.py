#!/usr/bin/env python3
"""
시사오늘 전체 카테고리 완전 크롤링 스크립트
모든 카테고리의 모든 페이지를 크롤링하여 데이터베이스에 저장
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# 프로젝트 모듈 import
from sisaon_crawler_with_ranking import SisaonCrawler, JournalistRankingSystem
from database_manager import db_manager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('full_crawling.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FullCrawlingManager:
    """전체 크롤링 관리자"""
    
    def __init__(self, max_workers: int = 5, delay_between_categories: int = 10):
        self.max_workers = max_workers
        self.delay_between_categories = delay_between_categories
        
        # 환경변수 설정
        os.environ['DB_HOST'] = 'localhost'
        os.environ['DB_PORT'] = '5432'
        os.environ['DB_NAME'] = 'postgres'
        os.environ['DB_USER'] = 'postgres'
        # os.environ['DB_PASSWORD'] = 'your_password_here'  # 실제 비밀번호로 변경하세요
        
        # 크롤러 초기화
        self.crawler = SisaonCrawler(max_workers=max_workers)
        self.ranking_system = JournalistRankingSystem()
        
        # 전체 통계
        self.total_stats = {
            'start_time': None,
            'end_time': None,
            'total_articles': 0,
            'total_categories': 0,
            'total_pages': 0,
            'successful_categories': 0,
            'failed_categories': [],
            'category_details': {},
            'errors': 0,
            'duplicates': 0
        }
    
    def estimate_total_pages(self) -> Dict[str, int]:
        """각 카테고리의 총 페이지 수 추정"""
        logger.info("각 카테고리의 총 페이지 수를 확인하는 중...")
        
        category_pages = {}
        total_estimated_pages = 0
        
        for category, code in self.crawler.categories.items():
            try:
                category_url = f"{self.crawler.base_url}/news/articleList.html?sc_sub_section_code={code}&view_type=sm"
                total_pages = self.crawler.get_total_pages(category_url)
                category_pages[category] = total_pages
                total_estimated_pages += total_pages
                
                logger.info(f"  {category}: {total_pages}페이지")
                
                # 서버 부하 방지를 위한 지연
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"  {category} 페이지 수 확인 실패: {e}")
                category_pages[category] = 0
        
        logger.info(f"총 예상 페이지 수: {total_estimated_pages}페이지")
        return category_pages
    
    def crawl_single_category(self, category: str, category_code: str, max_pages: int = None) -> Dict[str, Any]:
        """단일 카테고리 크롤링"""
        logger.info(f"카테고리 '{category}' 크롤링 시작...")
        
        category_start_time = datetime.now()
        category_stats = {
            'category': category,
            'start_time': category_start_time,
            'end_time': None,
            'articles_count': 0,
            'pages_crawled': 0,
            'errors': 0,
            'duplicates': 0,
            'success': False
        }
        
        try:
            # 카테고리 URL 생성
            category_url = f"{self.crawler.base_url}/news/articleList.html?sc_sub_section_code={category_code}&view_type=sm"
            
            # 총 페이지 수 확인
            total_pages = self.crawler.get_total_pages(category_url)
            if max_pages:
                total_pages = min(total_pages, max_pages)
            
            logger.info(f"카테고리 '{category}': 총 {total_pages}페이지 크롤링 예정")
            
            # 페이지별 크롤링
            for page in range(1, total_pages + 1):
                try:
                    page_url = f"{category_url}&page={page}"
                    logger.info(f"  페이지 {page}/{total_pages} 크롤링 중... ({page/total_pages*100:.1f}%)")
                    
                    # 페이지에서 기사 링크 추출
                    article_links = self.crawler.get_article_links_from_page(page_url)
                    
                    if not article_links:
                        logger.warning(f"  페이지 {page}에서 기사 링크를 찾을 수 없습니다.")
                        continue
                    
                    logger.info(f"  페이지 {page}에서 {len(article_links)}개 기사 링크 발견")
                    
                    # 병렬 처리로 기사 크롤링
                    successful_articles = 0
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        future_to_url = {
                            executor.submit(self.crawler._process_article, article_url, category): article_url 
                            for article_url in article_links
                        }
                        
                        for future in as_completed(future_to_url):
                            article_url = future_to_url[future]
                            try:
                                result = future.result(timeout=60)
                                if result:
                                    successful_articles += 1
                                    category_stats['articles_count'] += 1
                                    self.total_stats['total_articles'] += 1
                            except Exception as e:
                                logger.error(f"  기사 처리 실패 {article_url}: {e}")
                                category_stats['errors'] += 1
                                self.total_stats['errors'] += 1
                    
                    category_stats['pages_crawled'] += 1
                    self.total_stats['total_pages'] += 1
                    
                    logger.info(f"  페이지 {page} 완료: {successful_articles}/{len(article_links)} 기사 성공")
                    
                    # 페이지 간격 조절
                    self.crawler._random_delay()
                    
                except Exception as e:
                    logger.error(f"  페이지 {page} 크롤링 실패: {e}")
                    category_stats['errors'] += 1
                    self.total_stats['errors'] += 1
                    continue
            
            category_stats['success'] = True
            self.total_stats['successful_categories'] += 1
            
        except Exception as e:
            logger.error(f"카테고리 '{category}' 크롤링 실패: {e}")
            category_stats['errors'] += 1
            self.total_stats['errors'] += 1
            self.total_stats['failed_categories'].append(category)
        
        finally:
            category_stats['end_time'] = datetime.now()
            category_duration = (category_stats['end_time'] - category_stats['start_time']).total_seconds()
            
            logger.info(f"카테고리 '{category}' 크롤링 완료:")
            logger.info(f"  - 기사 수: {category_stats['articles_count']}개")
            logger.info(f"  - 페이지 수: {category_stats['pages_crawled']}페이지")
            logger.info(f"  - 오류 수: {category_stats['errors']}개")
            logger.info(f"  - 소요 시간: {category_duration:.2f}초")
            
            self.total_stats['category_details'][category] = category_stats
        
        return category_stats
    
    def crawl_all_categories(self, max_pages_per_category: int = None, 
                           skip_categories: List[str] = None) -> bool:
        """모든 카테고리 크롤링"""
        logger.info("전체 카테고리 크롤링을 시작합니다...")
        
        self.total_stats['start_time'] = datetime.now()
        self.total_stats['total_categories'] = len(self.crawler.categories)
        
        # 데이터베이스 연결 확인
        if not db_manager.initialize_pool():
            logger.error("데이터베이스 연결 실패")
            return False
        
        # 테이블 생성 확인
        db_manager.create_tables()
        
        # 총 페이지 수 추정
        category_pages = self.estimate_total_pages()
        
        # 크롤링할 카테고리 결정
        categories_to_crawl = {}
        for category, code in self.crawler.categories.items():
            if skip_categories and category in skip_categories:
                logger.info(f"카테고리 '{category}' 건너뛰기")
                continue
            categories_to_crawl[category] = code
        
        logger.info(f"크롤링할 카테고리 수: {len(categories_to_crawl)}개")
        
        # 카테고리별 순차 크롤링
        for i, (category, code) in enumerate(categories_to_crawl.items(), 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"카테고리 {i}/{len(categories_to_crawl)}: {category}")
            logger.info(f"{'='*60}")
            
            # 카테고리 크롤링
            category_stats = self.crawl_single_category(category, code, max_pages_per_category)
            
            # 카테고리 간격 조절
            if i < len(categories_to_crawl):
                logger.info(f"다음 카테고리까지 {self.delay_between_categories}초 대기...")
                time.sleep(self.delay_between_categories)
        
        self.total_stats['end_time'] = datetime.now()
        self.print_final_summary()
        
        return True
    
    def print_final_summary(self):
        """최종 요약 출력"""
        duration = (self.total_stats['end_time'] - self.total_stats['start_time']).total_seconds()
        
        print("\n" + "="*80)
        print("🎉 전체 카테고리 크롤링 완료!")
        print("="*80)
        
        print(f"\n📊 전체 통계:")
        print(f"  - 총 소요 시간: {duration:.2f}초 ({duration/3600:.2f}시간)")
        print(f"  - 총 기사 수: {self.total_stats['total_articles']}개")
        print(f"  - 총 페이지 수: {self.total_stats['total_pages']}페이지")
        print(f"  - 성공한 카테고리: {self.total_stats['successful_categories']}/{self.total_stats['total_categories']}개")
        print(f"  - 총 오류 수: {self.total_stats['errors']}개")
        print(f"  - 중복 기사 수: {self.total_stats['duplicates']}개")
        
        if self.total_stats['failed_categories']:
            print(f"  - 실패한 카테고리: {', '.join(self.total_stats['failed_categories'])}")
        
        print(f"\n📈 성능 통계:")
        if duration > 0:
            print(f"  - 평균 처리 속도: {self.total_stats['total_articles']/duration*60:.1f}개/분")
            print(f"  - 페이지당 평균 시간: {duration/self.total_stats['total_pages']:.2f}초")
        
        print(f"\n📋 카테고리별 상세 결과:")
        print("-" * 80)
        for category, stats in self.total_stats['category_details'].items():
            if stats['success']:
                duration = (stats['end_time'] - stats['start_time']).total_seconds()
                print(f"✅ {category:<15}: {stats['articles_count']:4d}개 기사, {stats['pages_crawled']:3d}페이지, {duration:6.1f}초")
            else:
                print(f"❌ {category:<15}: 실패")
        
        # 크롤링 로그 저장
        if hasattr(db_manager, 'connection_pool') and db_manager.connection_pool:
            db_manager.log_crawling_job(
                job_type='full_sisaon_crawling',
                source_key='시사오늘',
                status='completed',
                articles_count=self.total_stats['total_articles'],
                duration_seconds=duration,
                duplicates_count=self.total_stats['duplicates'],
                errors_count=self.total_stats['errors'],
                error_message=f"실패한 카테고리: {', '.join(self.total_stats['failed_categories'])}" if self.total_stats['failed_categories'] else None
            )

def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='시사오늘 전체 카테고리 완전 크롤링')
    parser.add_argument('--workers', type=int, default=5, 
                       help='병렬 처리 워커 수 (기본값: 5)')
    parser.add_argument('--max-pages', type=int, 
                       help='카테고리당 최대 페이지 수 (기본값: 모든 페이지)')
    parser.add_argument('--delay', type=int, default=10, 
                       help='카테고리 간 대기 시간 (초, 기본값: 10)')
    parser.add_argument('--skip', nargs='+', 
                       help='건너뛸 카테고리 목록')
    parser.add_argument('--estimate-only', action='store_true', 
                       help='페이지 수만 추정하고 크롤링하지 않음')
    
    args = parser.parse_args()
    
    print("="*80)
    print("🚀 시사오늘 전체 카테고리 완전 크롤링 시스템")
    print("="*80)
    
    # 크롤링 매니저 초기화
    manager = FullCrawlingManager(
        max_workers=args.workers,
        delay_between_categories=args.delay
    )
    
    if args.estimate_only:
        # 페이지 수만 추정
        print("\n📊 페이지 수 추정 모드")
        category_pages = manager.estimate_total_pages()
        total_pages = sum(category_pages.values())
        print(f"\n총 예상 페이지 수: {total_pages}페이지")
        return
    
    # 전체 크롤링 실행
    success = manager.crawl_all_categories(
        max_pages_per_category=args.max_pages,
        skip_categories=args.skip
    )
    
    if success:
        print("\n🎉 전체 크롤링이 성공적으로 완료되었습니다!")
    else:
        print("\n💥 크롤링 중 오류가 발생했습니다.")
        sys.exit(1)

if __name__ == "__main__":
    main() 