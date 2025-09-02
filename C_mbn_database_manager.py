#!/usr/bin/env python3
"""
PostgreSQL 데이터베이스 연결 및 관리 모듈 (개선된 버전)
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import os
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json
import time
import pytz

logger = logging.getLogger(__name__)

class DatabaseManager:
    """PostgreSQL 데이터베이스 관리 클래스 (개선된 버전)"""
    
    def __init__(self): #데이터베이스 연결 파라미터 설정
        self.connection_pool = None
        self.connection_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5433'),
            'database': os.getenv('DB_NAME', 'mbn'),
            'user': os.getenv('DB_USER', 'postgres'),
                            'password': os.getenv('DB_PASSWORD', '1226')
        }
        
        # 성능 설정
        self.batch_size = 100
        self.max_retries = 3
        self.retry_delay = 1
        
        # 통계 캐시
        self._stats_cache = {}
        self._cache_expiry = {}
        self.cache_duration = 300  # 5분
        
        # 시간대 설정
        self.korea_tz = pytz.timezone('Asia/Seoul')
        self.utc_tz = pytz.timezone('UTC')
    
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
    
    def _execute_with_retry(self, func, *args, **kwargs):
        """재시도 로직이 포함된 함수 실행"""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except psycopg2.OperationalError as e:
                if attempt == self.max_retries - 1:
                    raise e
                logger.warning(f"데이터베이스 작업 실패 (재시도 {attempt + 1}/{self.max_retries}): {e}")
                time.sleep(self.retry_delay * (2 ** attempt))  # 지수 백오프
            except Exception as e:
                raise e
    
    def initialize_pool(self, min_connections=1, max_connections=10):
        """연결 풀 초기화 (개선된 버전)"""
        try:
            self.connection_pool = SimpleConnectionPool(
                min_connections, 
                max_connections, 
                **self.connection_params
            )
            
            # 연결 테스트
            test_connection = self.get_connection()
            if test_connection:
                test_connection.close()
                self.return_connection(test_connection)
                logger.info("데이터베이스 연결 풀이 성공적으로 초기화되었습니다.")
                return True
            else:
                logger.error("데이터베이스 연결 테스트 실패")
                return False
                
        except Exception as e:
            logger.error(f"연결 풀 초기화 실패: {e}")
            return False
    
    def get_connection(self):
        """연결 풀에서 연결 가져오기 (개선된 버전)"""
        if not self.connection_pool:
            if not self.initialize_pool():
                return None
        
        try:
            connection = self.connection_pool.getconn()
            if connection:
                # 연결 상태 확인
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
            return connection
        except Exception as e:
            logger.error(f"연결 가져오기 실패: {e}")
            return None
    
    def return_connection(self, connection):
        """연결을 풀로 반환 (개선된 버전)"""
        if self.connection_pool and connection:
            try:
                # 연결 상태 확인 후 반환
                if not connection.closed:
                    self.connection_pool.putconn(connection)
                else:
                    logger.warning("닫힌 연결을 반환하려고 시도했습니다.")
            except Exception as e:
                logger.error(f"연결 반환 실패: {e}")
    
    def close_pool(self):
        """연결 풀 종료 (개선된 버전)"""
        if self.connection_pool:
            try:
                self.connection_pool.closeall()
                logger.info("데이터베이스 연결 풀이 안전하게 종료되었습니다.")
            except Exception as e:
                logger.error(f"연결 풀 종료 실패: {e}")
    
    def create_tables(self):
        """필요한 테이블들 생성 (개선된 버전)"""
        connection = self.get_connection()
        if not connection:
            logger.error("데이터베이스 연결을 가져올 수 없습니다.")
            return False
            
        try:
            cursor = connection.cursor()
            
            # 뉴스 기사 테이블 (개선된 스키마)
            create_articles_table = """
            CREATE TABLE IF NOT EXISTS news_articles (
                id SERIAL PRIMARY KEY,
                title VARCHAR(500) NOT NULL,
                content TEXT,
                url VARCHAR(1000) UNIQUE NOT NULL,
                source VARCHAR(100) NOT NULL,
                author VARCHAR(200),
                published_date TIMESTAMP,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_processed BOOLEAN DEFAULT FALSE,
                categories TEXT[],
                tags TEXT[],
                metadata JSONB,
                word_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_articles_table)
            
            # RSS 피드 테이블
            create_rss_feeds_table = """
            CREATE TABLE IF NOT EXISTS rss_feeds (
                id SERIAL PRIMARY KEY,
                source_key VARCHAR(100) NOT NULL,
                feed_url VARCHAR(500) NOT NULL,
                category VARCHAR(100),
                last_fetched TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_rss_feeds_table)
            
            # 크롤링 로그 테이블 (개선된 스키마)
            create_crawling_logs_table = """
            CREATE TABLE IF NOT EXISTS crawling_logs (
                id SERIAL PRIMARY KEY,
                job_type VARCHAR(50) NOT NULL,
                source_key VARCHAR(100),
                status VARCHAR(20) NOT NULL,
                articles_count INTEGER DEFAULT 0,
                duplicates_count INTEGER DEFAULT 0,
                errors_count INTEGER DEFAULT 0,
                duration_seconds DECIMAL(10,2),
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_crawling_logs_table)
            
            # 기자 정보 테이블 (시사오늘 기사 내용 포함)
            create_journalists_table = """
            CREATE TABLE IF NOT EXISTS journalists (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL UNIQUE,
                source VARCHAR(100) NOT NULL,
                total_articles INTEGER DEFAULT 0,
                first_article_date TIMESTAMP,
                last_article_date TIMESTAMP,
                categories TEXT[],
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                -- 기사 내용 저장용 컬럼들
                article_titles TEXT[],
                article_contents TEXT[],
                article_urls TEXT[],
                article_published_dates TIMESTAMP[],
                article_categories TEXT[]
            );
            """
            cursor.execute(create_journalists_table)
            
            # 기자 카테고리 통계 테이블 추가
            create_journalist_category_stats_table = """
            CREATE TABLE IF NOT EXISTS journalist_category_stats (
                id SERIAL PRIMARY KEY,
                journalist_name VARCHAR(200) NOT NULL,
                category VARCHAR(100) NOT NULL,
                article_count INTEGER DEFAULT 0,
                last_article_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(journalist_name, category)
            );
            """
            cursor.execute(create_journalist_category_stats_table)
            

            
            # 성능 최적화 인덱스 생성
            create_indexes = """
            -- 기존 인덱스
            CREATE INDEX IF NOT EXISTS idx_journalists_name ON journalists(name);
            CREATE INDEX IF NOT EXISTS idx_journalists_source ON journalists(source);
            CREATE INDEX IF NOT EXISTS idx_journalists_total_articles ON journalists(total_articles DESC);
            CREATE INDEX IF NOT EXISTS idx_journalists_last_article_date ON journalists(last_article_date DESC);
            
            -- 새로운 인덱스
            CREATE INDEX IF NOT EXISTS idx_articles_source ON news_articles(source);
            CREATE INDEX IF NOT EXISTS idx_articles_author ON news_articles(author);
            CREATE INDEX IF NOT EXISTS idx_articles_published_date ON news_articles(published_date DESC);
            CREATE INDEX IF NOT EXISTS idx_articles_created_at ON news_articles(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_articles_url_hash ON news_articles USING hash(url);
            
            -- 기자 카테고리 통계 인덱스
            CREATE INDEX IF NOT EXISTS idx_journalist_stats_name ON journalist_category_stats(journalist_name);
            CREATE INDEX IF NOT EXISTS idx_journalist_stats_category ON journalist_category_stats(category);
            CREATE INDEX IF NOT EXISTS idx_journalist_stats_updated_at ON journalist_category_stats(updated_at DESC);
            """
            cursor.execute(create_indexes)
            
            connection.commit()
            logger.info("데이터베이스 테이블이 성공적으로 생성되었습니다.")
            return True
            
        except Exception as e:
            connection.rollback()
            logger.error(f"테이블 생성 실패: {e}")
            return False
        finally:
            self.return_connection(connection)
    
    def save_article(self, article_data: Dict[str, Any]) -> bool:
        """뉴스 기사를 데이터베이스에 저장 (개선된 버전)"""
        def _save():
            connection = self.get_connection()
            if not connection:
                return False
                
            try:
                cursor = connection.cursor()
                
                # 중복 체크 (URL 기반)
                cursor.execute("SELECT id FROM news_articles WHERE url = %s", (article_data['url'],))
                if cursor.fetchone():
                    logger.debug(f"기사가 이미 존재합니다: {article_data['title']}")
                    return False
                
                # 단어 수 계산
                word_count = len(article_data.get('content', '').split())
                
                # 기사 저장
                insert_sql = """
                INSERT INTO news_articles (
                    title, content, url, source, author, published_date, 
                    categories, tags, metadata, word_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(insert_sql, (
                    article_data.get('title'),
                    article_data.get('content'),
                    article_data.get('url'),
                    article_data.get('source'),
                    article_data.get('author'),
                    article_data.get('published_date'),
                    article_data.get('categories', []),
                    article_data.get('tags', []),
                    json.dumps(article_data.get('metadata', {})),
                    word_count
                ))
                
                connection.commit()
                logger.debug(f"기사 저장 완료: {article_data.get('title')}")
                return True
                
            except Exception as e:
                connection.rollback()
                logger.error(f"기사 저장 실패: {e}")
                return False
            finally:
                self.return_connection(connection)
        
        return self._execute_with_retry(_save)
    
    def save_articles_batch(self, articles: List[Dict[str, Any]]) -> int:
        """여러 기사를 일괄 저장 (개선된 버전)"""
        if not articles:
            return 0
        
        def _save_batch():
            connection = self.get_connection()
            if not connection:
                return 0
                
            saved_count = 0
            
            try:
                cursor = connection.cursor()
                
                # 배치 크기로 나누어 처리
                for i in range(0, len(articles), self.batch_size):
                    batch = articles[i:i + self.batch_size]
                    
                    for article in batch:
                        try:
                            # 중복 체크
                            cursor.execute("SELECT id FROM news_articles WHERE url = %s", (article['url'],))
                            if cursor.fetchone():
                                continue
                            
                            # 단어 수 계산
                            word_count = len(article.get('content', '').split())
                            
                            # 기사 저장
                            insert_sql = """
                            INSERT INTO news_articles (
                                title, content, url, source, author, published_date, 
                                categories, tags, metadata, word_count
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """
                            
                            cursor.execute(insert_sql, (
                                article.get('title'),
                                article.get('content'),
                                article.get('url'),
                                article.get('source'),
                                article.get('author'),
                                article.get('published_date'),
                                article.get('categories', []),
                                article.get('tags', []),
                                json.dumps(article.get('metadata', {})),
                                word_count
                            ))
                            
                            saved_count += 1
                            
                        except Exception as e:
                            logger.error(f"개별 기사 저장 실패: {e}")
                            continue
                    
                    # 배치마다 커밋
                    connection.commit()
                
                logger.info(f"일괄 저장 완료: {saved_count}/{len(articles)} 기사")
                return saved_count
                
            except Exception as e:
                connection.rollback()
                logger.error(f"일괄 저장 실패: {e}")
                return 0
            finally:
                self.return_connection(connection)
        
        return self._execute_with_retry(_save_batch)
    
    def log_crawling_job(self, job_type: str, source_key: str, status: str, 
                        articles_count: int = 0, duration_seconds: float = 0, 
                        error_message: str = None, duplicates_count: int = 0, 
                        errors_count: int = 0):
        """크롤링 작업 로그 저장 (개선된 버전)"""
        def _log():
            connection = self.get_connection()
            if not connection:
                return False
                
            try:
                cursor = connection.cursor()
                
                insert_sql = """
                INSERT INTO crawling_logs (
                    job_type, source_key, status, articles_count, 
                    duration_seconds, error_message, duplicates_count, errors_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(insert_sql, (
                    job_type, source_key, status, articles_count, 
                    duration_seconds, error_message, duplicates_count, errors_count
                ))
                
                connection.commit()
                logger.info(f"크롤링 로그 저장: {job_type} - {status}")
                return True
                
            except Exception as e:
                connection.rollback()
                logger.error(f"크롤링 로그 저장 실패: {e}")
                return False
            finally:
                self.return_connection(connection)
        
        return self._execute_with_retry(_log)
    
    def get_articles(self, limit: int = 100, offset: int = 0, 
                    source: str = None, processed: bool = None) -> List[Dict[str, Any]]:
        """기사 목록 조회"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            where_conditions = []
            params = []
            
            if source:
                where_conditions.append("source = %s")
                params.append(source)
            
            if processed is not None:
                where_conditions.append("is_processed = %s")
                params.append(processed)
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            query = f"""
            SELECT * FROM news_articles 
            WHERE {where_clause}
            ORDER BY created_at DESC 
            LIMIT %s OFFSET %s
            """
            
            params.extend([limit, offset])
            cursor.execute(query, params)
            
            articles = cursor.fetchall()
            return [dict(article) for article in articles]
            
        except Exception as e:
            logger.error(f"기사 조회 실패: {e}")
            return []
        finally:
            self.return_connection(connection)
    
    def get_article_count(self, source: str = None) -> int:
        """기사 개수 조회"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            
            if source:
                cursor.execute("SELECT COUNT(*) FROM news_articles WHERE source = %s", (source,))
            else:
                cursor.execute("SELECT COUNT(*) FROM news_articles")
            
            count = cursor.fetchone()[0]
            return count
            
        except Exception as e:
            logger.error(f"기사 개수 조회 실패: {e}")
            return 0
        finally:
            self.return_connection(connection)
    
    def article_exists(self, url: str) -> bool:
        """URL로 기사 존재 여부 확인"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM news_articles WHERE url = %s", (url,))
            count = cursor.fetchone()[0]
            return count > 0
            
        except Exception as e:
            logger.error(f"기사 존재 여부 확인 실패: {e}")
            return False
        finally:
            self.return_connection(connection)
    
    def save_or_update_journalist(self, journalist_name: str, source: str, category: str = None) -> bool:
        """기자 정보 저장 또는 업데이트"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            
            # 기자 존재 여부 확인
            cursor.execute("SELECT id, categories, total_articles FROM journalists WHERE name = %s AND source = %s", 
                         (journalist_name, source))
            existing_journalist = cursor.fetchone()
            
            if existing_journalist:
                # 기존 기자 정보 업데이트
                journalist_id, existing_categories, total_articles = existing_journalist
                
                # 카테고리 추가
                if category and category not in (existing_categories or []):
                    if existing_categories:
                        updated_categories = existing_categories + [category]
                    else:
                        updated_categories = [category]
                else:
                    updated_categories = existing_categories
                
                # 기사 수 증가 및 마지막 기사 날짜 업데이트
                cursor.execute("""
                    UPDATE journalists 
                    SET total_articles = total_articles + 1,
                        last_article_date = CURRENT_TIMESTAMP,
                        categories = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (updated_categories, journalist_id))
                
            else:
                # 새로운 기자 추가
                cursor.execute("""
                    INSERT INTO journalists (name, source, total_articles, first_article_date, last_article_date, categories)
                    VALUES (%s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s)
                """, (journalist_name, source, [category] if category else None))
            
            connection.commit()
            return True
            
        except Exception as e:
            connection.rollback()
            logger.error(f"기자 정보 저장 실패: {e}")
            return False
        finally:
            self.return_connection(connection)
    
    def get_journalist_info(self, journalist_name: str, source: str = None) -> Optional[Dict[str, Any]]:
        """기자 정보 조회"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            if source:
                cursor.execute("""
                    SELECT * FROM journalists 
                    WHERE name = %s AND source = %s
                """, (journalist_name, source))
            else:
                cursor.execute("""
                    SELECT * FROM journalists 
                    WHERE name = %s
                """, (journalist_name,))
            
            result = cursor.fetchone()
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"기자 정보 조회 실패: {e}")
            return None
        finally:
            self.return_connection(connection)
    
    def get_all_journalists(self, source: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """모든 기자 정보 조회"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            if source:
                cursor.execute("""
                    SELECT * FROM journalists 
                    WHERE source = %s 
                    ORDER BY total_articles DESC, last_article_date DESC
                    LIMIT %s
                """, (source, limit))
            else:
                cursor.execute("""
                    SELECT * FROM journalists 
                    ORDER BY total_articles DESC, last_article_date DESC
                    LIMIT %s
                """, (limit,))
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"기자 목록 조회 실패: {e}")
            return []
        finally:
            self.return_connection(connection)
    
    def get_journalists_by_category(self, category: str, source: str = None, limit: int = 50) -> List[Dict[str, Any]]:
        """특정 카테고리의 기자 조회"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            if source:
                cursor.execute("""
                    SELECT * FROM journalists 
                    WHERE %s = ANY(categories) AND source = %s
                    ORDER BY total_articles DESC, last_article_date DESC
                    LIMIT %s
                """, (category, source, limit))
            else:
                cursor.execute("""
                    SELECT * FROM journalists 
                    WHERE %s = ANY(categories)
                    ORDER BY total_articles DESC, last_article_date DESC
                    LIMIT %s
                """, (category, limit))
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"카테고리별 기자 조회 실패: {e}")
            return []
        finally:
            self.return_connection(connection)
    
    def mark_article_processed(self, article_id: int) -> bool:
        """기사를 처리 완료로 표시"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            
            cursor.execute(
                "UPDATE news_articles SET is_processed = TRUE WHERE id = %s", 
                (article_id,)
            )
            
            connection.commit()
            return True
            
        except Exception as e:
            connection.rollback()
            logger.error(f"기사 처리 상태 업데이트 실패: {e}")
            return False
        finally:
            self.return_connection(connection)
    
    def get_crawling_statistics(self) -> Dict[str, Any]:
        """크롤링 통계 조회"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            
            # 전체 기사 수
            cursor.execute("SELECT COUNT(*) FROM news_articles")
            total_articles = cursor.fetchone()[0]
            
            # 소스별 기사 수
            cursor.execute("""
                SELECT source, COUNT(*) as count 
                FROM news_articles 
                GROUP BY source 
                ORDER BY count DESC
            """)
            articles_by_source = dict(cursor.fetchall())
            
            # 오늘 수집된 기사 수
            cursor.execute("""
                SELECT COUNT(*) FROM news_articles 
                WHERE DATE(created_at) = CURRENT_DATE
            """)
            today_articles = cursor.fetchone()[0]
            
            # 최근 크롤링 로그
            cursor.execute("""
                SELECT job_type, status, articles_count, created_at 
                FROM crawling_logs 
                ORDER BY created_at DESC 
                LIMIT 10
            """)
            recent_logs = cursor.fetchall()
            
            return {
                'total_articles': total_articles,
                'articles_by_source': articles_by_source,
                'today_articles': today_articles,
                'recent_logs': recent_logs
            }
            
        except Exception as e:
            logger.error(f"크롤링 통계 조회 실패: {e}")
            return {}
        finally:
            self.return_connection(connection)

    # 기자 카테고리 통계 관련 메서드들
    def update_journalist_stats(self, journalist_name: str, category: str, increment: int = 1, article_data: Dict[str, Any] = None) -> bool:
        """기자 통계 업데이트 (journalists 테이블 기반, 기사 내용 포함)"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            
            # 기자 존재 여부 확인
            cursor.execute("SELECT id, categories, total_articles, article_titles, article_contents, article_urls, article_published_dates, article_categories FROM journalists WHERE name = %s AND source = '시사오늘'", (journalist_name,))
            existing_journalist = cursor.fetchone()
            
            if existing_journalist:
                # 기존 기자 정보 업데이트
                journalist_id, existing_categories, total_articles, existing_titles, existing_contents, existing_urls, existing_dates, existing_article_categories = existing_journalist
                
                # 카테고리 추가
                if category and category not in (existing_categories or []):
                    if existing_categories:
                        updated_categories = existing_categories + [category]
                    else:
                        updated_categories = [category]
                else:
                    updated_categories = existing_categories
                
                # 기사 내용 배열 업데이트
                if article_data:
                    new_title = article_data.get('title', '')
                    new_content = article_data.get('content', '')
                    new_url = article_data.get('url', '')
                    new_published_date = article_data.get('published_date')
                    new_category = category
                    
                    # 기존 배열에 새 기사 정보 추가
                    updated_titles = (existing_titles or []) + [new_title]
                    updated_contents = (existing_contents or []) + [new_content]
                    updated_urls = (existing_urls or []) + [new_url]
                    updated_dates = (existing_dates or []) + [new_published_date]
                    updated_article_categories = (existing_article_categories or []) + [new_category]
                else:
                    updated_titles = existing_titles
                    updated_contents = existing_contents
                    updated_urls = existing_urls
                    updated_dates = existing_dates
                    updated_article_categories = existing_article_categories
                
                # 기사 수 증가 및 마지막 기사 날짜 업데이트
                cursor.execute("""
                    UPDATE journalists 
                    SET total_articles = total_articles + %s,
                        last_article_date = CURRENT_TIMESTAMP,
                        categories = %s,
                        article_titles = %s,
                        article_contents = %s,
                        article_urls = %s,
                        article_published_dates = %s,
                        article_categories = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (increment, updated_categories, updated_titles, updated_contents, updated_urls, updated_dates, updated_article_categories, journalist_id))
                
            else:
                # 새로운 기자 추가
                if article_data:
                    new_title = article_data.get('title', '')
                    new_content = article_data.get('content', '')
                    new_url = article_data.get('url', '')
                    new_published_date = article_data.get('published_date')
                    
                    cursor.execute("""
                        INSERT INTO journalists (
                            name, source, total_articles, first_article_date, last_article_date, categories,
                            article_titles, article_contents, article_urls, article_published_dates, article_categories
                        ) VALUES (%s, '시사오늘', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s)
                    """, (journalist_name, increment, [category] if category else None, 
                          [new_title], [new_content], [new_url], [new_published_date], [category]))
                else:
                    cursor.execute("""
                        INSERT INTO journalists (name, source, total_articles, first_article_date, last_article_date, categories)
                        VALUES (%s, '시사오늘', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s)
                    """, (journalist_name, increment, [category] if category else None))
            
            connection.commit()
            logger.info(f"기자 통계 업데이트: {journalist_name} - {category} (+{increment})")
            return True
            
        except Exception as e:
            connection.rollback()
            logger.error(f"기자 통계 업데이트 실패: {e}")
            return False
        finally:
            self.return_connection(connection)
    
    def get_journalist_stats_by_category(self, category: str = None, limit: int = 50) -> List[Dict[str, Any]]:
        """카테고리별 기자 통계 조회 (news_articles 테이블 기반)"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            if category:
                query = """
                SELECT author as journalist_name, %s as category, COUNT(*) as article_count, MAX(created_at) as updated_at
                FROM news_articles
                WHERE author IS NOT NULL AND %s = ANY(categories)
                GROUP BY author
                ORDER BY article_count DESC, author
                LIMIT %s
                """
                cursor.execute(query, (category, category, limit))
            else:
                query = """
                SELECT author as journalist_name, 
                       (SELECT unnest(categories) LIMIT 1) as category,
                       COUNT(*) as article_count, 
                       MAX(created_at) as updated_at
                FROM news_articles
                WHERE author IS NOT NULL
                GROUP BY author
                ORDER BY article_count DESC, author
                LIMIT %s
                """
                cursor.execute(query, (limit,))
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"기자 통계 조회 실패: {e}")
            return []
        finally:
            self.return_connection(connection)
    
    def get_journalist_stats_by_journalist(self, journalist_name: str) -> List[Dict[str, Any]]:
        """특정 기자의 카테고리별 통계 조회 (news_articles 테이블 기반)"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            query = """
            SELECT author as journalist_name, 
                   unnest(categories) as category,
                   COUNT(*) as article_count, 
                   MAX(created_at) as updated_at
            FROM news_articles
            WHERE author = %s
            GROUP BY author, unnest(categories)
            ORDER BY article_count DESC
            """
            
            cursor.execute(query, (journalist_name,))
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"기자별 통계 조회 실패: {e}")
            return []
        finally:
            self.return_connection(connection)
    
    def get_category_distribution(self) -> Dict[str, Any]:
        """카테고리별 전체 분포도 조회 (news_articles 테이블 기반)"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            
            # 카테고리별 총 기사 수
            cursor.execute("""
                SELECT unnest(categories) as category, 
                       COUNT(*) as total_articles, 
                       COUNT(DISTINCT author) as journalist_count
                FROM news_articles
                WHERE author IS NOT NULL
                GROUP BY unnest(categories)
                ORDER BY total_articles DESC
            """)
            category_stats = cursor.fetchall()
            
            # 카테고리별 평균 기사 수
            cursor.execute("""
                SELECT unnest(categories) as category, 
                       AVG(article_count) as avg_articles
                FROM (
                    SELECT unnest(categories) as category, author, COUNT(*) as article_count
                    FROM news_articles
                    WHERE author IS NOT NULL
                    GROUP BY unnest(categories), author
                ) sub
                GROUP BY category
                ORDER BY avg_articles DESC
            """)
            category_avg = cursor.fetchall()
            
            # 전체 통계
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT author) as total_journalists,
                    COUNT(DISTINCT unnest(categories)) as total_categories,
                    SUM(article_count) as total_articles,
                    AVG(article_count) as overall_avg_articles
                FROM journalist_category_stats
            """)
            overall_stats = cursor.fetchone()
            
            return {
                'category_stats': [{'category': row[0], 'total_articles': row[1], 'journalist_count': row[2]} for row in category_stats],
                'category_averages': [{'category': row[0], 'avg_articles': float(row[1])} for row in category_avg],
                'overall_stats': {
                    'total_journalists': overall_stats[0],
                    'total_categories': overall_stats[1],
                    'total_articles': overall_stats[2],
                    'overall_avg_articles': float(overall_stats[3]) if overall_stats[3] else 0
                }
            }
            
        except Exception as e:
            logger.error(f"카테고리 분포도 조회 실패: {e}")
            return {}
        finally:
            self.return_connection(connection)
    
    def get_top_journalists(self, limit: int = 10, category: str = None) -> List[Dict[str, Any]]:
        """가장 활발한 기자들 조회"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor(cursor_factory=RealDictCursor)
            
            if category:
                query = """
                SELECT journalist_name, category, article_count, updated_at
                FROM journalist_category_stats
                WHERE category = %s
                ORDER BY article_count DESC
                LIMIT %s
                """
                cursor.execute(query, (category, limit))
            else:
                query = """
                SELECT journalist_name, SUM(article_count) as total_articles, 
                       COUNT(DISTINCT category) as category_count, MAX(updated_at) as last_updated
                FROM journalist_category_stats
                GROUP BY journalist_name
                ORDER BY total_articles DESC
                LIMIT %s
                """
                cursor.execute(query, (limit,))
            
            results = cursor.fetchall()
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"상위 기자 조회 실패: {e}")
            return []
        finally:
            self.return_connection(connection)
    


    def analyze_sisaon_journalists(self) -> Dict[str, Any]:
        """시사온 신문사 기자 분석"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            
            # 시사온/시사오늘 기자들의 기사 데이터 조회
            cursor.execute("""
                SELECT author, categories
                FROM news_articles
                WHERE (source = '시사온' OR source = '시사오늘') AND author IS NOT NULL AND categories IS NOT NULL
            """)
            articles = cursor.fetchall()
            
            # 기자별 카테고리 통계 계산
            journalist_stats = {}
            for author, categories in articles:
                if not author or not categories:
                    continue
                
                if author not in journalist_stats:
                    journalist_stats[author] = {}
                
                for category in categories:
                    if category not in journalist_stats[author]:
                        journalist_stats[author][category] = 0
                    journalist_stats[author][category] += 1
            
            # 데이터베이스에 통계 저장
            for journalist, categories in journalist_stats.items():
                for category, count in categories.items():
                    self.update_journalist_category_stats(journalist, category, count)
            
            return {
                'analyzed_journalists': len(journalist_stats),
                'total_articles_analyzed': len(articles),
                'journalist_stats': journalist_stats
            }
            
        except Exception as e:
            logger.error(f"시사온 기자 분석 실패: {e}")
            return {}
        finally:
            self.return_connection(connection)

    def update_journalist_category_stats(self, journalist_name: str, category: str, increment: int = 1, article_data: Dict[str, Any] = None) -> bool:
        """기자 카테고리 통계 업데이트 (journalist_category_stats 테이블 기반)"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            
            # 기자 카테고리 통계 존재 여부 확인
            cursor.execute("SELECT id, article_count, last_article_date FROM journalist_category_stats WHERE journalist_name = %s AND category = %s", (journalist_name, category))
            existing_stat = cursor.fetchone()
            
            if existing_stat:
                # 기존 통계 업데이트
                stat_id, article_count, last_article_date = existing_stat
                new_article_count = article_count + increment
                
                # 마지막 기사 날짜 업데이트
                if article_data and article_data.get('published_date'):
                    new_last_article_date = article_data['published_date']
                else:
                    new_last_article_date = datetime.now() # 기사 데이터가 없으면 현재 시간
                
                cursor.execute("""
                    UPDATE journalist_category_stats 
                    SET article_count = %s,
                        last_article_date = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (new_article_count, new_last_article_date, stat_id))
            else:
                # 새로운 통계 추가
                if article_data:
                    new_article_count = increment
                    new_last_article_date = article_data['published_date']
                else:
                    new_article_count = increment
                    new_last_article_date = datetime.now()
                
                cursor.execute("""
                    INSERT INTO journalist_category_stats (journalist_name, category, article_count, last_article_date)
                    VALUES (%s, %s, %s, %s)
                """, (journalist_name, category, new_article_count, new_last_article_date))
            
            connection.commit()
            logger.info(f"기자 카테고리 통계 업데이트: {journalist_name} - {category} (+{increment})")
            return True
            
        except Exception as e:
            connection.rollback()
            logger.error(f"기자 카테고리 통계 업데이트 실패: {e}")
            return False
        finally:
            self.return_connection(connection)

# 전역 데이터베이스 매니저 인스턴스
db_manager = DatabaseManager() 