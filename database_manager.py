#!/usr/bin/env python3
"""
PostgreSQL 데이터베이스 연결 및 관리 모듈
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
import os
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import json

logger = logging.getLogger(__name__)

class DatabaseManager:
    """PostgreSQL 데이터베이스 관리 클래스"""
    
    def __init__(self):
        self.connection_pool = None
        self.connection_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'newspaper_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', '')
        }
    
    def initialize_pool(self, min_connections=1, max_connections=10):
        """연결 풀 초기화"""
        try:
            self.connection_pool = SimpleConnectionPool(
                min_connections, 
                max_connections, 
                **self.connection_params
            )
            logger.info("데이터베이스 연결 풀이 초기화되었습니다.")
            return True
        except Exception as e:
            logger.error(f"연결 풀 초기화 실패: {e}")
            return False
    
    def get_connection(self):
        """연결 풀에서 연결 가져오기"""
        if not self.connection_pool:
            self.initialize_pool()
        return self.connection_pool.getconn()
    
    def return_connection(self, connection):
        """연결을 풀로 반환"""
        if self.connection_pool:
            self.connection_pool.putconn(connection)
    
    def close_pool(self):
        """연결 풀 종료"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("데이터베이스 연결 풀이 종료되었습니다.")
    
    def create_tables(self):
        """필요한 테이블들 생성"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            
            # 뉴스 기사 테이블
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            
            # 크롤링 로그 테이블
            create_crawling_logs_table = """
            CREATE TABLE IF NOT EXISTS crawling_logs (
                id SERIAL PRIMARY KEY,
                job_type VARCHAR(50) NOT NULL,
                source_key VARCHAR(100),
                status VARCHAR(20) NOT NULL,
                articles_count INTEGER DEFAULT 0,
                duration_seconds DECIMAL(10,2),
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_crawling_logs_table)
            
            connection.commit()
            logger.info("데이터베이스 테이블이 생성되었습니다.")
            return True
            
        except Exception as e:
            connection.rollback()
            logger.error(f"테이블 생성 실패: {e}")
            return False
        finally:
            self.return_connection(connection)
    
    def save_article(self, article_data: Dict[str, Any]) -> bool:
        """뉴스 기사를 데이터베이스에 저장"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            
            # 중복 체크
            cursor.execute("SELECT id FROM news_articles WHERE url = %s", (article_data['url'],))
            if cursor.fetchone():
                logger.info(f"기사가 이미 존재합니다: {article_data['title']}")
                return False
            
            # 기사 저장
            insert_sql = """
            INSERT INTO news_articles (
                title, content, url, source, author, published_date, 
                categories, tags, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                json.dumps(article_data.get('metadata', {}))
            ))
            
            connection.commit()
            logger.info(f"기사 저장 완료: {article_data.get('title')}")
            return True
            
        except Exception as e:
            connection.rollback()
            logger.error(f"기사 저장 실패: {e}")
            return False
        finally:
            self.return_connection(connection)
    
    def save_articles_batch(self, articles: List[Dict[str, Any]]) -> int:
        """여러 기사를 일괄 저장"""
        if not articles:
            return 0
        
        connection = self.get_connection()
        saved_count = 0
        
        try:
            cursor = connection.cursor()
            
            for article in articles:
                try:
                    # 중복 체크
                    cursor.execute("SELECT id FROM news_articles WHERE url = %s", (article['url'],))
                    if cursor.fetchone():
                        continue
                    
                    # 기사 저장
                    insert_sql = """
                    INSERT INTO news_articles (
                        title, content, url, source, author, published_date, 
                        categories, tags, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        json.dumps(article.get('metadata', {}))
                    ))
                    
                    saved_count += 1
                    
                except Exception as e:
                    logger.error(f"개별 기사 저장 실패: {e}")
                    continue
            
            connection.commit()
            logger.info(f"일괄 저장 완료: {saved_count}/{len(articles)} 기사")
            return saved_count
            
        except Exception as e:
            connection.rollback()
            logger.error(f"일괄 저장 실패: {e}")
            return 0
        finally:
            self.return_connection(connection)
    
    def log_crawling_job(self, job_type: str, source_key: str, status: str, 
                        articles_count: int = 0, duration_seconds: float = 0, 
                        error_message: str = None):
        """크롤링 작업 로그 저장"""
        connection = self.get_connection()
        try:
            cursor = connection.cursor()
            
            insert_sql = """
            INSERT INTO crawling_logs (
                job_type, source_key, status, articles_count, 
                duration_seconds, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (
                job_type, source_key, status, articles_count, 
                duration_seconds, error_message
            ))
            
            connection.commit()
            logger.info(f"크롤링 로그 저장: {job_type} - {status}")
            
        except Exception as e:
            connection.rollback()
            logger.error(f"크롤링 로그 저장 실패: {e}")
        finally:
            self.return_connection(connection)
    
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

# 전역 데이터베이스 매니저 인스턴스
db_manager = DatabaseManager() 