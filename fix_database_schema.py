#!/usr/bin/env python3
"""
데이터베이스 스키마 수정 스크립트
"""

import os
from database_manager import db_manager

def fix_database_schema():
    """데이터베이스 스키마 수정"""
    print("🔧 데이터베이스 스키마 수정을 시작합니다...")
    
    # 환경변수 설정
    os.environ['DB_HOST'] = 'database-1.c1iymg62ikbr.us-east-2.rds.amazonaws.com'
    os.environ['DB_PORT'] = '5432'
    os.environ['DB_NAME'] = 'postgres'
    os.environ['DB_USER'] = 'EZ0320'
    # os.environ['DB_PASSWORD'] = 'your_password_here'  # 실제 비밀번호로 변경하세요
    
    # 데이터베이스 연결
    if not db_manager.initialize_pool():
        print("❌ 데이터베이스 연결 실패")
        return False
    
    try:
        connection = db_manager.get_connection()
        if not connection:
            print("❌ 데이터베이스 연결을 가져올 수 없습니다.")
            return False
        
        cursor = connection.cursor()
        
        # 1. word_count 컬럼 추가
        print("\n📋 word_count 컬럼 추가 중...")
        try:
            cursor.execute("""
                ALTER TABLE news_articles 
                ADD COLUMN IF NOT EXISTS word_count INTEGER DEFAULT 0
            """)
            print("✅ word_count 컬럼 추가 완료")
        except Exception as e:
            print(f"⚠️  word_count 컬럼 추가 실패 (이미 존재할 수 있음): {e}")
        
        # 2. updated_at 컬럼 추가
        print("\n📋 updated_at 컬럼 추가 중...")
        try:
            cursor.execute("""
                ALTER TABLE news_articles 
                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """)
            print("✅ updated_at 컬럼 추가 완료")
        except Exception as e:
            print(f"⚠️  updated_at 컬럼 추가 실패 (이미 존재할 수 있음): {e}")
        
        # 3. journalist_category_stats 테이블에 last_article_date 컬럼 추가
        print("\n📋 journalist_category_stats 테이블 수정 중...")
        try:
            cursor.execute("""
                ALTER TABLE journalist_category_stats 
                ADD COLUMN IF NOT EXISTS last_article_date TIMESTAMP
            """)
            print("✅ last_article_date 컬럼 추가 완료")
        except Exception as e:
            print(f"⚠️  last_article_date 컬럼 추가 실패 (이미 존재할 수 있음): {e}")
        
        # 4. crawling_logs 테이블에 추가 컬럼들
        print("\n📋 crawling_logs 테이블 수정 중...")
        try:
            cursor.execute("""
                ALTER TABLE crawling_logs 
                ADD COLUMN IF NOT EXISTS duplicates_count INTEGER DEFAULT 0
            """)
            cursor.execute("""
                ALTER TABLE crawling_logs 
                ADD COLUMN IF NOT EXISTS errors_count INTEGER DEFAULT 0
            """)
            print("✅ crawling_logs 테이블 수정 완료")
        except Exception as e:
            print(f"⚠️  crawling_logs 테이블 수정 실패: {e}")
        
        # 5. 인덱스 추가
        print("\n📋 인덱스 추가 중...")
        try:
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_articles_source ON news_articles(source)",
                "CREATE INDEX IF NOT EXISTS idx_articles_author ON news_articles(author)",
                "CREATE INDEX IF NOT EXISTS idx_articles_published_date ON news_articles(published_date DESC)",
                "CREATE INDEX IF NOT EXISTS idx_articles_created_at ON news_articles(created_at DESC)",
                "CREATE INDEX IF NOT EXISTS idx_articles_url_hash ON news_articles USING hash(url)",
                "CREATE INDEX IF NOT EXISTS idx_journalist_stats_updated_at ON journalist_category_stats(updated_at DESC)"
            ]
            
            for index_sql in indexes:
                cursor.execute(index_sql)
            print("✅ 인덱스 추가 완료")
        except Exception as e:
            print(f"⚠️  인덱스 추가 실패: {e}")
        
        connection.commit()
        
        # 스키마 확인
        print("\n📊 현재 테이블 구조 확인:")
        print("-" * 50)
        
        cursor.execute("""
            SELECT column_name, data_type, is_nullable 
            FROM information_schema.columns 
            WHERE table_name = 'news_articles' 
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        print("news_articles 테이블 컬럼:")
        for col_name, data_type, is_nullable in columns:
            print(f"  - {col_name}: {data_type} ({'NULL' if is_nullable == 'YES' else 'NOT NULL'})")
        
        print("\n✅ 데이터베이스 스키마 수정이 완료되었습니다!")
        return True
        
    except Exception as e:
        print(f"❌ 스키마 수정 실패: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            db_manager.return_connection(connection)

def main():
    """메인 함수"""
    print("=" * 60)
    print("🔧 데이터베이스 스키마 수정 도구")
    print("=" * 60)
    
    success = fix_database_schema()
    
    if success:
        print("\n🎉 스키마 수정이 성공적으로 완료되었습니다!")
    else:
        print("\n💥 스키마 수정 중 오류가 발생했습니다.")

if __name__ == "__main__":
    main() 