#!/usr/bin/env python3
"""
journalists 테이블을 새로운 구조로 재생성
"""

import os
import psycopg2

# 환경변수 설정
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_PORT'] = '5432'
os.environ['DB_NAME'] = 'postgres'
os.environ['DB_USER'] = 'postgres'
# os.environ['DB_PASSWORD'] = 'your_password_here'  # 실제 비밀번호로 변경하세요

def recreate_journalists_table():
    """journalists 테이블 재생성"""
    connection_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', '5432'),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', '')
    }
    
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        print("🔄 journalists 테이블 재생성 시작")
        print("=" * 40)
        
        # 1. 기존 테이블 삭제
        print("\n🗑️ 기존 journalists 테이블 삭제 중...")
        cursor.execute("DROP TABLE IF EXISTS journalists CASCADE")
        print("  ✅ 기존 테이블 삭제 완료")
        
        # 2. 새로운 테이블 생성
        print("\n📋 새로운 journalists 테이블 생성 중...")
        create_journalists_table = """
        CREATE TABLE journalists (
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
        
        # 3. 인덱스 생성
        print("\n🔗 인덱스 생성 중...")
        create_indexes = """
        CREATE INDEX idx_journalists_name ON journalists(name);
        CREATE INDEX idx_journalists_source ON journalists(source);
        CREATE INDEX idx_journalists_total_articles ON journalists(total_articles DESC);
        CREATE INDEX idx_journalists_last_article_date ON journalists(last_article_date DESC);
        """
        cursor.execute(create_indexes)
        
        conn.commit()
        
        # 4. 테이블 구조 확인
        print("\n🔍 새로운 테이블 구조 확인:")
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'journalists'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        for col in columns:
            nullable = "NULL" if col[2] == "YES" else "NOT NULL"
            print(f"  - {col[0]}: {col[1]} {nullable}")
        
        conn.close()
        print(f"\n✅ journalists 테이블 재생성 완료")
        
    except Exception as e:
        print(f"❌ 오류: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()

if __name__ == "__main__":
    recreate_journalists_table() 