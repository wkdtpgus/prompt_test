"""
챕터 기반 파이프라인 마이그레이션 스크립트.

새로운 테이블 생성 및 기존 데이터 마이그레이션.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from src.db.connection import get_session, create_db_engine
from src.db.models import Base, Book, Chapter, ParagraphChunk, ProcessingProgress


def create_new_tables():
    """새로운 테이블 생성 (chapters) 및 기존 테이블 확장."""
    print("=" * 60)
    print("Phase 1: 테이블 스키마 업데이트")
    print("=" * 60)

    engine = create_db_engine()

    # SQLAlchemy로 새 테이블 생성
    # 기존 테이블은 유지하고 새 테이블만 생성
    Base.metadata.create_all(bind=engine)
    print("✅ 테이블 스키마 업데이트 완료")

    # 새 컬럼 추가 (ALTER TABLE)
    session = get_session()
    try:
        # paragraph_chunks 테이블에 새 컬럼 추가
        _add_column_if_not_exists(
            session,
            "paragraph_chunks",
            "chapter_id",
            "INTEGER REFERENCES chapters(id)"
        )
        _add_column_if_not_exists(
            session,
            "paragraph_chunks",
            "chapter_paragraph_index",
            "INTEGER"
        )
        _add_column_if_not_exists(
            session,
            "paragraph_chunks",
            "section_path",
            "TEXT"
        )

        # processing_progress 테이블에 새 컬럼 추가
        _add_column_if_not_exists(
            session,
            "processing_progress",
            "chapter_id",
            "INTEGER REFERENCES chapters(id)"
        )
        _add_column_if_not_exists(
            session,
            "processing_progress",
            "processing_unit",
            "VARCHAR(50) DEFAULT 'page'"
        )

        session.commit()
        print("✅ 새 컬럼 추가 완료")

    except Exception as e:
        print(f"❌ 컬럼 추가 실패: {e}")
        session.rollback()
    finally:
        session.close()


def _add_column_if_not_exists(session, table_name: str, column_name: str, column_def: str):
    """컬럼이 없으면 추가."""
    # 컬럼 존재 여부 확인
    check_sql = text(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}' AND column_name = '{column_name}'
    """)
    result = session.execute(check_sql).fetchone()

    if not result:
        alter_sql = text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")
        session.execute(alter_sql)
        print(f"  ✅ {table_name}.{column_name} 컬럼 추가")
    else:
        print(f"  ⏭️  {table_name}.{column_name} 컬럼 이미 존재")


def migrate_existing_data():
    """기존 데이터 마이그레이션.

    챕터가 없는 책에 대해 "Full Book (Legacy)" 챕터 생성.
    """
    print("\n" + "=" * 60)
    print("Phase 2: 기존 데이터 마이그레이션")
    print("=" * 60)

    session = get_session()
    stats = {
        'books_migrated': 0,
        'chapters_created': 0,
        'chunks_updated': 0,
    }

    try:
        # 챕터가 없는 책 찾기
        books = session.query(Book).all()
        print(f"📚 총 {len(books)}개 책 확인 중...")

        for book in books:
            # 이 책에 챕터가 있는지 확인
            existing_chapters = (
                session.query(Chapter)
                .filter_by(book_id=book.id)
                .count()
            )

            if existing_chapters > 0:
                print(f"  ⏭️  '{book.title}' - 이미 {existing_chapters}개 챕터 존재")
                continue

            # 기존 청크 수 확인
            chunk_count = (
                session.query(ParagraphChunk)
                .filter_by(book_id=book.id)
                .count()
            )

            if chunk_count == 0:
                print(f"  ⏭️  '{book.title}' - 청크 없음")
                continue

            # 최대 페이지 번호 찾기
            max_page_result = (
                session.query(ParagraphChunk.page_number)
                .filter_by(book_id=book.id)
                .order_by(ParagraphChunk.page_number.desc())
                .first()
            )
            max_page = max_page_result[0] if max_page_result else 0

            # Legacy 챕터 생성
            legacy_chapter = Chapter(
                book_id=book.id,
                chapter_number=1,
                title="Full Book (Legacy)",
                start_page=0,
                end_page=max_page,
                level=1,
                detection_method='migration',
            )
            session.add(legacy_chapter)
            session.flush()  # ID 할당

            # 기존 청크들에 chapter_id 설정
            session.query(ParagraphChunk).filter_by(book_id=book.id).update(
                {'chapter_id': legacy_chapter.id},
                synchronize_session=False
            )

            stats['books_migrated'] += 1
            stats['chapters_created'] += 1
            stats['chunks_updated'] += chunk_count

            print(f"  ✅ '{book.title}' - Legacy 챕터 생성 ({chunk_count}개 청크 연결)")

        session.commit()

        print("\n" + "-" * 40)
        print("마이그레이션 결과:")
        print(f"  - 마이그레이션된 책: {stats['books_migrated']}")
        print(f"  - 생성된 챕터: {stats['chapters_created']}")
        print(f"  - 업데이트된 청크: {stats['chunks_updated']}")
        print("-" * 40)

    except Exception as e:
        print(f"❌ 마이그레이션 실패: {e}")
        session.rollback()
        raise
    finally:
        session.close()

    return stats


def verify_migration():
    """마이그레이션 검증."""
    print("\n" + "=" * 60)
    print("Phase 3: 마이그레이션 검증")
    print("=" * 60)

    session = get_session()

    try:
        # 챕터 테이블 확인
        chapter_count = session.query(Chapter).count()
        print(f"  chapters 테이블: {chapter_count}개 레코드")

        # 책별 챕터 확인
        books = session.query(Book).all()
        for book in books:
            chapters = session.query(Chapter).filter_by(book_id=book.id).count()
            chunks_with_chapter = (
                session.query(ParagraphChunk)
                .filter_by(book_id=book.id)
                .filter(ParagraphChunk.chapter_id.isnot(None))
                .count()
            )
            chunks_total = session.query(ParagraphChunk).filter_by(book_id=book.id).count()

            print(f"  📖 '{book.title}':")
            print(f"      - 챕터: {chapters}개")
            print(f"      - 청크: {chunks_with_chapter}/{chunks_total} (chapter_id 연결됨)")

        print("\n✅ 검증 완료")

    finally:
        session.close()


def main():
    print("\n" + "=" * 60)
    print("챕터 기반 파이프라인 마이그레이션")
    print("=" * 60 + "\n")

    # 1. 테이블 스키마 업데이트
    create_new_tables()

    # 2. 기존 데이터 마이그레이션
    migrate_existing_data()

    # 3. 검증
    verify_migration()

    print("\n" + "=" * 60)
    print("마이그레이션 완료!")
    print("=" * 60)
    print("\n처리 방법:")
    print("  python -m src.orchestrator.batch --pdf <파일>")
    print("  python scripts/process_pdfs.py --pdf <파일>")


if __name__ == "__main__":
    main()
