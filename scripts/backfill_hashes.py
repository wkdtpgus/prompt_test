#!/usr/bin/env python3
"""기존 paragraph_chunks에 해시값 백필.

사용법:
    python scripts/backfill_hashes.py
    python scripts/backfill_hashes.py --book-id 1
    python scripts/backfill_hashes.py --batch-size 500
"""

import argparse
import sys
from pathlib import Path

# 프로젝트 루트를 path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tqdm import tqdm
from sqlalchemy import text

from src.db.connection import get_session
from src.db.models import ParagraphChunk
from src.dedup.hash_utils import compute_paragraph_hash, compute_simhash64


def backfill_hashes(
    book_id: int = None,
    batch_size: int = 100,
    force: bool = False
):
    """기존 청크에 해시값 채우기.

    Args:
        book_id: 특정 책만 처리 (None이면 전체)
        batch_size: 배치 크기
        force: 이미 해시가 있는 것도 다시 계산
    """
    session = get_session()

    try:
        # 처리할 청크 쿼리
        query = session.query(ParagraphChunk)

        if book_id:
            query = query.filter(ParagraphChunk.book_id == book_id)

        if not force:
            # 해시가 없는 것만
            query = query.filter(
                (ParagraphChunk.paragraph_hash.is_(None)) |
                (ParagraphChunk.simhash64.is_(None))
            )

        total = query.count()
        print(f"처리할 청크 수: {total}")

        if total == 0:
            print("처리할 청크가 없습니다.")
            return

        # 배치 처리
        processed = 0
        updated = 0

        with tqdm(total=total, desc="해시 계산") as pbar:
            while True:
                # 매번 처음부터 조회 (commit 후 필터 조건이 바뀌므로)
                batch = query.order_by(ParagraphChunk.id).limit(batch_size).all()

                if not batch:
                    break

                for chunk in batch:
                    try:
                        paragraph_hash = compute_paragraph_hash(chunk.body_text)
                        simhash64 = compute_simhash64(chunk.body_text)

                        chunk.paragraph_hash = paragraph_hash
                        chunk.simhash64 = simhash64
                        updated += 1
                    except Exception as e:
                        print(f"\n청크 {chunk.id} 처리 실패: {e}")

                    processed += 1
                    pbar.update(1)

                session.commit()

        print(f"\n완료: {updated}/{processed} 청크 업데이트됨")

        # 통계 출력
        stats = session.execute(text("""
            SELECT
                COUNT(*) as total,
                COUNT(paragraph_hash) as with_hash,
                COUNT(simhash64) as with_simhash
            FROM paragraph_chunks
        """)).first()

        print(f"\n현재 상태:")
        print(f"  전체 청크: {stats[0]}")
        print(f"  paragraph_hash 있음: {stats[1]} ({stats[1]*100//stats[0] if stats[0] else 0}%)")
        print(f"  simhash64 있음: {stats[2]} ({stats[2]*100//stats[0] if stats[0] else 0}%)")

    except Exception as e:
        session.rollback()
        print(f"오류 발생: {e}")
        raise
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="기존 청크에 해시값 백필")
    parser.add_argument("--book-id", type=int, help="특정 책만 처리")
    parser.add_argument("--batch-size", type=int, default=100, help="배치 크기 (기본: 100)")
    parser.add_argument("--force", action="store_true", help="이미 해시가 있어도 다시 계산")

    args = parser.parse_args()

    backfill_hashes(
        book_id=args.book_id,
        batch_size=args.batch_size,
        force=args.force
    )


if __name__ == "__main__":
    main()
