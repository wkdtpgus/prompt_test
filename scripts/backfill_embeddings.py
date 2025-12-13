#!/usr/bin/env python3
"""기존 paragraph_chunks에 임베딩 생성 및 저장.

사용법:
    python scripts/backfill_embeddings.py
    python scripts/backfill_embeddings.py --book-id 1
    python scripts/backfill_embeddings.py --batch-size 50
    python scripts/backfill_embeddings.py --dry-run  # 비용만 계산
"""

import argparse
import sys
from pathlib import Path
from decimal import Decimal

# 프로젝트 루트를 path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tqdm import tqdm
from sqlalchemy import text

from src.db.connection import get_session
from src.db.models import ParagraphChunk, ParagraphEmbedding
from src.dedup.embedding_utils import compute_embeddings_batch, DEFAULT_MODEL


def estimate_cost(session, book_id: int = None) -> dict:
    """예상 비용 계산.

    Args:
        session: DB 세션
        book_id: 특정 책만

    Returns:
        비용 정보 dict
    """
    # 임베딩 없는 청크 수
    query = session.query(ParagraphChunk).outerjoin(
        ParagraphEmbedding,
        ParagraphChunk.id == ParagraphEmbedding.chunk_id
    ).filter(ParagraphEmbedding.id.is_(None))

    if book_id:
        query = query.filter(ParagraphChunk.book_id == book_id)

    chunks = query.all()
    total_chunks = len(chunks)

    # 대략적인 토큰 수 추정 (1 토큰 ≈ 4 글자)
    total_chars = sum(len(c.body_text) for c in chunks)
    estimated_tokens = total_chars / 4

    # 비용 계산 (text-embedding-3-small: $0.00002 / 1K tokens)
    cost_per_1k = 0.00002
    estimated_cost = (estimated_tokens / 1000) * cost_per_1k

    return {
        'total_chunks': total_chunks,
        'total_chars': total_chars,
        'estimated_tokens': int(estimated_tokens),
        'estimated_cost_usd': estimated_cost,
        'estimated_cost_krw': estimated_cost * 1400  # 대략적인 환율
    }


def backfill_embeddings(
    book_id: int = None,
    batch_size: int = 50,
    dry_run: bool = False
):
    """기존 청크에 임베딩 생성 및 저장.

    Args:
        book_id: 특정 책만 처리 (None이면 전체)
        batch_size: 배치 크기 (OpenAI API 호출당)
        dry_run: True면 비용만 계산하고 실행 안함
    """
    session = get_session()

    try:
        # 비용 추정
        cost_info = estimate_cost(session, book_id)

        print(f"\n=== 임베딩 생성 예상 ===")
        print(f"처리할 청크 수: {cost_info['total_chunks']}")
        print(f"총 문자 수: {cost_info['total_chars']:,}")
        print(f"예상 토큰 수: {cost_info['estimated_tokens']:,}")
        print(f"예상 비용: ${cost_info['estimated_cost_usd']:.4f} (약 {cost_info['estimated_cost_krw']:.0f}원)")

        if dry_run:
            print("\n[Dry Run] 실제 실행하지 않음")
            return

        if cost_info['total_chunks'] == 0:
            print("\n처리할 청크가 없습니다.")
            return

        # 확인
        confirm = input(f"\n{cost_info['total_chunks']}개 청크에 임베딩을 생성하시겠습니까? (y/N): ")
        if confirm.lower() != 'y':
            print("취소됨")
            return

        # 임베딩 없는 청크 조회
        query = session.query(ParagraphChunk).outerjoin(
            ParagraphEmbedding,
            ParagraphChunk.id == ParagraphEmbedding.chunk_id
        ).filter(ParagraphEmbedding.id.is_(None))

        if book_id:
            query = query.filter(ParagraphChunk.book_id == book_id)

        chunks = query.order_by(ParagraphChunk.id).all()

        # 배치 처리
        total_cost = Decimal('0')
        total_tokens = 0

        with tqdm(total=len(chunks), desc="임베딩 생성") as pbar:
            for i in range(0, len(chunks), batch_size):
                batch = chunks[i:i + batch_size]
                texts = [c.body_text for c in batch]

                try:
                    # 배치 임베딩 생성
                    results = compute_embeddings_batch(texts, batch_size=batch_size)

                    # DB에 저장
                    for chunk, result in zip(batch, results):
                        embedding = ParagraphEmbedding(
                            chunk_id=chunk.id,
                            book_id=chunk.book_id,
                            embedding=result.embedding,
                            body_text=chunk.body_text,
                            model=result.model,
                            embedding_cost_cents=Decimal(str(result.cost_cents))
                        )
                        session.add(embedding)
                        total_cost += Decimal(str(result.cost_cents))
                        total_tokens += result.tokens_used

                    session.commit()

                except Exception as e:
                    session.rollback()
                    print(f"\n배치 {i//batch_size + 1} 처리 실패: {e}")
                    continue

                pbar.update(len(batch))

        print(f"\n=== 완료 ===")
        print(f"생성된 임베딩: {len(chunks)}")
        print(f"사용 토큰: {total_tokens:,}")
        print(f"총 비용: ${float(total_cost)/100:.4f}")

        # 최종 통계
        stats = session.execute(text("""
            SELECT
                (SELECT COUNT(*) FROM paragraph_chunks) as total_chunks,
                (SELECT COUNT(*) FROM paragraph_embeddings) as total_embeddings
        """)).first()

        print(f"\n현재 상태:")
        print(f"  전체 청크: {stats[0]}")
        print(f"  임베딩 있음: {stats[1]} ({stats[1]*100//stats[0] if stats[0] else 0}%)")

    except Exception as e:
        session.rollback()
        print(f"오류 발생: {e}")
        raise
    finally:
        session.close()


def main():
    parser = argparse.ArgumentParser(description="기존 청크에 임베딩 생성")
    parser.add_argument("--book-id", type=int, help="특정 책만 처리")
    parser.add_argument("--batch-size", type=int, default=50, help="배치 크기 (기본: 50)")
    parser.add_argument("--dry-run", action="store_true", help="비용만 계산하고 실행 안함")

    args = parser.parse_args()

    backfill_embeddings(
        book_id=args.book_id,
        batch_size=args.batch_size,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    main()
