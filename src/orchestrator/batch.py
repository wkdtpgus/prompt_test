"""Batch PDF processing orchestrator.

챕터 기반 계층적 PDF 처리.
챕터 → 섹션 → 문단 → 아이디어 추출 파이프라인.
"""

import os
from typing import List, Optional
from tqdm import tqdm

from src.pdf.parser import get_pdf_metadata, extract_all_pages
from src.pdf.chunker import split_chapter_into_paragraphs
from src.pdf.chapter_detector import ChapterDetector
from src.pdf.text_normalizer import TextNormalizer
from src.db.connection import get_session
from src.db.models import Book, Chapter
from src.db.operations import (
    create_book,
    get_book_by_title,
    create_chapters_from_detected,
    get_chapters_by_book,
)
from src.db.progress import (
    initialize_chapter_progress,
    get_pending_chapters,
    mark_chapter_processing,
    mark_chapter_completed,
    mark_chapter_failed,
    get_chapter_progress_stats,
    reset_stuck_chapters,
)
from src.model.schemas import ParagraphChunk
from src.workflow.state import State
from src.workflow.nodes import extract_core_idea, save_to_database


def process_pdf(
    pdf_path: str,
    resume: bool = False,
    book_id: Optional[int] = None,
    model_version: str = "gemini-2.5-flash",
) -> dict:
    """
    챕터 기반 계층적 PDF 처리.

    파이프라인:
    1. 전체 텍스트 추출
    2. 챕터 감지 (TOC/패턴 기반)
    3. 챕터별 문단 분할
    4. 아이디어 추출 및 저장

    Args:
        pdf_path: PDF 파일 경로
        resume: 재개 모드 여부
        book_id: 재개 시 책 ID
        model_version: LLM 모델 버전

    Returns:
        처리 통계 딕셔너리
    """
    if not resume and not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF 파일 없음: {pdf_path}")

    session = get_session()

    try:
        # Phase 1: 책 설정
        if resume and book_id:
            book, chapters = _resume_book(session, book_id)
        else:
            book, chapters = _create_book_with_chapters(session, pdf_path)

        # Phase 2: 대기 중인 챕터 확인
        pending_chapters = get_pending_chapters(session, book.id)

        if not pending_chapters:
            print("✅ 모든 챕터가 처리 완료됨!")
            return get_chapter_progress_stats(session, book.id)

        print(f"\n🚀 {len(pending_chapters)}개 챕터 처리 시작...")

        # Phase 3: 전체 텍스트 추출 (1회만)
        print("📄 전체 텍스트 추출 중...")
        pages = extract_all_pages(pdf_path)
        normalizer = TextNormalizer()
        full_text = normalizer.normalize_full_text(pages)
        print(f"   텍스트 길이: {len(full_text):,} 문자")

        # 페이지별 문자 위치 계산
        page_char_positions = _calculate_page_positions(pages)

        # Phase 4: 챕터별 처리
        stats = _process_chapters(
            session=session,
            book=book,
            full_text=full_text,
            page_char_positions=page_char_positions,
            pending_chapters=pending_chapters,
            model_version=model_version,
        )

        # Phase 5: 요약 출력
        _print_summary(stats)

        return stats

    finally:
        session.close()


def _resume_book(session, book_id: int) -> tuple:
    """책 재개."""
    print(f"📖 책 재개 (ID: {book_id})")

    book = session.query(Book).filter_by(id=book_id).first()
    if not book:
        raise ValueError(f"책 ID {book_id} 없음")

    chapters = get_chapters_by_book(session, book_id)
    if not chapters:
        raise ValueError(f"책 ID {book_id}에 챕터 없음")

    # 멈춘 챕터 리셋
    stuck_count = reset_stuck_chapters(session, book_id)
    if stuck_count > 0:
        print(f"⚠️  {stuck_count}개 멈춘 챕터 리셋")

    return book, chapters


def _create_book_with_chapters(session, pdf_path: str) -> tuple:
    """책 생성 및 챕터 감지."""
    # 메타데이터 추출
    metadata = get_pdf_metadata(pdf_path)
    print(f"📖 처리 시작: {metadata['title']}")
    print(f"   저자: {metadata['author']}")
    print(f"   페이지: {metadata['total_pages']}")

    # 중복 확인
    existing = get_book_by_title(session, metadata['title'])
    if existing:
        print(f"⚠️  '{metadata['title']}' 이미 존재 (ID: {existing.id})")
        print(f"   재개: --resume --book-id {existing.id}")
        raise ValueError("책이 이미 존재함")

    # 책 생성
    book = create_book(
        session,
        title=metadata['title'],
        author=metadata['author'],
        source_path=pdf_path,
    )
    print(f"✅ 책 생성 완료 (ID: {book.id})")

    # 챕터 감지
    print("🔍 챕터 감지 중...")
    detector = ChapterDetector(pdf_path)
    detected_chapters = detector.detect_chapters()
    print(f"   {len(detected_chapters)}개 챕터 감지됨")

    for ch in detected_chapters[:5]:  # 처음 5개만 출력
        print(f"     - {ch.title} (p.{ch.start_page+1}-{ch.end_page+1})")
    if len(detected_chapters) > 5:
        print(f"     ... 외 {len(detected_chapters)-5}개")

    # DB에 챕터 저장
    chapters = create_chapters_from_detected(session, book.id, detected_chapters)

    # 진행 추적 초기화
    initialize_chapter_progress(session, book.id, chapters)

    return book, chapters


def _calculate_page_positions(pages: List[str]) -> List[tuple]:
    """페이지별 문자 위치 계산."""
    positions = []
    char_offset = 0

    for page_num, page_text in enumerate(pages):
        start = char_offset
        char_offset += len(page_text) + 1  # +1 for join character
        positions.append((page_num, start, char_offset))

    return positions


def _get_chapter_text(
    full_text: str,
    page_positions: List[tuple],
    chapter: Chapter
) -> str:
    """챕터 텍스트 추출."""
    # 시작/끝 페이지의 문자 위치 찾기
    start_char = 0
    end_char = len(full_text)

    for page_num, start, end in page_positions:
        if page_num == chapter.start_page:
            start_char = start
        if page_num == chapter.end_page:
            end_char = end
            break

    return full_text[start_char:end_char]


def _process_chapters(
    session,
    book: Book,
    full_text: str,
    page_char_positions: List[tuple],
    pending_chapters: List[Chapter],
    model_version: str,
) -> dict:
    """챕터별 처리 실행."""
    stats = {
        'total_chapters': len(pending_chapters),
        'completed': 0,
        'failed': 0,
        'total_paragraphs': 0,
        'total_ideas': 0,
    }

    global_para_idx = 0

    for chapter in tqdm(pending_chapters, desc="챕터 처리"):
        try:
            mark_chapter_processing(session, book.id, chapter.id)

            # 챕터 텍스트 추출
            chapter_text = _get_chapter_text(
                full_text, page_char_positions, chapter
            )

            if len(chapter_text.strip()) < 100:
                # 내용이 너무 짧음
                mark_chapter_completed(session, book.id, chapter.id)
                stats['completed'] += 1
                continue

            # 계층적 문단 분할
            chunks = split_chapter_into_paragraphs(
                chapter_text=chapter_text,
                chapter_id=chapter.id,
                chapter_title=chapter.title,
                base_paragraph_index=global_para_idx,
            )

            stats['total_paragraphs'] += len(chunks)

            # 각 문단 처리
            for chunk in chunks:
                # ParagraphChunk 스키마로 변환
                para_chunk = ParagraphChunk(
                    book_id=book.id,
                    chapter_id=chunk.chapter_id,
                    paragraph_index=chunk.paragraph_index,
                    chapter_paragraph_index=chunk.chapter_paragraph_index,
                    body_text=chunk.text,
                    section_id=chunk.section_id,
                )

                state = State(
                    chunk=para_chunk,
                    book_id=book.id,
                    model_version=model_version,
                )

                # 아이디어 추출
                state = extract_core_idea(state)

                if state.error:
                    continue

                # DB 저장
                state = save_to_database(state)

                if not state.error:
                    stats['total_ideas'] += 1

            global_para_idx += len(chunks)
            mark_chapter_completed(session, book.id, chapter.id)
            stats['completed'] += 1

        except Exception as e:
            mark_chapter_failed(session, book.id, chapter.id, str(e))
            stats['failed'] += 1
            print(f"\n❌ 챕터 '{chapter.title}' 실패: {e}")

    return stats


def _print_summary(stats: dict) -> None:
    """처리 요약 출력."""
    print("\n" + "=" * 60)
    print("처리 요약")
    print("=" * 60)
    print(f"총 챕터: {stats['total_chapters']}")
    print(f"완료: {stats['completed']}")
    print(f"실패: {stats['failed']}")
    print(f"총 문단: {stats['total_paragraphs']}")
    print(f"추출된 아이디어: {stats['total_ideas']}")
    print("=" * 60)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="PDF 처리 (챕터 기반)")
    parser.add_argument("--pdf", type=str, required=True, help="PDF 파일 경로")
    parser.add_argument("--resume", action="store_true", help="중단된 처리 재개")
    parser.add_argument("--book-id", type=int, help="재개 시 책 ID")
    parser.add_argument("--model", type=str, default="gemini-2.5-flash", help="모델 버전")

    args = parser.parse_args()

    process_pdf(
        pdf_path=args.pdf,
        resume=args.resume,
        book_id=args.book_id,
        model_version=args.model,
    )
