"""전체 파이프라인 테스트 스크립트 (DB 저장 없이).

챕터 감지 → 문단 분할 → 아이디어 추출까지 테스트.
결과를 JSON 파일로 저장하여 검수 가능.

--semantic 플래그로 하이브리드 청킹 방식 테스트 가능.
"""

import sys
import os
import json
from datetime import datetime

import fitz  # PyMuPDF

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pdf.parser import extract_full_text, extract_all_pages
from src.pdf.chapter_detector import ChapterDetector
from src.pdf.chunker import split_chapter_into_paragraphs
from src.pdf.text_normalizer import TextNormalizer
from src.model.schemas import ParagraphChunk
from src.workflow.state import State
from src.workflow.nodes import extract_core_idea
from src.pdf.semantic_chunker import (
    hybrid_chunk_and_extract,
    get_semantic_chunking_stats,
)


def extract_chapter_text_from_pdf(pdf_path: str, start_page: int, end_page: int) -> str:
    """
    PDF에서 페이지 범위로 텍스트 추출 후 정규화.

    페이지별 문자 위치 계산의 불일치 문제를 해결하기 위해
    각 챕터마다 해당 페이지 범위의 텍스트를 직접 추출하고 정규화.

    Args:
        pdf_path: PDF 파일 경로
        start_page: 시작 페이지 (0-indexed)
        end_page: 끝 페이지 (0-indexed, inclusive)

    Returns:
        정규화된 챕터 텍스트
    """
    doc = fitz.open(pdf_path)
    try:
        pages = []
        for page_num in range(start_page, min(end_page + 1, len(doc))):
            pages.append(doc[page_num].get_text())

        normalizer = TextNormalizer()
        return normalizer.normalize_full_text(pages)
    finally:
        doc.close()


def test_full_pipeline(
    pdf_path: str,
    max_chapters: int = 3,
    max_paragraphs: int = 5,
    output_path: str = None,
    use_semantic: bool = False
):
    """
    전체 파이프라인 테스트.

    Args:
        pdf_path: PDF 파일 경로
        max_chapters: 테스트할 최대 챕터 수
        max_paragraphs: 챕터당 최대 문단 수
        output_path: 결과 저장 경로 (기본: ./output/pipeline_result_YYYYMMDD_HHMMSS.json)
        use_semantic: 하이브리드 의미적 청킹 사용 여부
    """
    # 결과 저장 구조
    result = {
        "meta": {
            "pdf_file": os.path.basename(pdf_path),
            "pdf_path": pdf_path,
            "test_time": datetime.now().isoformat(),
            "max_chapters": max_chapters,
            "max_paragraphs_per_chapter": max_paragraphs,
            "chunking_method": "semantic" if use_semantic else "rule_based",
        },
        "extraction": {},
        "chapters": [],
        "stats": {},
    }

    print("=" * 70)
    print(f"전체 파이프라인 테스트: {os.path.basename(pdf_path)}")
    print(f"청킹 방식: {'하이브리드 의미적 청킹' if use_semantic else '규칙 기반 청킹'}")
    print("=" * 70)

    # Phase 1: 전체 텍스트 추출
    print("\n[Phase 1] 전체 텍스트 추출...")
    pages = extract_all_pages(pdf_path)
    normalizer = TextNormalizer()
    full_text = normalizer.normalize_full_text(pages)
    print(f"  총 페이지: {len(pages)}")
    print(f"  총 문자 수: {len(full_text):,}")

    result["extraction"] = {
        "total_pages": len(pages),
        "total_characters": len(full_text),
    }

    # Phase 2: 챕터 감지
    print("\n[Phase 2] 챕터 감지...")
    detector = ChapterDetector(pdf_path)
    chapters = detector.detect_chapters()
    print(f"  감지된 챕터: {len(chapters)}개")
    print(f"  감지 방법: {chapters[0].detection_method if chapters else 'none'}")

    result["extraction"]["detected_chapters"] = len(chapters)
    result["extraction"]["detection_method"] = chapters[0].detection_method if chapters else 'none'

    # Phase 3: 챕터별 문단 분할 및 아이디어 추출
    print(f"\n[Phase 3] 챕터별 처리 (최대 {max_chapters}개 챕터)...")

    stats = {
        'chapters_processed': 0,
        'total_paragraphs': 0,
        'total_ideas': 0,
        'failed_extractions': 0,
    }

    # 실제 내용이 있는 챕터만 필터링 (Cover, TOC 등 제외)
    content_chapters = [
        ch for ch in chapters
        if ch.level == 1 and 'Chapter' in ch.title
    ]

    if not content_chapters:
        # Chapter가 없으면 level 1 중 페이지 범위가 큰 것들 선택
        content_chapters = [
            ch for ch in chapters
            if ch.level == 1 and (ch.end_page - ch.start_page) > 5
        ]

    if not content_chapters:
        content_chapters = chapters[:max_chapters]

    print(f"  내용 챕터: {len(content_chapters)}개 중 {min(max_chapters, len(content_chapters))}개 처리")

    for chapter in content_chapters[:max_chapters]:
        print(f"\n{'─' * 60}")
        print(f"📖 챕터: {chapter.title}")
        print(f"   페이지: {chapter.start_page + 1} - {chapter.end_page + 1}")
        print(f"{'─' * 60}")

        # 챕터 결과 구조
        chapter_result = {
            "title": chapter.title,
            "start_page": chapter.start_page + 1,
            "end_page": chapter.end_page + 1,
            "level": chapter.level,
            "detection_method": chapter.detection_method,
            "confidence": chapter.confidence,
            "paragraphs": [],
        }

        # 챕터 텍스트 추출 (PDF에서 직접 페이지 범위로 추출)
        chapter_text = extract_chapter_text_from_pdf(
            pdf_path, chapter.start_page, chapter.end_page
        )
        chapter_result["text_length"] = len(chapter_text)

        if len(chapter_text.strip()) < 100:
            print("  ⏭️  내용이 너무 짧음, 건너뜀")
            chapter_result["skipped"] = True
            chapter_result["skip_reason"] = "텍스트 100자 미만"
            result["chapters"].append(chapter_result)
            continue

        if use_semantic:
            # ===== 하이브리드 의미적 청킹 =====
            # LLM이 문단 분할 + 아이디어 추출을 동시에 수행
            chunk_idea_pairs = hybrid_chunk_and_extract(
                chapter_text=chapter_text,
                chapter_id=None,
                chapter_title=chapter.title,
                base_paragraph_index=stats['total_paragraphs'],
            )

            semantic_stats = get_semantic_chunking_stats(chunk_idea_pairs)
            print(f"  📝 의미적 분할 문단: {semantic_stats['total_paragraphs']}개")
            print(f"     - 개념 추출됨: {semantic_stats['paragraphs_with_concept']}개")
            print(f"     - 고유 개념: {semantic_stats['unique_concepts']}개")
            print(f"     - 평균 문단 길이: {semantic_stats['avg_paragraph_length']:.0f}자")

            chapter_result["total_paragraphs"] = semantic_stats['total_paragraphs']
            chapter_result["semantic_stats"] = semantic_stats
            stats['total_paragraphs'] += semantic_stats['total_paragraphs']

            # 문단별 결과 (최대 N개)
            for i, (chunk, concept) in enumerate(chunk_idea_pairs[:max_paragraphs]):
                print(f"\n  ── 문단 {i+1}/{min(len(chunk_idea_pairs), max_paragraphs)} ──")
                print(f"  section_id: {chunk.section_id}, section_title: {chunk.section_title}")
                print(f"  텍스트 ({len(chunk.text)}자):")

                preview = chunk.text[:200].replace('\n', ' ')
                print(f"    \"{preview}...\"")
                print(f"  ✅ 추출된 아이디어: {concept if concept else '(없음)'}")

                para_result = {
                    "paragraph_index": chunk.paragraph_index,
                    "chapter_paragraph_index": chunk.chapter_paragraph_index,
                    "section_id": chunk.section_id,
                    "section_title": chunk.section_title,
                    "text": chunk.text,
                    "text_length": len(chunk.text),
                    "start_char": chunk.start_char,
                    "end_char": chunk.end_char,
                    "idea_extraction": {
                        "status": "success" if concept else "no_idea",
                        "concept": concept,
                    },
                }

                if concept:
                    stats['total_ideas'] += 1

                chapter_result["paragraphs"].append(para_result)

            if len(chunk_idea_pairs) > max_paragraphs:
                print(f"\n  ... 외 {len(chunk_idea_pairs) - max_paragraphs}개 문단 생략")
                chapter_result["paragraphs_omitted"] = len(chunk_idea_pairs) - max_paragraphs

        else:
            # ===== 기존 규칙 기반 청킹 =====
            # 문단 분할
            chunks = split_chapter_into_paragraphs(
                chapter_text=chapter_text,
                chapter_id=None,  # DB 없으므로 None
                chapter_title=chapter.title,
                base_paragraph_index=stats['total_paragraphs'],
            )

            print(f"  📝 분할된 문단: {len(chunks)}개")
            chapter_result["total_paragraphs"] = len(chunks)
            stats['total_paragraphs'] += len(chunks)

            # 문단별 아이디어 추출 (최대 N개)
            for i, chunk in enumerate(chunks[:max_paragraphs]):
                print(f"\n  ── 문단 {i+1}/{min(len(chunks), max_paragraphs)} ──")
                print(f"  section_id: {chunk.section_id}, section_title: {chunk.section_title}")
                print(f"  텍스트 ({len(chunk.text)}자):")

                # 텍스트 미리보기 (처음 200자)
                preview = chunk.text[:200].replace('\n', ' ')
                print(f"    \"{preview}...\"")

                # 문단 결과 구조
                para_result = {
                    "paragraph_index": chunk.paragraph_index,
                    "chapter_paragraph_index": chunk.chapter_paragraph_index,
                    "section_id": chunk.section_id,
                    "section_title": chunk.section_title,
                    "text": chunk.text,
                    "text_length": len(chunk.text),
                    "start_char": chunk.start_char,
                    "end_char": chunk.end_char,
                    "idea_extraction": None,
                }

                # 아이디어 추출
                para_chunk = ParagraphChunk(
                    book_id=1,  # 임시
                    chapter_id=None,
                    paragraph_index=chunk.paragraph_index,
                    chapter_paragraph_index=chunk.chapter_paragraph_index,
                    body_text=chunk.text,
                )

                state = State(
                    chunk=para_chunk,
                    book_id=1,
                    model_version="gemini-2.5-flash",
                )

                try:
                    state = extract_core_idea(state)

                    if state.error:
                        print(f"  ❌ 추출 실패: {state.error}")
                        stats['failed_extractions'] += 1
                        para_result["idea_extraction"] = {
                            "status": "failed",
                            "error": state.error,
                        }
                    elif state.result:  # State.result가 ExtractedIdea임
                        print(f"  ✅ 추출된 아이디어: {state.result.concept}")
                        stats['total_ideas'] += 1
                        para_result["idea_extraction"] = {
                            "status": "success",
                            "concept": state.result.concept,
                        }
                    else:
                        print(f"  ⚠️  아이디어 없음")
                        para_result["idea_extraction"] = {
                            "status": "no_idea",
                        }

                except Exception as e:
                    print(f"  ❌ 오류: {e}")
                    stats['failed_extractions'] += 1
                    para_result["idea_extraction"] = {
                        "status": "error",
                        "error": str(e),
                    }

                chapter_result["paragraphs"].append(para_result)

            if len(chunks) > max_paragraphs:
                print(f"\n  ... 외 {len(chunks) - max_paragraphs}개 문단 생략")
                chapter_result["paragraphs_omitted"] = len(chunks) - max_paragraphs

        result["chapters"].append(chapter_result)
        stats['chapters_processed'] += 1

    # 요약
    result["stats"] = stats

    print("\n" + "=" * 70)
    print("테스트 요약")
    print("=" * 70)
    print(f"  처리된 챕터: {stats['chapters_processed']}")
    print(f"  총 문단: {stats['total_paragraphs']}")
    print(f"  추출된 아이디어: {stats['total_ideas']}")
    print(f"  추출 실패: {stats['failed_extractions']}")
    print("=" * 70)

    # 결과 파일 저장
    if output_path is None:
        os.makedirs("output", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"output/pipeline_result_{timestamp}.json"

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n📁 결과 저장: {output_path}")

    return result


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="PDF 파이프라인 테스트 (챕터 감지 → 문단 분할 → 아이디어 추출)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python test_full_pipeline.py ./AI_Engineering.pdf
  python test_full_pipeline.py ./AI_Engineering.pdf --chapters 2 --paragraphs 3
  python test_full_pipeline.py ./AI_Engineering.pdf --semantic
  python test_full_pipeline.py ./AI_Engineering.pdf --semantic --chapters 1 --paragraphs 5
        """
    )

    parser.add_argument("pdf_path", help="PDF 파일 경로")
    parser.add_argument("--chapters", "-c", type=int, default=3, help="테스트할 최대 챕터 수 (기본: 3)")
    parser.add_argument("--paragraphs", "-p", type=int, default=5, help="챕터당 최대 문단 수 (기본: 5)")
    parser.add_argument("--output", "-o", help="결과 저장 경로")
    parser.add_argument("--semantic", "-s", action="store_true",
                        help="하이브리드 의미적 청킹 사용 (LLM이 문단 분할 + 아이디어 추출 동시 수행)")

    args = parser.parse_args()

    if not os.path.exists(args.pdf_path):
        print(f"오류: 파일을 찾을 수 없습니다: {args.pdf_path}")
        sys.exit(1)

    test_full_pipeline(
        pdf_path=args.pdf_path,
        max_chapters=args.chapters,
        max_paragraphs=args.paragraphs,
        output_path=args.output,
        use_semantic=args.semantic,
    )


if __name__ == "__main__":
    main()
