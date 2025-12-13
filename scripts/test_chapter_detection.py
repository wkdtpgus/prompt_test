"""챕터 감지 테스트 스크립트.

DB 저장 없이 챕터 감지 결과만 확인.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.pdf.parser import extract_full_text, extract_toc
from src.pdf.chapter_detector import ChapterDetector
from src.pdf.text_normalizer import TextNormalizer


def test_chapter_detection(pdf_path: str):
    """PDF 파일의 챕터 감지 테스트."""
    print("=" * 60)
    print(f"챕터 감지 테스트: {os.path.basename(pdf_path)}")
    print("=" * 60)

    # 1. TOC 추출
    print("\n[1] TOC 추출...")
    toc = extract_toc(pdf_path)
    if toc:
        print(f"  TOC 항목: {len(toc)}개")
        for i, entry in enumerate(toc[:10]):  # 처음 10개만
            level = entry['level']
            title = entry['title']
            page = entry['page']
            print(f"    {'  ' * (level-1)}[{level}] {title} (p.{page})")
        if len(toc) > 10:
            print(f"    ... 외 {len(toc) - 10}개")
    else:
        print("  TOC 없음")

    # 2. 전체 텍스트 추출
    print("\n[2] 전체 텍스트 추출...")
    full_text = extract_full_text(pdf_path)
    print(f"  총 문자 수: {len(full_text):,}")

    # 3. 텍스트 정규화
    print("\n[3] 텍스트 정규화...")
    normalizer = TextNormalizer()
    normalized = normalizer.normalize_full_text([full_text])
    print(f"  정규화 후 문자 수: {len(normalized):,}")

    # 4. 챕터 감지
    print("\n[4] 챕터 감지...")
    detector = ChapterDetector(pdf_path)
    chapters = detector.detect_chapters()

    if chapters:
        detection_method = chapters[0].detection_method
        avg_confidence = sum(ch.confidence for ch in chapters) / len(chapters)
    else:
        detection_method = "none"
        avg_confidence = 0.0

    print(f"\n  감지 방법: {detection_method}")
    print(f"  평균 신뢰도: {avg_confidence:.1%}")
    print(f"  감지된 챕터: {len(chapters)}개")

    print("\n" + "-" * 40)
    print("감지된 챕터 목록:")
    print("-" * 40)

    for i, ch in enumerate(chapters[:30]):  # 처음 30개만
        page_info = f"p.{ch.start_page}"
        if ch.end_page:
            page_info += f"-{ch.end_page}"

        level_indent = "  " * (ch.level - 1)
        print(f"  {i+1}. {level_indent}[L{ch.level}] {ch.title}")
        print(f"      {page_info} | {ch.detection_method} | conf: {ch.confidence:.1%}")

    if len(chapters) > 30:
        print(f"\n  ... 외 {len(chapters) - 30}개")

    # 5. 샘플 텍스트 출력
    print("\n" + "=" * 60)
    print("샘플 텍스트 (처음 500자):")
    print("=" * 60)
    print(normalized[:500])
    print("...")

    return chapters


def main():
    if len(sys.argv) < 2:
        print("사용법: python test_chapter_detection.py <PDF_파일_경로>")
        print("\n예시:")
        print("  python test_chapter_detection.py ./data/sample.pdf")
        sys.exit(1)

    pdf_path = sys.argv[1]

    if not os.path.exists(pdf_path):
        print(f"오류: 파일을 찾을 수 없습니다: {pdf_path}")
        sys.exit(1)

    test_chapter_detection(pdf_path)


if __name__ == "__main__":
    main()
