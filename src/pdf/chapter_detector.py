"""
챕터 감지 모듈

PDF에서 챕터/섹션 구조를 감지.
TOC 기반 또는 패턴 기반 감지 지원.
"""

import re
import fitz  # PyMuPDF
from typing import List, Optional

from src.model.schemas import DetectedChapter


class ChapterDetector:
    """다중 전략 챕터 감지기.

    기술 서적(LLM, AI, Agent)에 최적화된 챕터 감지.
    """

    # 기술 서적용 챕터 패턴
    CHAPTER_PATTERNS = [
        # Chapter 형식
        r'^(?:Chapter|CHAPTER)\s+(\d+)[\s.:–-]+(.+)$',
        # 숫자. 제목 형식 (예: "1. Introduction")
        r'^(\d+)\.\s+([A-Z][^.]{4,80})$',
        # 숫자 제목 형식 (예: "1 Introduction")
        r'^(\d+)\s+([A-Z][A-Za-z\s]{4,80})$',
        # Part 형식
        r'^(?:Part|PART)\s+(\d+|[IVXLCDM]+)[\s.:–-]+(.+)$',
        # Section 형식
        r'^(?:Section|SECTION)\s+(\d+)[\s.:–-]+(.+)$',
        # Appendix 형식
        r'^(?:Appendix|APPENDIX)\s+([A-Z])[\s.:–-]+(.+)$',
    ]

    # 섹션 패턴 (챕터 하위)
    SECTION_PATTERNS = [
        # 1.1 형식
        r'^(\d+\.\d+)\s+([A-Z][^.]{4,80})$',
        # 1.1.1 형식
        r'^(\d+\.\d+\.\d+)\s+([A-Z][^.]{4,60})$',
    ]

    def __init__(self, pdf_path: str):
        """
        Args:
            pdf_path: PDF 파일 경로
        """
        self.pdf_path = pdf_path

    def detect_chapters(self) -> List[DetectedChapter]:
        """
        챕터 감지 실행.

        1. TOC 기반 감지 시도 (가장 신뢰성 높음)
        2. TOC가 없거나 부족하면 패턴 기반 감지

        Returns:
            감지된 챕터 리스트
        """
        # 1. TOC 기반 감지 시도
        toc_chapters = self._detect_from_toc()
        if toc_chapters and len(toc_chapters) >= 3:
            return toc_chapters

        # 2. 패턴 기반 감지
        pattern_chapters = self._detect_from_patterns()

        # 3. TOC와 패턴 둘 다 있으면 병합
        if toc_chapters and pattern_chapters:
            return self._merge_detections(toc_chapters, pattern_chapters)

        # 4. 둘 중 하나라도 있으면 반환
        if pattern_chapters:
            return pattern_chapters
        if toc_chapters:
            return toc_chapters

        # 5. 감지 실패 시 전체 책을 단일 챕터로
        return self._create_fallback_chapter()

    def _detect_from_toc(self) -> List[DetectedChapter]:
        """PDF 내장 TOC에서 챕터 감지."""
        doc = fitz.open(self.pdf_path)
        try:
            toc = doc.get_toc()  # [[level, title, page], ...]
            total_pages = len(doc)

            if not toc:
                return []

            chapters = []
            for i, (level, title, page) in enumerate(toc):
                # 끝 페이지 계산: 같은 레벨 이하의 다음 항목 찾기
                end_page = total_pages - 1
                for j in range(i + 1, len(toc)):
                    next_level, _, next_page = toc[j]
                    # 같은 레벨이거나 상위 레벨이면 그 전 페이지까지
                    if next_level <= level:
                        end_page = next_page - 2  # 다음 챕터 시작 전 페이지
                        break

                # end_page가 start_page보다 작으면 최소한 start_page로 설정
                start_page_0idx = page - 1
                end_page = max(start_page_0idx, end_page)

                chapters.append(DetectedChapter(
                    title=title.strip(),
                    start_page=start_page_0idx,
                    end_page=end_page,
                    level=level,
                    detection_method='toc',
                    confidence=0.95
                ))

            return chapters
        finally:
            doc.close()

    def _detect_from_patterns(self) -> List[DetectedChapter]:
        """패턴 기반 챕터 감지."""
        doc = fitz.open(self.pdf_path)
        try:
            chapters = []
            total_pages = len(doc)

            for page_num in range(total_pages):
                page = doc[page_num]
                text = page.get_text()

                # 페이지 상단 텍스트만 검사 (챕터 제목은 보통 상단에 위치)
                lines = text.split('\n')[:15]

                for line in lines:
                    line = line.strip()
                    if not line or len(line) < 3:
                        continue

                    # 챕터 패턴 매칭
                    for pattern in self.CHAPTER_PATTERNS:
                        match = re.match(pattern, line, re.IGNORECASE)
                        if match:
                            chapters.append(DetectedChapter(
                                title=line,
                                start_page=page_num,
                                end_page=None,  # 나중에 채움
                                level=1,
                                detection_method='pattern',
                                confidence=0.7
                            ))
                            break
                    else:
                        continue
                    break  # 페이지당 하나의 챕터만

            # 끝 페이지 채우기
            for i in range(len(chapters)):
                if i + 1 < len(chapters):
                    chapters[i].end_page = chapters[i + 1].start_page - 1
                else:
                    chapters[i].end_page = total_pages - 1

            return chapters
        finally:
            doc.close()

    def _merge_detections(
        self,
        toc_chapters: List[DetectedChapter],
        pattern_chapters: List[DetectedChapter]
    ) -> List[DetectedChapter]:
        """TOC와 패턴 감지 결과 병합.

        TOC를 기본으로 하고, 패턴에서 추가 정보 보완.
        """
        # TOC가 더 신뢰성 높으므로 기본으로 사용
        if len(toc_chapters) >= len(pattern_chapters):
            return toc_chapters

        # TOC가 불완전하면 패턴 결과 사용
        return pattern_chapters

    def _create_fallback_chapter(self) -> List[DetectedChapter]:
        """감지 실패 시 전체 책을 단일 챕터로."""
        doc = fitz.open(self.pdf_path)
        try:
            total_pages = len(doc)
            return [DetectedChapter(
                title="Full Book",
                start_page=0,
                end_page=total_pages - 1,
                level=1,
                detection_method='fallback',
                confidence=0.5
            )]
        finally:
            doc.close()

    def detect_sections_in_chapter(
        self,
        chapter_text: str,
        chapter_title: str
    ) -> List[tuple]:
        """
        챕터 내 섹션 감지.

        Args:
            chapter_text: 챕터 텍스트
            chapter_title: 챕터 제목

        Returns:
            (section_title, start_pos, end_pos) 튜플 리스트
        """
        sections = []
        lines = chapter_text.split('\n')
        current_pos = 0

        for i, line in enumerate(lines):
            line_stripped = line.strip()
            if not line_stripped:
                current_pos += len(line) + 1
                continue

            # 섹션 패턴 매칭
            for pattern in self.SECTION_PATTERNS:
                match = re.match(pattern, line_stripped)
                if match:
                    sections.append((line_stripped, current_pos))
                    break

            current_pos += len(line) + 1

        # 섹션 끝 위치 계산
        result = []
        for i, (title, start) in enumerate(sections):
            if i + 1 < len(sections):
                end = sections[i + 1][1]
            else:
                end = len(chapter_text)
            result.append((title, start, end))

        return result


def detect_chapters(pdf_path: str) -> List[DetectedChapter]:
    """
    편의 함수: PDF에서 챕터 감지.

    Args:
        pdf_path: PDF 파일 경로

    Returns:
        감지된 챕터 리스트
    """
    detector = ChapterDetector(pdf_path)
    return detector.detect_chapters()
