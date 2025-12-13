"""Paragraph chunking algorithm - Hybrid Strategy.

Optimized multi-pass algorithm for splitting PDF page text into meaningful paragraphs.
Uses hybrid approach: paragraph structure + sentence count + length constraints.
계층적 청킹 지원 (챕터 컨텍스트 포함).
"""

import re
from typing import List, Optional

from src.model.schemas import HierarchicalChunk


# Configuration - Hybrid Strategy (Optimized)
MIN_PARAGRAPH_LENGTH = 150   # Minimum 150 characters
MAX_PARAGRAPH_LENGTH = 1000  # Maximum 1000 characters
TARGET_SENTENCES = 5         # Target 5 sentences per chunk


def split_paragraphs(page_text: str) -> List[str]:
    """Split page text into paragraphs using hybrid strategy.

    Hybrid algorithm:
    1. Split by double newlines (preserve paragraph structure)
    2. Normalize whitespace
    3. Handle length constraints (MIN: 150, MAX: 1000)
    4. Split long chunks by sentence count (target: 5 sentences)
    5. Merge short chunks
    6. Remove headers/footers

    Args:
        page_text: Raw text from PDF page

    Returns:
        List of paragraph texts (150-1000 chars each)
    """
    if not page_text or len(page_text.strip()) < MIN_PARAGRAPH_LENGTH:
        return []

    # Clean up PDF artifacts
    # Remove footnote numbers at start of lines
    text = re.sub(r'^\d+\s+', '', page_text, flags=re.MULTILINE)
    # Remove page break markers (| at end of lines)
    text = re.sub(r'\s*\|\s*$', '', text, flags=re.MULTILINE)
    # Remove hyphenation at line breaks (word- \n word → word)
    text = re.sub(r'(\w+)-\s*\n\s*(\w+)', r'\1\2', text)

    # Step 1: Split by double newlines (paragraph boundaries)
    paragraphs = text.split("\n\n")

    chunks = []
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # Step 2: Normalize whitespace
        normalized = re.sub(r"\s+", " ", para)

        # Step 3: Check length
        if MIN_PARAGRAPH_LENGTH <= len(normalized) <= MAX_PARAGRAPH_LENGTH:
            # Good size - keep as is
            chunks.append(normalized)
        elif len(normalized) < MIN_PARAGRAPH_LENGTH:
            # Too short - will merge later
            chunks.append(normalized)
        else:
            # Too long - split by sentences
            sentences = re.split(r"(?<=[.!?])\s+", normalized)
            current = []

            for sent in sentences:
                current.append(sent)
                chunk_text = " ".join(current)

                # Split when reaching target sentences or max length
                if len(current) >= TARGET_SENTENCES or len(chunk_text) >= MAX_PARAGRAPH_LENGTH:
                    if len(chunk_text) >= MIN_PARAGRAPH_LENGTH:
                        chunks.append(chunk_text)
                    current = []

            # Don't forget remaining sentences
            if current:
                chunk_text = " ".join(current)
                if len(chunk_text) >= MIN_PARAGRAPH_LENGTH:
                    chunks.append(chunk_text)

    # Step 4: Merge short chunks
    merged = []
    buffer = ""

    for chunk in chunks:
        if len(buffer) == 0:
            buffer = chunk
        elif len(buffer) < MIN_PARAGRAPH_LENGTH:
            # Merge with buffer
            buffer += " " + chunk
        else:
            # Buffer is good, save it
            merged.append(buffer)
            buffer = chunk

    # Don't forget last buffer
    if buffer and len(buffer) >= MIN_PARAGRAPH_LENGTH:
        merged.append(buffer)

    # Step 5: Filter headers/footers
    filtered = [
        chunk
        for chunk in merged
        if not is_header_footer(chunk) and len(chunk) >= MIN_PARAGRAPH_LENGTH
    ]

    return filtered


def is_header_footer(text: str) -> bool:
    """Check if text looks like a header or footer.

    Headers/footers are typically:
    - Very short (< 20 chars)
    - Contain page numbers
    - All caps and short
    - Repeated patterns

    Args:
        text: Text to check

    Returns:
        True if text looks like header/footer
    """
    text = text.strip()

    # Too short
    if len(text) < 20:
        return True

    # Only digits (page number)
    if re.match(r"^\d+$", text):
        return True

    # Pattern like "Page 123" or "Chapter 5"
    if re.match(r"^(page|chapter)\s+\d+$", text, re.IGNORECASE):
        return True

    # All caps and short
    if text.isupper() and len(text) < 50:
        return True

    return False


def get_paragraph_stats(paragraphs: List[str]) -> dict:
    """Get statistics about paragraphs.

    Args:
        paragraphs: List of paragraph texts

    Returns:
        Dictionary with statistics:
        - count: Number of paragraphs
        - total_chars: Total characters
        - avg_length: Average paragraph length
        - min_length: Minimum paragraph length
        - max_length: Maximum paragraph length
    """
    if not paragraphs:
        return {
            "count": 0,
            "total_chars": 0,
            "avg_length": 0,
            "min_length": 0,
            "max_length": 0,
        }

    lengths = [len(p) for p in paragraphs]

    return {
        "count": len(paragraphs),
        "total_chars": sum(lengths),
        "avg_length": sum(lengths) / len(lengths),
        "min_length": min(lengths),
        "max_length": max(lengths),
    }


# ============================================================
# 계층적 청킹 (챕터 컨텍스트 포함)
# ============================================================

def split_chapter_into_paragraphs(
    chapter_text: str,
    chapter_id: Optional[int] = None,
    chapter_title: Optional[str] = None,
    base_paragraph_index: int = 0
) -> List[HierarchicalChunk]:
    """
    챕터 텍스트를 계층적 문단으로 분할.

    챕터 컨텍스트(ID, 제목, section_title)를 포함한 청크 생성.
    section_id는 DB 저장 시 할당됨.

    Args:
        chapter_text: 챕터 전체 텍스트
        chapter_id: 챕터 DB ID
        chapter_title: 챕터 제목
        base_paragraph_index: 전역 문단 인덱스 시작값

    Returns:
        HierarchicalChunk 리스트 (section_id=None, section_title 포함)
    """
    if not chapter_text or len(chapter_text.strip()) < MIN_PARAGRAPH_LENGTH:
        return []

    # 섹션 감지
    sections = _detect_sections(chapter_text)

    chunks = []
    global_idx = base_paragraph_index
    chapter_idx = 0

    for section_title, section_start, section_end in sections:
        section_text = chapter_text[section_start:section_end]

        # 섹션 내 문단 분할
        paragraphs = split_paragraphs(section_text)

        # 각 문단의 위치 추적
        current_pos = section_start
        for para_text in paragraphs:
            # 문단 위치 찾기
            para_start = chapter_text.find(para_text, current_pos)
            if para_start == -1:
                para_start = current_pos
            para_end = para_start + len(para_text)

            chunks.append(HierarchicalChunk(
                text=para_text,
                chapter_id=chapter_id,
                chapter_title=chapter_title,
                section_id=None,  # DB 저장 후 할당
                section_title=section_title,
                paragraph_index=global_idx,
                chapter_paragraph_index=chapter_idx,
                start_char=para_start,
                end_char=para_end,
            ))

            global_idx += 1
            chapter_idx += 1
            current_pos = para_end

    return chunks


def _detect_sections(text: str) -> List[tuple]:
    """
    텍스트 내 섹션 경계 감지.

    Args:
        text: 챕터 텍스트

    Returns:
        (section_title, start_pos, end_pos) 튜플 리스트
    """
    # 섹션 헤더 패턴
    section_patterns = [
        r'^(\d+\.\d+)\s+([A-Z][^\n]{4,80})$',  # 1.1 Title
        r'^(\d+\.\d+\.\d+)\s+([A-Z][^\n]{4,60})$',  # 1.1.1 Title
    ]

    sections = []
    lines = text.split('\n')
    current_pos = 0
    last_section_start = 0
    last_section_title = None

    for line in lines:
        line_stripped = line.strip()

        # 섹션 패턴 매칭
        matched = False
        for pattern in section_patterns:
            match = re.match(pattern, line_stripped)
            if match:
                # 이전 섹션 저장
                if last_section_title is not None or sections:
                    if not sections:
                        # 첫 섹션 이전 내용
                        sections.append((None, 0, current_pos))
                    else:
                        sections[-1] = (
                            sections[-1][0],
                            sections[-1][1],
                            current_pos
                        )

                # 새 섹션 시작
                last_section_title = line_stripped
                last_section_start = current_pos
                sections.append((last_section_title, current_pos, len(text)))
                matched = True
                break

        current_pos += len(line) + 1  # +1 for newline

    # 섹션이 없으면 전체를 하나의 섹션으로
    if not sections:
        sections.append((None, 0, len(text)))

    return sections


def split_text_hierarchically(
    full_text: str,
    chapters: List[dict],
    book_id: Optional[int] = None
) -> List[HierarchicalChunk]:
    """
    전체 텍스트를 챕터 구조에 따라 계층적으로 분할.

    Args:
        full_text: 전체 문서 텍스트
        chapters: 챕터 정보 리스트 (DB 모델 또는 dict)
        book_id: 책 ID

    Returns:
        전체 HierarchicalChunk 리스트
    """
    all_chunks = []
    global_para_idx = 0

    for chapter in chapters:
        # chapter가 dict인 경우와 객체인 경우 모두 처리
        if hasattr(chapter, 'id'):
            ch_id = chapter.id
            ch_title = chapter.title
            ch_start = getattr(chapter, 'start_char', None)
            ch_end = getattr(chapter, 'end_char', None)
        else:
            ch_id = chapter.get('id')
            ch_title = chapter.get('title')
            ch_start = chapter.get('start_char')
            ch_end = chapter.get('end_char')

        # 챕터 텍스트 추출 (문자 위치 기반)
        if ch_start is not None and ch_end is not None:
            chapter_text = full_text[ch_start:ch_end]
        else:
            # 위치 정보가 없으면 건너뜀
            continue

        # 챕터 내 문단 분할
        chapter_chunks = split_chapter_into_paragraphs(
            chapter_text=chapter_text,
            chapter_id=ch_id,
            chapter_title=ch_title,
            base_paragraph_index=global_para_idx
        )

        all_chunks.extend(chapter_chunks)
        global_para_idx += len(chapter_chunks)

    return all_chunks
