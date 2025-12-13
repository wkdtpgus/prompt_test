"""
의미적 청킹 모듈 (하이브리드 접근법)

규칙 기반 대략 분할 + LLM 세부 분할/아이디어 추출 통합.
"""

import re
from typing import List, Tuple, Optional

from langchain_core.prompts import ChatPromptTemplate
from sqlalchemy.orm import Session

from src.model.model import get_default_llm
from src.model.schemas import (
    HierarchicalChunk,
    SemanticParagraph,
    ChunkAndExtractResult,
)
from src.prompts.semantic_chunking import (
    SEMANTIC_CHUNKING_PROMPT,
    SEMANTIC_CHUNKING_HUMAN,
)
from src.db.operations import get_or_create_section


# 설정
BIG_CHUNK_MAX_SIZE = 2500  # 대략 분할 청크 최대 크기
BIG_CHUNK_MIN_SIZE = 500   # 대략 분할 청크 최소 크기
SMALL_CHUNK_MIN_SIZE = 100  # LLM 분할 결과 최소 크기


def hybrid_chunk_and_extract(
    chapter_text: str,
    chapter_id: Optional[int] = None,
    chapter_title: Optional[str] = None,
    book_id: Optional[int] = None,
    session: Optional[Session] = None,
    base_paragraph_index: int = 0
) -> List[Tuple[HierarchicalChunk, str]]:
    """
    하이브리드 청킹: 규칙 기반 대략 분할 + LLM 세부 분할/추출.

    Args:
        chapter_text: 챕터 전체 텍스트
        chapter_id: 챕터 DB ID
        chapter_title: 챕터 제목
        book_id: 책 DB ID (섹션 저장용)
        session: DB 세션 (섹션 저장용, None이면 섹션 저장 안함)
        base_paragraph_index: 전역 문단 인덱스 시작값

    Returns:
        (HierarchicalChunk, concept) 튜플 리스트
    """
    if not chapter_text or len(chapter_text.strip()) < SMALL_CHUNK_MIN_SIZE:
        return []

    # 1단계: 규칙 기반 대략 분할 (2000-3000자 청크)
    big_chunks = split_into_big_chunks(chapter_text, max_size=BIG_CHUNK_MAX_SIZE)

    results = []
    global_idx = base_paragraph_index
    chapter_idx = 0
    current_section = None  # 섹션 상태 추적
    current_section_id = None  # 섹션 ID 추적

    for big_chunk in big_chunks:
        # 2단계: LLM 세부 분할 + 아이디어 추출
        try:
            semantic_paragraphs = llm_split_and_extract(big_chunk)
        except Exception as e:
            # LLM 실패 시 폴백: 전체 청크를 하나의 문단으로
            print(f"LLM 청킹 실패, 폴백 사용: {e}")
            semantic_paragraphs = [
                SemanticParagraph(text=big_chunk, concept="", section_title=None)
            ]

        for para in semantic_paragraphs:
            # 너무 짧은 문단 필터링
            if len(para.text.strip()) < SMALL_CHUNK_MIN_SIZE:
                continue

            # 섹션 상태 업데이트 (새 섹션이 감지되면 갱신)
            # "null" 문자열도 None으로 처리
            if para.section_title and para.section_title.lower() != "null":
                current_section = para.section_title

                # DB에 섹션 저장 (session이 제공된 경우)
                if session and chapter_id and book_id:
                    section = get_or_create_section(
                        session=session,
                        chapter_id=chapter_id,
                        book_id=book_id,
                        title=current_section,
                    )
                    current_section_id = section.id

            # 문단 위치 찾기 (대략적)
            para_start = chapter_text.find(para.text[:50])
            if para_start == -1:
                para_start = 0
            para_end = para_start + len(para.text)

            chunk = HierarchicalChunk(
                text=para.text,
                chapter_id=chapter_id,
                chapter_title=chapter_title,
                section_id=current_section_id,
                section_title=current_section,
                paragraph_index=global_idx,
                chapter_paragraph_index=chapter_idx,
                start_char=para_start,
                end_char=para_end,
            )
            results.append((chunk, para.concept))
            global_idx += 1
            chapter_idx += 1

    return results


def split_into_big_chunks(text: str, max_size: int = 2500) -> List[str]:
    """
    규칙 기반 대략 분할 (더블 뉴라인 기준).

    Args:
        text: 전체 텍스트
        max_size: 청크 최대 크기

    Returns:
        대략 분할된 청크 리스트
    """
    # 더블 뉴라인으로 분할
    paragraphs = text.split("\n\n")

    chunks = []
    current = []
    current_len = 0

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # 공백 정규화
        para = re.sub(r'\s+', ' ', para)

        if current_len + len(para) > max_size and current:
            # 현재 청크 저장
            chunk_text = "\n\n".join(current)
            if len(chunk_text) >= BIG_CHUNK_MIN_SIZE:
                chunks.append(chunk_text)
            current = [para]
            current_len = len(para)
        else:
            current.append(para)
            current_len += len(para)

    # 마지막 청크
    if current:
        chunk_text = "\n\n".join(current)
        if len(chunk_text) >= BIG_CHUNK_MIN_SIZE:
            chunks.append(chunk_text)

    return chunks


def llm_split_and_extract(text: str) -> List[SemanticParagraph]:
    """
    LLM 호출하여 세부 분할 + 아이디어 추출.

    Args:
        text: 대략 분할된 청크 텍스트

    Returns:
        SemanticParagraph 리스트
    """
    llm = get_default_llm()
    structured_llm = llm.with_structured_output(
        ChunkAndExtractResult,
        method="json_mode"
    )

    prompt = ChatPromptTemplate.from_messages([
        ("system", SEMANTIC_CHUNKING_PROMPT),
        ("human", SEMANTIC_CHUNKING_HUMAN),
    ])

    chain = prompt | structured_llm
    result = chain.invoke({"text": text})

    return result.paragraphs


def get_semantic_chunking_stats(results: List[Tuple[HierarchicalChunk, str]]) -> dict:
    """
    의미적 청킹 결과 통계.

    Args:
        results: (HierarchicalChunk, concept) 튜플 리스트

    Returns:
        통계 딕셔너리
    """
    if not results:
        return {
            "total_paragraphs": 0,
            "paragraphs_with_concept": 0,
            "paragraphs_without_concept": 0,
            "unique_concepts": 0,
            "avg_paragraph_length": 0,
        }

    concepts = [concept for _, concept in results if concept]
    lengths = [len(chunk.text) for chunk, _ in results]

    return {
        "total_paragraphs": len(results),
        "paragraphs_with_concept": len(concepts),
        "paragraphs_without_concept": len(results) - len(concepts),
        "unique_concepts": len(set(concepts)),
        "avg_paragraph_length": sum(lengths) / len(lengths) if lengths else 0,
    }
