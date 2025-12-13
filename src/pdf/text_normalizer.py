"""
텍스트 정규화 모듈

PDF에서 추출한 텍스트의 아티팩트 제거 및 페이지 간 텍스트 연결.
"""

import re
from typing import List


class TextNormalizer:
    """PDF 텍스트 정규화기.

    PDF 추출 시 발생하는 아티팩트를 제거하고,
    페이지 경계를 넘어가는 텍스트를 자연스럽게 연결.
    """

    def __init__(self):
        # 하이픈으로 끊긴 단어 패턴 (줄 끝 하이픈)
        self.hyphen_pattern = re.compile(r'(\w+)-\s*\n\s*(\w+)')
        # 단독 페이지 번호 패턴
        self.page_number_pattern = re.compile(r'\n\s*\d{1,4}\s*\n')
        # 연속 공백
        self.multi_space_pattern = re.compile(r' {2,}')
        # 연속 줄바꿈 (3개 이상)
        self.multi_newline_pattern = re.compile(r'\n{3,}')
        # 헤더/푸터 패턴 (페이지 상단/하단 반복 텍스트)
        self.header_footer_patterns = [
            re.compile(r'^.{0,50}Page\s+\d+.{0,50}$', re.MULTILINE | re.IGNORECASE),
            re.compile(r'^\s*\d+\s*$', re.MULTILINE),  # 단독 숫자
            re.compile(r'^.{0,30}Chapter\s+\d+.{0,30}$', re.MULTILINE),  # 반복되는 챕터 헤더
        ]
        # 테이블/벤치마크 데이터 패턴 (숫자와 짧은 텍스트가 반복되는 줄)
        # 예: "Country211 17.8 14.8" 또는 "UCF101 64.5 63.1"
        self.table_row_pattern = re.compile(
            r'^[A-Za-z0-9_\-\s]{3,40}\s+\d+\.?\d*\s+\d+\.?\d*.*$',
            re.MULTILINE
        )

    def normalize_full_text(self, pages: List[str]) -> str:
        """
        페이지 리스트를 연결하고 정규화.

        Args:
            pages: 페이지별 텍스트 리스트

        Returns:
            정규화된 전체 텍스트
        """
        if not pages:
            return ""

        # 1. 페이지 연결 (페이지 경계 처리)
        full_text = self._join_pages(pages)

        # 2. 하이픈으로 끊긴 단어 연결
        full_text = self._fix_hyphenation(full_text)

        # 3. 페이지 번호 제거
        full_text = self._remove_page_numbers(full_text)

        # 4. 헤더/푸터 제거
        full_text = self._remove_headers_footers(full_text)

        # 5. 테이블/벤치마크 데이터 제거
        full_text = self._remove_table_data(full_text)

        # 6. 공백 정규화
        full_text = self._normalize_whitespace(full_text)

        return full_text.strip()

    def _join_pages(self, pages: List[str]) -> str:
        """페이지 연결 (페이지 경계에서 문장이 끊기지 않도록)."""
        result = []

        for i, page in enumerate(pages):
            page = page.strip()
            if not page:
                continue

            if result:
                prev_text = result[-1]
                # 이전 페이지가 문장 중간에서 끝났는지 확인
                if prev_text and not prev_text[-1] in '.!?:"\'\n':
                    # 문장 중간이면 공백으로 연결
                    result.append(' ')
                else:
                    # 문장이 끝났으면 줄바꿈으로 연결
                    result.append('\n')

            result.append(page)

        return ''.join(result)

    def _fix_hyphenation(self, text: str) -> str:
        """줄 끝 하이픈으로 끊긴 단어 연결."""
        return self.hyphen_pattern.sub(r'\1\2', text)

    def _remove_page_numbers(self, text: str) -> str:
        """단독 페이지 번호 제거."""
        return self.page_number_pattern.sub('\n', text)

    def _remove_headers_footers(self, text: str) -> str:
        """반복되는 헤더/푸터 패턴 제거."""
        for pattern in self.header_footer_patterns:
            text = pattern.sub('', text)
        return text

    def _remove_table_data(self, text: str) -> str:
        """테이블/벤치마크 데이터 행 제거.

        숫자가 많이 포함된 짧은 줄들(테이블 행)을 감지하여 제거.
        연속된 테이블 행이 3줄 이상이면 테이블로 판단.
        """
        lines = text.split('\n')
        result_lines = []
        table_buffer = []

        for line in lines:
            stripped = line.strip()
            # 테이블 행 판단 기준:
            # 1. 패턴 매칭 (숫자 + 텍스트 조합)
            # 2. 숫자/소수점 비율이 높음 (30% 이상)
            # 3. 줄 길이가 적당함 (10~100자)
            is_table_row = False

            if self.table_row_pattern.match(stripped):
                is_table_row = True
            elif 10 <= len(stripped) <= 100:
                digit_count = sum(1 for c in stripped if c.isdigit() or c == '.')
                if len(stripped) > 0 and digit_count / len(stripped) > 0.3:
                    is_table_row = True

            if is_table_row:
                table_buffer.append(line)
            else:
                # 버퍼에 쌓인 테이블 행이 3줄 미만이면 일반 텍스트로 간주
                if len(table_buffer) < 3:
                    result_lines.extend(table_buffer)
                # 3줄 이상이면 테이블로 간주하여 제거 (버퍼 비움)
                table_buffer = []
                result_lines.append(line)

        # 마지막 버퍼 처리
        if len(table_buffer) < 3:
            result_lines.extend(table_buffer)

        return '\n'.join(result_lines)

    def _normalize_whitespace(self, text: str) -> str:
        """공백 정규화."""
        # 연속 공백 -> 단일 공백
        text = self.multi_space_pattern.sub(' ', text)
        # 연속 줄바꿈 -> 이중 줄바꿈 (문단 구분)
        text = self.multi_newline_pattern.sub('\n\n', text)
        return text

    def extract_chapter_text(
        self,
        full_text: str,
        start_char: int,
        end_char: int
    ) -> str:
        """
        전체 텍스트에서 챕터 텍스트 추출.

        Args:
            full_text: 전체 문서 텍스트
            start_char: 시작 문자 위치
            end_char: 끝 문자 위치

        Returns:
            정규화된 챕터 텍스트
        """
        chapter_text = full_text[start_char:end_char]

        # 챕터 시작 부분의 중복 제목 제거
        lines = chapter_text.split('\n')
        if lines:
            # 첫 줄이 짧고 대문자가 많으면 제목으로 간주
            first_line = lines[0].strip()
            if len(first_line) < 100 and sum(1 for c in first_line if c.isupper()) > len(first_line) * 0.3:
                # 이미 챕터 메타데이터에 제목이 있으므로 본문에서는 제거 가능
                pass  # 필요시 제거 로직 추가

        return chapter_text.strip()


def normalize_text(pages: List[str]) -> str:
    """
    편의 함수: 페이지 리스트를 정규화된 텍스트로 변환.

    Args:
        pages: 페이지별 텍스트 리스트

    Returns:
        정규화된 전체 텍스트
    """
    normalizer = TextNormalizer()
    return normalizer.normalize_full_text(pages)
