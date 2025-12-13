"""해시 계산 유틸리티 - 중복 제거용"""

import hashlib
import re
from typing import Tuple


def normalize_for_hash(text: str) -> str:
    """해싱 전 텍스트 정규화.
    
    - 소문자 변환
    - 연속 공백을 단일 공백으로
    - 앞뒤 공백 제거
    - 구두점 제거 (선택적)
    
    Args:
        text: 원본 텍스트
        
    Returns:
        정규화된 텍스트
    """
    # 소문자 변환
    text = text.lower()
    
    # 연속 공백 → 단일 공백
    text = re.sub(r'\s+', ' ', text)
    
    # 앞뒤 공백 제거
    text = text.strip()
    
    return text


def compute_paragraph_hash(text: str) -> str:
    """SHA-256 해시 계산 (정확 매칭용).
    
    Args:
        text: 문단 텍스트
        
    Returns:
        64자 hex digest
    """
    normalized = normalize_for_hash(text)
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()


def compute_simhash64(text: str, ngram_size: int = 3) -> int:
    """64비트 SimHash 계산 (유사 매칭용).

    SimHash 알고리즘:
    1. 텍스트를 n-gram으로 분할
    2. 각 n-gram을 64비트 해시로 변환
    3. 가중치 벡터 계산 (비트가 1이면 +1, 0이면 -1)
    4. 최종 fingerprint 생성 (양수면 1, 음수면 0)

    Args:
        text: 문단 텍스트
        ngram_size: n-gram 크기 (기본 3)

    Returns:
        64비트 signed 정수 (PostgreSQL BIGINT 호환)
    """
    normalized = normalize_for_hash(text)

    # n-gram 생성
    ngrams = []
    for i in range(len(normalized) - ngram_size + 1):
        ngrams.append(normalized[i:i + ngram_size])

    if not ngrams:
        return 0

    # 64비트 가중치 벡터 초기화
    weights = [0] * 64

    for ngram in ngrams:
        # 각 n-gram을 64비트 해시로
        h = int(hashlib.md5(ngram.encode('utf-8')).hexdigest()[:16], 16)

        # 각 비트 위치에 가중치 적용
        for i in range(64):
            bit = (h >> i) & 1
            if bit:
                weights[i] += 1
            else:
                weights[i] -= 1

    # 최종 fingerprint 생성
    fingerprint = 0
    for i in range(64):
        if weights[i] > 0:
            fingerprint |= (1 << i)

    # unsigned -> signed 변환 (PostgreSQL BIGINT 호환)
    # 2^63 이상이면 음수로 변환
    if fingerprint >= (1 << 63):
        fingerprint -= (1 << 64)

    return fingerprint


def hamming_distance(hash1: int, hash2: int) -> int:
    """두 SimHash 간 해밍 거리 계산.

    Args:
        hash1: 첫 번째 SimHash (signed)
        hash2: 두 번째 SimHash (signed)

    Returns:
        다른 비트 수 (0-64)
    """
    xor = hash1 ^ hash2
    # signed 음수의 경우 64비트로 마스킹
    xor = xor & 0xFFFFFFFFFFFFFFFF
    return bin(xor).count('1')


def is_fuzzy_duplicate(hash1: int, hash2: int, threshold: int = 6) -> bool:
    """퍼지 중복 여부 판단.
    
    Args:
        hash1: 첫 번째 SimHash
        hash2: 두 번째 SimHash  
        threshold: 해밍 거리 임계값 (기본 6)
            - <= 3: ~95% 유사
            - <= 6: ~90% 유사
            - <= 10: ~85% 유사
            
    Returns:
        중복 여부
    """
    return hamming_distance(hash1, hash2) <= threshold


def compute_hashes(text: str) -> Tuple[str, int]:
    """paragraph_hash와 simhash64를 한번에 계산.
    
    Args:
        text: 문단 텍스트
        
    Returns:
        (paragraph_hash, simhash64) 튜플
    """
    return compute_paragraph_hash(text), compute_simhash64(text)