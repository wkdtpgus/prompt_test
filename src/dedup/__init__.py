"""중복 제거 모듈"""
from .hash_utils import (
    compute_hashes,
    compute_paragraph_hash,
    compute_simhash64,
    hamming_distance,
    is_fuzzy_duplicate,
    normalize_for_hash,
)
from .dedup_service import DeduplicationService, DeduplicationResult

__all__ = [
    # Hash utilities
    "compute_hashes",
    "compute_paragraph_hash",
    "compute_simhash64",
    "hamming_distance",
    "is_fuzzy_duplicate",
    "normalize_for_hash",
    # Service
    "DeduplicationService",
    "DeduplicationResult",
]