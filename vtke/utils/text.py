"""Text processing utilities for VTKE."""

import re
from difflib import SequenceMatcher
from typing import List, Set


def normalize_text(text: str, preserve_case: bool = False) -> str:
    """Normalize text for comparison.

    Args:
        text: Input text
        preserve_case: If True, preserve original casing

    Returns:
        Normalized text
    """
    # Collapse multiple whitespace
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()

    if not preserve_case:
        text = text.lower()

    return text


def text_to_ngrams(text: str, k: int = 20) -> Set[str]:
    """Convert text to k-character n-grams.

    Args:
        text: Input text
        k: N-gram size (default 20)

    Returns:
        Set of n-grams
    """
    if len(text) < k:
        return {text}

    return {text[i:i+k] for i in range(len(text) - k + 1)}


def jaccard_similarity(set1: Set[str], set2: Set[str]) -> float:
    """Calculate Jaccard similarity between two sets.

    Args:
        set1: First set
        set2: Second set

    Returns:
        Jaccard similarity (0.0 to 1.0)
    """
    if not set1 and not set2:
        return 1.0
    if not set1 or not set2:
        return 0.0

    intersection = len(set1 & set2)
    union = len(set1 | set2)

    return intersection / union if union > 0 else 0.0


def find_overlap_and_novel_tail(
    prev_text: str,
    curr_text: str,
    min_unique_lines: int = 2
) -> tuple[str, int]:
    """Find overlapping content and extract novel tail.

    Uses difflib to find the best overlap between the end of prev_text
    and the beginning of curr_text, then extracts the novel tail.

    Args:
        prev_text: Previous text
        curr_text: Current text
        min_unique_lines: Minimum number of unique lines required

    Returns:
        Tuple of (novel_tail, num_novel_lines)
    """
    prev_lines = prev_text.splitlines()
    curr_lines = curr_text.splitlines()

    if not curr_lines:
        return "", 0

    # Find best overlap using SequenceMatcher
    matcher = SequenceMatcher(None, prev_lines, curr_lines)
    match = matcher.find_longest_match(0, len(prev_lines), 0, len(curr_lines))

    if match.size == 0:
        # No overlap found, entire current text is novel
        return curr_text, len(curr_lines)

    # Extract tail after the overlap
    tail_start_idx = match.b + match.size
    novel_lines = curr_lines[tail_start_idx:]

    if len(novel_lines) < min_unique_lines:
        return "", 0

    return "\n".join(novel_lines), len(novel_lines)


def collapse_blank_lines(text: str, max_consecutive: int = 2) -> str:
    """Collapse consecutive blank lines.

    Args:
        text: Input text
        max_consecutive: Maximum consecutive blank lines to keep

    Returns:
        Text with collapsed blank lines
    """
    lines = text.splitlines()
    result = []
    blank_count = 0

    for line in lines:
        if not line.strip():
            blank_count += 1
            if blank_count <= max_consecutive:
                result.append(line)
        else:
            blank_count = 0
            result.append(line)

    return "\n".join(result)
