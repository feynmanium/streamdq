"""Tests for text utilities."""

import pytest

from vtke.utils.text import (
    normalize_text,
    text_to_ngrams,
    jaccard_similarity,
    find_overlap_and_novel_tail,
    collapse_blank_lines
)


class TestNormalizeText:
    """Tests for normalize_text."""

    def test_basic_normalization(self):
        """Test basic whitespace normalization."""
        text = "Hello   World\n\nTest"
        result = normalize_text(text)
        assert result == "hello world test"

    def test_preserve_case(self):
        """Test case preservation."""
        text = "Hello World"
        result = normalize_text(text, preserve_case=True)
        assert result == "Hello World"

    def test_multiple_spaces(self):
        """Test multiple space collapsing."""
        text = "a    b     c"
        result = normalize_text(text)
        assert result == "a b c"


class TestTextToNgrams:
    """Tests for text_to_ngrams."""

    def test_basic_ngrams(self):
        """Test basic n-gram generation."""
        text = "hello world"
        ngrams = text_to_ngrams(text, k=5)
        assert "hello" in ngrams
        assert "ello " in ngrams
        assert len(ngrams) == len(text) - 5 + 1

    def test_short_text(self):
        """Test with text shorter than k."""
        text = "hi"
        ngrams = text_to_ngrams(text, k=5)
        assert ngrams == {"hi"}


class TestJaccardSimilarity:
    """Tests for jaccard_similarity."""

    def test_identical_sets(self):
        """Test with identical sets."""
        set1 = {"a", "b", "c"}
        set2 = {"a", "b", "c"}
        assert jaccard_similarity(set1, set2) == 1.0

    def test_no_overlap(self):
        """Test with no overlap."""
        set1 = {"a", "b"}
        set2 = {"c", "d"}
        assert jaccard_similarity(set1, set2) == 0.0

    def test_partial_overlap(self):
        """Test with partial overlap."""
        set1 = {"a", "b", "c"}
        set2 = {"b", "c", "d"}
        # Intersection: {b, c} = 2, Union: {a, b, c, d} = 4
        assert jaccard_similarity(set1, set2) == 0.5

    def test_empty_sets(self):
        """Test with empty sets."""
        assert jaccard_similarity(set(), set()) == 1.0


class TestFindOverlapAndNovelTail:
    """Tests for find_overlap_and_novel_tail."""

    def test_complete_overlap(self):
        """Test when texts are identical."""
        prev = "line1\nline2\nline3"
        curr = "line1\nline2\nline3"
        tail, count = find_overlap_and_novel_tail(prev, curr, min_unique_lines=2)
        assert tail == ""
        assert count == 0

    def test_no_overlap(self):
        """Test when there's no overlap."""
        prev = "line1\nline2"
        curr = "line3\nline4"
        tail, count = find_overlap_and_novel_tail(prev, curr, min_unique_lines=1)
        assert tail == "line3\nline4"
        assert count == 2

    def test_partial_overlap_with_tail(self):
        """Test partial overlap with novel tail."""
        prev = "line1\nline2\nline3"
        curr = "line2\nline3\nline4\nline5"
        tail, count = find_overlap_and_novel_tail(prev, curr, min_unique_lines=2)
        assert "line4" in tail
        assert "line5" in tail
        assert count == 2

    def test_insufficient_unique_lines(self):
        """Test when novel tail doesn't meet minimum."""
        prev = "line1\nline2\nline3"
        curr = "line2\nline3\nline4"
        tail, count = find_overlap_and_novel_tail(prev, curr, min_unique_lines=5)
        assert tail == ""
        assert count == 0


class TestCollapseBlankLines:
    """Tests for collapse_blank_lines."""

    def test_collapse_multiple_blanks(self):
        """Test collapsing multiple blank lines."""
        text = "line1\n\n\n\nline2"
        result = collapse_blank_lines(text, max_consecutive=2)
        assert result.count('\n\n\n') == 0

    def test_preserve_single_blanks(self):
        """Test preserving single blank lines."""
        text = "line1\n\nline2"
        result = collapse_blank_lines(text, max_consecutive=2)
        assert result == text

    def test_no_blanks(self):
        """Test with no blank lines."""
        text = "line1\nline2\nline3"
        result = collapse_blank_lines(text, max_consecutive=2)
        assert result == text
