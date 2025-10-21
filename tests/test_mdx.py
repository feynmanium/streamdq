"""Tests for markdown extraction."""

import tempfile
from pathlib import Path

import pytest

from vtke.stages.mdx import MarkdownExtractor, MDXSettings


class TestMarkdownExtractor:
    """Tests for MarkdownExtractor."""

    def test_extract_fenced_blocks(self):
        """Test extracting fenced code blocks."""
        content = """
# Header

```python
def hello():
    print("world")
```

Some text.

```javascript
console.log("test");
```
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(content)
            f.flush()
            temp_path = Path(f.name)

        try:
            extractor = MarkdownExtractor(MDXSettings())
            blocks = extractor.extract_from_file(temp_path)

            assert len(blocks) == 2
            assert blocks[0].language == "python"
            assert "def hello" in blocks[0].code
            assert blocks[1].language == "javascript"
            assert "console.log" in blocks[1].code

        finally:
            temp_path.unlink()

    def test_extract_indented_blocks(self):
        """Test extracting indented code blocks."""
        content = """
# Header

Some text.

    def indented_code():
        return True

More text.
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(content)
            f.flush()
            temp_path = Path(f.name)

        try:
            extractor = MarkdownExtractor(MDXSettings())
            blocks = extractor.extract_from_file(temp_path)

            # Should find the indented block
            indented = [b for b in blocks if b.language == "indented"]
            assert len(indented) >= 1
            assert "def indented_code" in indented[0].code

        finally:
            temp_path.unlink()

    def test_language_filtering(self):
        """Test language include/exclude filtering."""
        content = """
```python
print("hello")
```

```java
System.out.println("world");
```

```javascript
console.log("test");
```
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(content)
            f.flush()
            temp_path = Path(f.name)

        try:
            # Include only python
            settings = MDXSettings(include_lang=["python"])
            extractor = MarkdownExtractor(settings)
            blocks = extractor.extract_from_file(temp_path)

            assert len(blocks) == 1
            assert blocks[0].language == "python"

            # Exclude java
            settings = MDXSettings(exclude_lang=["java"])
            extractor = MarkdownExtractor(settings)
            blocks = extractor.extract_from_file(temp_path)

            assert len(blocks) == 2
            assert all(b.language != "java" for b in blocks)

        finally:
            temp_path.unlink()

    def test_min_block_lines(self):
        """Test minimum block lines filtering."""
        content = """
```python
x = 1
```

```python
def long_function():
    line1 = 1
    line2 = 2
    line3 = 3
    return line1 + line2 + line3
```
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(content)
            f.flush()
            temp_path = Path(f.name)

        try:
            settings = MDXSettings(min_block_lines=3)
            extractor = MarkdownExtractor(settings)
            blocks = extractor.extract_from_file(temp_path)

            # Should only get the longer block
            assert len(blocks) == 1
            assert "def long_function" in blocks[0].code

        finally:
            temp_path.unlink()

    def test_deduplication(self):
        """Test code block deduplication."""
        content = """
```python
def hello():
    print("world")
```

Some text.

```python
def hello():
    print("world")
```
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(content)
            f.flush()
            temp_path = Path(f.name)

        try:
            extractor = MarkdownExtractor(MDXSettings())
            df = extractor.extract_from_files([temp_path])

            # Should deduplicate identical blocks
            assert len(df) == 1

        finally:
            temp_path.unlink()

    def test_unclosed_fence(self):
        """Test handling of unclosed fence."""
        content = """
```python
def unclosed():
    print("no closing fence")

More text here.
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(content)
            f.flush()
            temp_path = Path(f.name)

        try:
            extractor = MarkdownExtractor(MDXSettings())
            blocks = extractor.extract_from_file(temp_path)

            # Should handle gracefully (may extract or skip)
            # No exception should be raised
            assert isinstance(blocks, list)

        finally:
            temp_path.unlink()
