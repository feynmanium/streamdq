"""Markdown/code extraction stage for VTKE."""

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Set

import pandas as pd

from vtke.utils.io import read_text
from vtke.utils.logging import Logger


@dataclass
class MDXSettings:
    """Markdown extraction settings."""
    include_lang: List[str] = None
    exclude_lang: List[str] = None
    min_block_lines: int = 2
    max_block_chars: int = 100000

    def __post_init__(self):
        if self.include_lang is None:
            self.include_lang = []
        if self.exclude_lang is None:
            self.exclude_lang = []


@dataclass
class CodeBlock:
    """Extracted code block."""
    file: str
    language: str
    code: str
    start_line: int
    end_line: int
    block_index: int
    code_hash: str


class MarkdownExtractor:
    """Extract code blocks from markdown files."""

    def __init__(
        self,
        settings: MDXSettings,
        logger: Optional[Logger] = None
    ):
        self.settings = settings
        self.logger = logger or Logger()
        self.blocks: List[CodeBlock] = []

    def _compute_hash(self, code: str) -> str:
        """Compute hash of code block.

        Args:
            code: Code content

        Returns:
            SHA256 hash
        """
        return hashlib.sha256(code.encode('utf-8')).hexdigest()

    def _should_include_language(self, lang: str) -> bool:
        """Check if language should be included.

        Args:
            lang: Language identifier

        Returns:
            True if should include
        """
        if self.settings.include_lang and lang not in self.settings.include_lang:
            return False
        if self.settings.exclude_lang and lang in self.settings.exclude_lang:
            return False
        return True

    def _extract_fenced_blocks(self, content: str, file_path: str) -> List[CodeBlock]:
        """Extract fenced code blocks.

        Args:
            content: File content
            file_path: File path

        Returns:
            List of code blocks
        """
        blocks = []
        lines = content.splitlines()

        i = 0
        block_index = 0

        while i < len(lines):
            line = lines[i]

            # Check for fence start (``` or ~~~)
            fence_match = re.match(r'^(```|~~~)(\w*)', line)

            if fence_match:
                fence_chars = fence_match.group(1)
                language = fence_match.group(2) or "unknown"
                start_line = i + 1

                # Find matching fence end
                code_lines = []
                i += 1

                while i < len(lines):
                    if lines[i].startswith(fence_chars):
                        # Found end fence
                        end_line = i + 1

                        code = '\n'.join(code_lines)

                        # Apply filters
                        if len(code_lines) >= self.settings.min_block_lines:
                            if len(code) <= self.settings.max_block_chars:
                                if self._should_include_language(language):
                                    blocks.append(CodeBlock(
                                        file=file_path,
                                        language=language,
                                        code=code,
                                        start_line=start_line,
                                        end_line=end_line,
                                        block_index=block_index,
                                        code_hash=self._compute_hash(code)
                                    ))
                                    block_index += 1

                        break

                    code_lines.append(lines[i])
                    i += 1

            i += 1

        return blocks

    def _extract_indented_blocks(self, content: str, file_path: str, fenced_ranges: Set[tuple]) -> List[CodeBlock]:
        """Extract indented code blocks (not in fenced regions).

        Args:
            content: File content
            file_path: File path
            fenced_ranges: Set of (start_line, end_line) tuples for fenced blocks

        Returns:
            List of code blocks
        """
        blocks = []
        lines = content.splitlines()

        i = 0
        block_index = 0

        while i < len(lines):
            # Skip if in a fenced region
            in_fenced = any(start <= i + 1 <= end for start, end in fenced_ranges)

            if in_fenced:
                i += 1
                continue

            line = lines[i]

            # Check if line is indented (4 spaces or tab)
            if line.startswith('    ') or line.startswith('\t'):
                code_lines = []
                start_line = i + 1

                # Collect consecutive indented lines
                while i < len(lines):
                    in_fenced = any(start <= i + 1 <= end for start, end in fenced_ranges)
                    if in_fenced:
                        break

                    line = lines[i]

                    if line.startswith('    ') or line.startswith('\t'):
                        # Remove indentation
                        if line.startswith('    '):
                            code_lines.append(line[4:])
                        else:
                            code_lines.append(line[1:])
                        i += 1
                    elif not line.strip():
                        # Blank line, might be part of code block
                        code_lines.append('')
                        i += 1
                    else:
                        # Non-indented line, end of block
                        break

                # Trim trailing blank lines
                while code_lines and not code_lines[-1].strip():
                    code_lines.pop()

                if len(code_lines) >= self.settings.min_block_lines:
                    code = '\n'.join(code_lines)

                    if len(code) <= self.settings.max_block_chars:
                        blocks.append(CodeBlock(
                            file=file_path,
                            language="indented",
                            code=code,
                            start_line=start_line,
                            end_line=i,
                            block_index=block_index,
                            code_hash=self._compute_hash(code)
                        ))
                        block_index += 1

                continue

            i += 1

        return blocks

    def extract_from_file(self, file_path: Path) -> List[CodeBlock]:
        """Extract code blocks from a single file.

        Args:
            file_path: File to process

        Returns:
            List of code blocks
        """
        try:
            content = read_text(file_path)
        except Exception as e:
            self.logger.warn(f"Cannot read {file_path}: {e}", stage="mdx")
            return []

        # Extract fenced blocks first
        fenced_blocks = self._extract_fenced_blocks(content, str(file_path))

        # Get fenced ranges to avoid duplicates with indented
        fenced_ranges = {(b.start_line, b.end_line) for b in fenced_blocks}

        # Extract indented blocks
        indented_blocks = self._extract_indented_blocks(content, str(file_path), fenced_ranges)

        return fenced_blocks + indented_blocks

    def extract_from_files(self, file_paths: List[Path]) -> pd.DataFrame:
        """Extract code blocks from multiple files.

        Args:
            file_paths: List of files to process

        Returns:
            DataFrame with code blocks
        """
        self.logger.info(f"Extracting code blocks from {len(file_paths)} files", stage="mdx")

        all_blocks = []

        for file_path in file_paths:
            blocks = self.extract_from_file(file_path)
            all_blocks.extend(blocks)

        self.logger.info(f"Extracted {len(all_blocks)} code blocks", stage="mdx")

        # Convert to DataFrame
        if all_blocks:
            df = pd.DataFrame([
                {
                    'file': b.file,
                    'language': b.language,
                    'code': b.code,
                    'start_line': b.start_line,
                    'end_line': b.end_line,
                    'block_index': b.block_index,
                    'code_hash': b.code_hash
                }
                for b in all_blocks
            ])

            # Remove duplicates based on hash
            df = df.drop_duplicates(subset=['file', 'code_hash'])

            self.logger.info(f"After deduplication: {len(df)} unique blocks", stage="mdx")

            return df
        else:
            # Return empty DataFrame with correct schema
            return pd.DataFrame(columns=[
                'file', 'language', 'code', 'start_line', 'end_line', 'block_index', 'code_hash'
            ])
