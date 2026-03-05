"""Document chunker with sentence-aware boundaries."""

from __future__ import annotations

import logging
import re

import tiktoken

from .config import RAGConfig
from .types import RAGChunk, RAGDocument

logger = logging.getLogger(__name__)

_SENTENCE_BOUNDARY = re.compile(r"(?<=[.!?])\s+(?=[A-Z])")


class DocumentChunker:
    """Splits RAGDocuments into token-bounded, sentence-aware chunks."""

    def __init__(self, config: RAGConfig | None = None) -> None:
        self._config = config or RAGConfig()
        self._enc = tiktoken.get_encoding("cl100k_base")

    def _token_count(self, text: str) -> int:
        return len(self._enc.encode(text))

    def _split_sentences(self, text: str) -> list[str]:
        """Split text into sentences, preserving newline structure."""
        sentences: list[str] = []
        for paragraph in text.split("\n"):
            paragraph = paragraph.strip()
            if not paragraph:
                continue
            parts = _SENTENCE_BOUNDARY.split(paragraph)
            sentences.extend(p.strip() for p in parts if p.strip())
        return sentences

    def chunk_document(self, doc: RAGDocument) -> list[RAGChunk]:
        """Split a document into overlapping, sentence-aware chunks."""
        sentences = self._split_sentences(doc.content)
        if not sentences:
            return []

        max_tokens = self._config.chunk_size_tokens
        overlap_tokens = self._config.chunk_overlap_tokens

        chunks: list[RAGChunk] = []
        current_sentences: list[str] = []
        current_tokens = 0

        for sentence in sentences:
            sent_tokens = self._token_count(sentence)

            if sent_tokens > max_tokens:
                if current_sentences:
                    chunks.append(
                        self._make_chunk(
                            current_sentences,
                            len(chunks),
                            doc,
                        )
                    )
                    current_sentences = []
                    current_tokens = 0
                chunks.append(
                    self._make_chunk(
                        [sentence],
                        len(chunks),
                        doc,
                    )
                )
                continue

            if current_tokens + sent_tokens > max_tokens and current_sentences:
                chunks.append(
                    self._make_chunk(
                        current_sentences,
                        len(chunks),
                        doc,
                    )
                )
                overlap_sents: list[str] = []
                overlap_tok = 0
                for s in reversed(current_sentences):
                    st = self._token_count(s)
                    if overlap_tok + st > overlap_tokens:
                        break
                    overlap_sents.insert(0, s)
                    overlap_tok += st
                current_sentences = overlap_sents
                current_tokens = overlap_tok

            current_sentences.append(sentence)
            current_tokens += sent_tokens

        if current_sentences:
            chunks.append(
                self._make_chunk(
                    current_sentences,
                    len(chunks),
                    doc,
                )
            )

        return chunks

    def _make_chunk(
        self,
        sentences: list[str],
        index: int,
        doc: RAGDocument,
    ) -> RAGChunk:
        return RAGChunk(
            text=" ".join(sentences),
            chunk_index=index,
            canonical_city_id=doc.canonical_city_id,
            source_type=doc.source_type,
            metadata={**doc.metadata},
        )

    def chunk_documents(self, docs: list[RAGDocument]) -> list[RAGChunk]:
        """Chunk a batch of documents."""
        all_chunks: list[RAGChunk] = []
        for doc in docs:
            all_chunks.extend(self.chunk_document(doc))
        logger.info(
            "Chunked %d documents into %d chunks",
            len(docs),
            len(all_chunks),
        )
        return all_chunks
