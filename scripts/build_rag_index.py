#!/usr/bin/env python3
"""Build the FAISS RAG index from all data sources.

Runs all loaders → chunks documents → builds FAISS index → saves to disk.
Optionally uploads to S3.

Usage:
    poetry run python scripts/build_rag_index.py [--upload-s3]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from move_to_happy.rag.chunker import DocumentChunker
from move_to_happy.rag.config import RAGConfig
from move_to_happy.rag.indexer import FAISSIndex
from move_to_happy.rag.loaders.community import load_community_profiles
from move_to_happy.rag.loaders.documents import load_lme_reference_docs
from move_to_happy.rag.loaders.economic import load_economic_narratives
from move_to_happy.rag.loaders.geospatial import load_geospatial_narratives
from move_to_happy.rag.loaders.health import load_healthcare_narratives
from move_to_happy.rag.loaders.hospital import load_hospital_documents
from move_to_happy.rag.types import RAGDocument

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build MTH RAG index")
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Upload index to S3 after building",
    )
    parser.add_argument(
        "--index-dir",
        default="data/rag_index",
        help="Directory to save the index",
    )
    args = parser.parse_args()

    config = RAGConfig(index_dir=args.index_dir)

    print("=" * 60)
    print("Building MTH RAG Index")
    print("=" * 60)

    t0 = time.time()
    all_docs: list[RAGDocument] = []

    print("\n[1/6] Loading community profiles...")
    docs = load_community_profiles()
    all_docs.extend(docs)
    print(f"  → {len(docs)} community documents")

    print("\n[2/6] Loading LME reference documents...")
    docs = load_lme_reference_docs()
    all_docs.extend(docs)
    print(f"  → {len(docs)} reference documents")

    print("\n[3/6] Loading healthcare narratives...")
    docs = load_healthcare_narratives()
    all_docs.extend(docs)
    print(f"  → {len(docs)} healthcare documents")

    print("\n[4/6] Loading economic narratives...")
    docs = load_economic_narratives()
    all_docs.extend(docs)
    print(f"  → {len(docs)} economic documents")

    print("\n[5/6] Loading geospatial narratives...")
    docs = load_geospatial_narratives()
    all_docs.extend(docs)
    print(f"  → {len(docs)} geospatial documents")

    print("\n[6/6] Loading hospital documents...")
    docs = load_hospital_documents()
    all_docs.extend(docs)
    print(f"  → {len(docs)} hospital documents")

    print(f"\nTotal documents: {len(all_docs)}")

    print("\nChunking documents...")
    chunker = DocumentChunker(config)
    chunks = chunker.chunk_documents(all_docs)
    print(f"  → {len(chunks)} chunks")

    print("\nBuilding FAISS index...")
    index = FAISSIndex(config)
    index.build(chunks)

    print(f"\nSaving index to {config.index_dir}...")
    index.save()

    if args.upload_s3:
        print("\nUploading to S3...")
        from move_to_happy.rag.s3_sync import upload_index_to_s3

        uri = upload_index_to_s3(config)
        print(f"  → {uri}")

    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Done! Built index in {elapsed:.1f}s")
    print(f"  Chunks: {len(chunks)}")
    print(f"  Index:  {config.index_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
