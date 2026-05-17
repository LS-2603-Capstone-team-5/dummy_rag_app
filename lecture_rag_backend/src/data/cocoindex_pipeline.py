from __future__ import annotations

import os
import pathlib
import re
from dataclasses import dataclass
from typing import Annotated, AsyncIterator

import asyncpg
import numpy as np
from numpy.typing import NDArray
from openai import AsyncOpenAI
from dotenv import load_dotenv

import cocoindex as coco
from cocoindex.connectors import localfs, postgres
from cocoindex.resources.file import FileLike, PatternFilePathMatcher
from cocoindex.resources.id import IdGenerator
from cocoindex.resources.schema import VectorSchema
from cocoindex.connectors.google_drive import GoogleDriveSource

load_dotenv()

TABLE_NAME = "coco_data_chunks"

PG_DB = coco.ContextKey[asyncpg.Pool]("lecture_rag_db")
OPENAI = coco.ContextKey[AsyncOpenAI]("openai")


@coco.lifespan
async def coco_lifespan(builder: coco.EnvironmentBuilder) -> AsyncIterator[None]:
    # CocoIndex uses an LMDB file to track incremental state between runs.
    # Place it at the project root (lecture_rag_backend/) alongside pyproject.toml.
    builder.settings.db_path = pathlib.Path(__file__).parents[2] / "cocoindex.db"

    async with await asyncpg.create_pool(os.getenv("ASYNC_PG_CONNECTION_STRING")) as pool:
        builder.provide(PG_DB, pool)
        builder.provide(OPENAI, AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY")))
        yield


@dataclass
class DataChunk:
    id: int
    lecture_title: str
    content: str
    embedding: Annotated[NDArray, VectorSchema(dtype=np.dtype("float32"), size=1536)]


@coco.fn(memo=True)
async def embed_content(enriched_text: str) -> NDArray:
    # memo=True: same enriched text → cached result, no API call on reruns
    response = await coco.use_context(OPENAI).embeddings.create(
        model="text-embedding-3-small",
        input=enriched_text,
    )
    return np.array(response.data[0].embedding, dtype=np.float32)


def _parse_slides(text: str) -> tuple[str, list[tuple[int, str]]]:
    """Parse a cleaned .txt file into (lecture_title, [(slide_num, text), ...])."""
    blocks = text.strip().split("\n\n")
    lecture_title = ""
    slides = []
    for block in blocks:
        block = block.strip()
        if block.startswith("# "):
            lecture_title = block[2:].strip()
        else:
            m = re.match(r"^\[Slide (\d+)\]\s+(.*)", block, re.DOTALL)
            if m:
                slides.append((int(m.group(1)), m.group(2).strip()))
    return lecture_title, slides


def _sliding_window_chunks(
    lecture_title: str,
    course: str,
    slides: list[tuple[int, str]],
    window: int = 4,
    stride: int = 2,
) -> list[tuple[str, str]]:
    """Return (content, enriched_content) pairs for each sliding-window chunk."""
    results = []
    for start in range(0, len(slides), stride):
        window_slides = slides[start : start + window]
        content = "\n\n".join(f"[Slide {num}] {text}" for num, text in window_slides)
        enriched = f"Course: {course}\nLecture: {lecture_title}\n{content}"
        results.append((content, enriched))
    return results


@coco.fn(memo=True)
async def process_txt_file(
    file: FileLike,
    table: postgres.TableTarget[DataChunk],
) -> None:
    # memo=True: if the file hasn't changed since last run, skip it entirely
    text = await file.read_text()
    filename = file.file_path.path.name  # e.g. "PHIL_1000_-_01_-_Intro.txt"

    lecture_title, slides = _parse_slides(text)

    m = re.match(r"^([A-Z]+_\d+)", filename)
    course = m.group(1).replace("_", " ") if m else "Unknown"

    id_gen = IdGenerator()

    if slides:
        chunks = _sliding_window_chunks(lecture_title, course, slides)
    else:
        # Plain text fallback: use filename stem as title, chunk by paragraph
        lecture_title = pathlib.Path(filename).stem.replace("_", " ")
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
        chunks = [
            (p, f"Course: {course}\nLecture: {lecture_title}\n{p}")
            for p in paragraphs
        ]

    print(f"[DEBUG txt] {filename}: {len(chunks)} chunks (slides={len(slides)})")
    for content, enriched in chunks:
        table.declare_row(
            row=DataChunk(
                id=await id_gen.next_id(enriched),
                lecture_title=lecture_title,
                content=content,
                embedding=await embed_content(enriched),
            )
        )


@coco.fn(memo=True)
async def process_qa_md(
    file: FileLike,
    table: postgres.TableTarget[DataChunk],
) -> None:
    # memo=True: unchanged Q&A files are skipped entirely
    text = await file.read_text()

    fname = file.file_path.path.name
    id_gen = IdGenerator()
    row_count = 0

    # Try structured Q&A format first: ## heading blocks with **Lecture:** metadata
    blocks = re.split(r'\n(?=## )', text.strip())
    qa_blocks = [b.strip() for b in blocks if b.strip().startswith("## ")]

    for block in qa_blocks:
        lines = block.splitlines()
        title = re.sub(r'^##\s*\*?\*?(.+?)\*?\*?\s*$', r'\1', lines[0]).strip()

        lecture_title = None
        body_start = 1
        for i, line in enumerate(lines[1:], 1):
            m = re.match(r'^\*\*Lecture:\*\*\s+(.+)$', line.strip())
            if m:
                lecture_title = m.group(1).strip()
                body_start = i + 1
                break

        if not lecture_title:
            continue

        body = "\n".join(lines[body_start:]).strip()
        if not body:
            continue

        content = f"Q: {title}\n\n{body}"
        enriched = f"Lecture: {lecture_title}\n{content}"
        row_count += 1
        table.declare_row(
            row=DataChunk(
                id=await id_gen.next_id(enriched),
                lecture_title=lecture_title,
                content=content,
                embedding=await embed_content(enriched),
            )
        )

    # Fallback: reading/supplement docs with a # Title and plain paragraphs
    if row_count == 0:
        lines = text.strip().splitlines()
        lecture_title = None
        body_lines = []
        for line in lines:
            m = re.match(r'^#+\s+\*?\*?(.+?)\*?\*?\s*$', line)
            if m and lecture_title is None:
                lecture_title = m.group(1).strip()
            else:
                body_lines.append(line)

        if lecture_title:
            body_text = "\n".join(body_lines)
            paragraphs = [p.strip() for p in body_text.split("\n\n") if p.strip()]
            for para in paragraphs:
                enriched = f"Lecture: {lecture_title}\n{para}"
                row_count += 1
                table.declare_row(
                    row=DataChunk(
                        id=await id_gen.next_id(enriched),
                        lecture_title=lecture_title,
                        content=para,
                        embedding=await embed_content(enriched),
                    )
                )

    print(f"[DEBUG qa] {fname}: {row_count} rows")


# @coco.fn
# async def app_main() -> None:
#     table = await postgres.mount_table_target(...)

#     drive = GoogleDriveSource(
#         service_account_credential_path=os.getenv("GDRIVE_CREDENTIALS_PATH"),
#         root_folder_ids=os.getenv("GDRIVE_ROOT_FOLDER_IDS").split(","),
#         mime_types=None,  # or filter: ["text/plain", "application/vnd.google-apps.presentation"]
#     )

#     await coco.mount_each(process_txt_file, drive.items(), table)

@coco.fn
async def app_main(corpus_dir: pathlib.Path) -> None:
    table = await postgres.mount_table_target(
        PG_DB,
        table_name=TABLE_NAME,
        table_schema=await postgres.TableSchema.from_class(
            DataChunk,
            primary_key=["id"],
        ),
    )
    table.declare_vector_index(column="embedding")

    txt_files = localfs.walk_dir(
        corpus_dir,
        live=False,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.txt"]),
    )
    await coco.mount_each(process_txt_file, txt_files.items(), table)


    md_files = localfs.walk_dir(
        corpus_dir,
        live=False,
        recursive=True,
        path_matcher=PatternFilePathMatcher(included_patterns=["**/*.md"]),
    )
    await coco.mount_each(process_qa_md, md_files.items(), table)

    gdrive_creds = os.getenv("GDRIVE_CREDENTIALS_PATH")
    gdrive_folders = os.getenv("GDRIVE_ROOT_FOLDER_IDS")
    if not gdrive_creds or not gdrive_folders:
        raise RuntimeError("GDRIVE_CREDENTIALS_PATH and GDRIVE_ROOT_FOLDER_IDS must be set in .env")

    folder_ids = gdrive_folders.split(",")

    slides_drive = GoogleDriveSource(
        service_account_credential_path=gdrive_creds,
        root_folder_ids=folder_ids,
        mime_types=["application/vnd.google-apps.presentation", "text/plain"],
    )
    await coco.mount_each(process_txt_file, slides_drive.items(), table)

    docs_drive = GoogleDriveSource(
        service_account_credential_path=gdrive_creds,
        root_folder_ids=folder_ids,
        mime_types=["application/vnd.google-apps.document"],
    )
    await coco.mount_each(process_qa_md, docs_drive.items(), table)


app = coco.App(
    coco.AppConfig(name="LectureRAG"),
    app_main,
    corpus_dir=pathlib.Path(__file__).parent / "data_corpus",
)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="Monitor for changes and re-index automatically")
    args = parser.parse_args()
    app.update_blocking(live=args.live, report_to_stdout=True)
