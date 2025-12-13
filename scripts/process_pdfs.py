#!/usr/bin/env python3
"""Main CLI entry point for PDF processing.

챕터 기반 계층적 PDF 처리.
챕터 → 섹션 → 문단 → 아이디어 추출 파이프라인.
Supports resume capability and progress tracking.
"""

import sys
import os
import argparse
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.orchestrator.batch import process_pdf
from src.utils.logger import setup_logger


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Process PDFs to extract core ideas for knowledge graph",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process a new PDF
  python scripts/process_pdfs.py --pdf /path/to/book.pdf

  # Resume processing for book ID 5
  python scripts/process_pdfs.py --resume --book-id 5

  # Process with custom model version
  python scripts/process_pdfs.py --pdf book.pdf --model gemini-2.0-pro

  # Enable debug logging
  python scripts/process_pdfs.py --pdf book.pdf --log-level DEBUG
        """
    )

    # Required arguments (one of --pdf or --resume)
    parser.add_argument(
        "--pdf",
        type=str,
        help="Path to PDF file to process"
    )

    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume processing from last checkpoint"
    )

    parser.add_argument(
        "--book-id",
        type=int,
        help="Book ID for resume mode (required with --resume)"
    )

    # Optional arguments
    parser.add_argument(
        "--model",
        type=str,
        default="gemini-2.5-flash",
        help="LLM model version (default: gemini-2.5-flash)"
    )

    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )

    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Output logs in JSON format (for production)"
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.resume and not args.pdf:
        parser.error("Either --pdf or --resume is required")

    if args.resume and not args.book_id:
        parser.error("--book-id is required when using --resume")

    if args.resume and args.pdf:
        parser.error("Cannot specify both --pdf and --resume")

    # Setup logging
    logger = setup_logger(
        name="pdf_processor",
        level=args.log_level,
        json_format=args.json_logs
    )

    try:
        logger.info("=" * 60)
        logger.info("PDF Processing Started")
        logger.info("=" * 60)

        if args.resume:
            logger.info(f"Resume mode: Book ID {args.book_id}")
            logger.info(f"Model version: {args.model}")

            stats = process_pdf(
                pdf_path="",  # Not needed for resume
                resume=True,
                book_id=args.book_id,
                model_version=args.model,
            )

        else:
            # Validate PDF file exists
            pdf_path = Path(args.pdf)
            if not pdf_path.exists():
                logger.error(f"PDF file not found: {args.pdf}")
                sys.exit(1)

            logger.info(f"Processing PDF: {pdf_path.absolute()}")
            logger.info(f"Model version: {args.model}")

            stats = process_pdf(
                pdf_path=str(pdf_path.absolute()),
                resume=False,
                book_id=None,
                model_version=args.model,
            )

        # Check for errors
        if stats.get("error"):
            logger.error(f"Processing failed: {stats['error']}")
            sys.exit(1)

        logger.info("=" * 60)
        logger.info("PDF Processing Completed Successfully")
        logger.info("=" * 60)
        sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("\n⚠️  Processing interrupted by user")
        logger.info("You can resume later using --resume --book-id <id>")
        sys.exit(130)

    except Exception as e:
        logger.error(f"Fatal error: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
