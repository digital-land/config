"""
Comprehensive test suite for batch_assign_entities.py

Tests cover:
- Unit tests for utility functions
- Integration tests with mocked external dependencies
- Edge cases and error handling
- CSV processing and validation logic
"""

import pytest
import pandas as pd
import subprocess
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
from io import StringIO
import tempfile
import shutil

import sys
import os
sys.path.insert(0, str(Path(__file__).parent.parent / "bin"))

from batch_assign_entities import (
    get_scope,
    _make_fingerprints,
    run_command,
    download_file,
    download_urls,
    get_old_resource_df,
    process_csv,
    ensure_specification_dir,
)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
