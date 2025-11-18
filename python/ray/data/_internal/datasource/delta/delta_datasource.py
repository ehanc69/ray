"""
Delta Lake datasource implementation for reading Delta tables.
"""

import logging
from typing import Any, Dict, List, Optional, Union

from ray.data._internal.util import _check_import
from ray.data.datasource import Datasource, ReadTask
from ray.data.datasource.partitioning import Partitioning
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class DeltaDatasource(Datasource):
    """Datasource for reading Delta Lake tables with Ray Data."""

    def __init__(
        self,
        path: str,
        *,
        version: Optional[Union[int, str]] = None,
        storage_options: Optional[Dict[str, str]] = None,
        partition_filters: Optional[List[tuple]] = None,
        filesystem: Optional[Any] = None,
        columns: Optional[List[str]] = None,
        partitioning: Partitioning = Partitioning("hive"),
        **arrow_parquet_args,
    ):
        """Initialize Delta Lake datasource.

        Args:
            path: Path to Delta Lake table.
            version: Version to read for time travel (integer or ISO 8601 timestamp).
            storage_options: Cloud storage authentication credentials.
            partition_filters: Delta Lake partition filters as list of tuples.
            filesystem: PyArrow filesystem for reading files.
            columns: List of column names to read.
            partitioning: Partitioning scheme for reading files.
            **arrow_parquet_args: Additional arguments passed to PyArrow parquet reader.
        """
        _check_import(self, module="deltalake", package="deltalake")

        if not isinstance(path, str):
            raise ValueError(
                "Only single Delta table path supported (not list of paths)"
            )

        self.path = path
        self.version = version
        self.storage_options = storage_options or {}
        self.partition_filters = partition_filters
        self.filesystem = filesystem
        self.columns = columns
        self.partitioning = partitioning
        self.arrow_parquet_args = arrow_parquet_args
        self._delta_table = None

    @property
    def delta_table(self):
        """Lazy-load Delta table object."""
        if self._delta_table is None:
            from deltalake import DeltaTable

            dt_kwargs = {}
            if self.storage_options:
                dt_kwargs["storage_options"] = self.storage_options
            if self.version is not None:
                dt_kwargs["version"] = self.version
            self._delta_table = DeltaTable(self.path, **dt_kwargs)
        return self._delta_table

    def get_file_paths(self) -> List[str]:
        """Get list of Parquet file paths from Delta table."""
        if self.partition_filters is not None:
            return self.delta_table.file_uris(partition_filters=self.partition_filters)
        return self.delta_table.file_uris()

    def get_read_tasks(
        self, parallelism: int, per_task_row_limit: Optional[int] = None
    ) -> List[ReadTask]:
        """Get read tasks for Delta table snapshot reads."""
        file_paths = self.get_file_paths()
        if not file_paths:
            return []

        from ray.data._internal.datasource.parquet_datasource import ParquetDatasource

        parquet_datasource = ParquetDatasource(
            file_paths,
            columns=self.columns,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            **self.arrow_parquet_args,
        )

        return parquet_datasource.get_read_tasks(parallelism, per_task_row_limit)

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate in-memory data size for the Delta table."""
        return None

    def get_name(self) -> str:
        """Return human-readable name for this datasource."""
        return "DeltaLake"

    def get_table_version(self) -> int:
        """Get current Delta table version."""
        return self.delta_table.version()

    def get_table_schema(self):
        """Get Delta table schema."""
        return self.delta_table.schema().to_pyarrow()

    def get_table_metadata(self) -> Dict[str, Any]:
        """Get Delta table metadata."""
        dt = self.delta_table
        file_paths = self.get_file_paths()
        return {
            "version": dt.version(),
            "num_files": len(file_paths),
            "schema": dt.schema().to_pyarrow(),
            "partition_columns": dt.metadata().partition_columns,
        }

    def __repr__(self) -> str:
        """String representation of datasource."""
        version_info = f", version={self.version}" if self.version else ""
        return f"DeltaDatasource(path={self.path}{version_info})"
