"""
Delta Lake utility functions for credential management and table operations.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from deltalake import DeltaTable


def convert_pyarrow_filter_to_sql(
    filters: Optional[
        List[Union[Tuple[str, str, Any], Tuple[Tuple[str, str, Any], ...]]]
    ],
) -> Optional[str]:
    """Convert PyArrow partition filters to Delta Lake SQL predicate format."""
    if not filters:
        return None

    def format_value(value: Any) -> str:
        """Format a single value for SQL with proper escaping."""
        if value is None:
            return "NULL"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        elif isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            str_value = str(value)
            escaped = str_value.replace("'", "''")
            return f"'{escaped}'"

    def format_condition(col: str, op: str, value: Any) -> str:
        """Format a single filter condition as SQL expression."""
        op_upper = op.upper()
        if op_upper in ("IN", "NOT IN"):
            if not isinstance(value, (list, tuple)):
                raise ValueError(
                    f"IN/NOT IN operator requires list or tuple value, got {type(value).__name__}"
                )
            formatted_values = ", ".join(format_value(v) for v in value)
            return f"{col} {op_upper} ({formatted_values})"
        return f"{col} {op} {format_value(value)}"

    sql_parts = []
    for filter_item in filters:
        if not isinstance(filter_item, (tuple, list)):
            raise ValueError(
                f"Each filter must be a tuple or list, got {type(filter_item).__name__}"
            )

        if len(filter_item) > 0 and isinstance(filter_item[0], (tuple, list)):
            conditions = []
            for condition in filter_item:
                if not isinstance(condition, (tuple, list)) or len(condition) != 3:
                    raise ValueError(
                        f"Each condition in conjunctive filter must be a 3-tuple, got {condition}"
                    )
                col, op, value = condition
                conditions.append(format_condition(col, op, value))
            sql_parts.append(f"({' AND '.join(conditions)})")
        else:
            if len(filter_item) != 3:
                raise ValueError(
                    f"Simple filter must be a 3-tuple, got {len(filter_item)} elements: {filter_item}"
                )
            col, op, value = filter_item
            sql_parts.append(format_condition(col, op, value))

    return sql_parts[0] if len(sql_parts) == 1 else " OR ".join(sql_parts)


def _get_aws_storage_options() -> Dict[str, str]:
    """Get S3 storage options from boto3 credentials."""
    import boto3

    session = boto3.Session()
    credentials = session.get_credentials()
    if not credentials:
        return {}

    storage_options = {
        "AWS_ACCESS_KEY_ID": credentials.access_key,
        "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
        "AWS_REGION": session.region_name or "us-east-1",
    }
    if credentials.token:
        storage_options["AWS_SESSION_TOKEN"] = credentials.token
    return storage_options


def _get_azure_storage_options() -> Dict[str, str]:
    """Get Azure storage options from DefaultAzureCredential."""
    from azure.identity import DefaultAzureCredential

    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default")
    return {"AZURE_STORAGE_TOKEN": token.token}


def try_get_deltatable(
    table_uri: str, storage_options: Optional[Dict[str, str]] = None
) -> Optional["DeltaTable"]:
    """Get a DeltaTable object if it exists, return None otherwise."""
    from deltalake import DeltaTable

    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except FileNotFoundError:
        return None


def get_storage_options(
    path: str, provided: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """Get storage options with auto-detection for cloud paths."""
    provided = provided or {}
    auto_options = {}

    if path.lower().startswith(("s3://", "s3a://")):
        try:
            auto_options = _get_aws_storage_options()
        except ImportError:
            pass
    elif path.lower().startswith(("abfss://", "abfs://")):
        try:
            auto_options = _get_azure_storage_options()
        except ImportError:
            pass

    return {**auto_options, **provided}
