import re
from typing import Optional

from litestar.exceptions import HTTPException


def parse_pg_detail(detail: str | None) -> Optional[dict[str, str]]:
    """Extract column names and values from a Postgres error 'detail' string.

    "Key (map_id, mechanic_id)=(1, 2) already exists."
    Returns a dict: {'map_id': '1', 'mechanic_id': '2'}
    Returns None if no match is found.

    Args:
        detail (str): Postgres error 'detail' string.

    Returns:
        Optional[dict[str, str]]: Column names and values.

    """
    if detail is None:
        return None
    match = re.search(r"\((.*?)\)=\((.*?)\)", detail)
    if match:
        columns = [col.strip() for col in match.group(1).split(",")]
        values = [val.strip() for val in match.group(2).split(",")]
        return dict(zip(columns, values))
    return None


class CustomHTTPException(HTTPException): ...
