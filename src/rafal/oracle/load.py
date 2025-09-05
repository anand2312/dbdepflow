import typing

import oracledb

_DBDepViewType = typing.Literal[
    "DBA_DEPENDENCIES", "USER_DEPENDENCIES", "ALL_DEPENDENCIES"
]


async def load_dependencies(
    con: oracledb.AsyncConnection,
    dep_view: _DBDepViewType = "ALL_DEPENDENCIES",
    owner: str | None = None,
    obj_type: list[str] | None = None,
) -> typing.AsyncGenerator[tuple, None]:
    """Load dependencies from the database.

    Args:
        con: The Oracle connection object.
        dep_view: The dependency view to use. Defaults to "ALL_DEPENDENCIES".
        owner: Only include objects owned by the specified user
        obj_type: Only include objects of the specified types
    """
    # people could do funny SQL injection stuff here as I'm using string interpolation
    # in the query to select the dep view
    if dep_view not in ("DBA_DEPENDENCIES", "USER_DEPENDENCIES", "ALL_DEPENDENCIES"):
        raise ValueError(f"Invalid dependency view: {dep_view}")

    query = f"SELECT * FROM {dep_view}"
    kwargs = {}
    if owner:
        query += " WHERE owner = :owner"
        kwargs["owner"] = owner

    sep = "WHERE" if owner is None else "AND"
    if obj_type:
        ot = "(" + ",".join([f"'{v}'" for v in obj_type]) + ")"
        query += " " + sep + f" type in {ot}"
    print(query)
    print(kwargs)
    with con.cursor() as cur:
        await cur.execute(query, **kwargs)
        while True:
            row = await cur.fetchone()
            if row is None:
                break
            yield row


async def build_neo4j_graph() -> ...: ...
