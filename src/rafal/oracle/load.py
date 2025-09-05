import typing

import neo4j as n4
import oracledb

_DBDepViewType = typing.Literal[
    "DBA_DEPENDENCIES", "USER_DEPENDENCIES", "ALL_DEPENDENCIES"
]


class _DependencyRow(typing.TypedDict):
    owner: str
    name: str
    type: str
    referenced_owner: str
    referenced_name: str
    referenced_type: str
    referenced_link_name: str | None
    dependency_type: str | None


async def load_dependencies(
    con: oracledb.AsyncConnection,
    dep_view: _DBDepViewType = "ALL_DEPENDENCIES",
    owner: str | None = None,
    obj_type: list[str] | None = None,
    batch_size: int = 100,
) -> typing.AsyncGenerator[list[_DependencyRow], None]:
    """Load dependencies from the database.

    Args:
        con: The Oracle connection object.
        dep_view: The dependency view to use. Defaults to "ALL_DEPENDENCIES".
        owner: Only include objects owned by the specified user
        obj_type: Only include objects of the specified types
        batch_size: Number of rows to fetch at once
    """
    # people could do funny SQL injection stuff here as I'm using string interpolation
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
        columns = [col.name.lower() for col in cur.description]
        cur.rowfactory = lambda *args: dict(zip(columns, args))
        while True:
            rows = await cur.fetchmany(batch_size)
            if not rows:
                break
            yield rows


async def build_neo4j_graph(
    driver: n4.AsyncDriver, batch: list[_DependencyRow], db: str = "neo4j"
) -> n4.EagerResult:
    # TODO: can i somehow use CREATE instead of MERGE to make all the nodes?
    # MERGE first checks if that node exists before creating, which should NOT
    # be the common case in this usecase
    return await driver.execute_query(
        """
        UNWIND $batch AS row
        MERGE (dependent:Object { owner: row.owner, name: row.name, type: row.type })
        MERGE (referenced:Object { owner: row.referenced_owner, name: row.referenced_name, type: row.referenced_type })
        MERGE (dependent)-[:DEPENDS_ON]->(referenced)
        """,
        batch=batch,
        database_=db,
    )
