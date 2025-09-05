import asyncio

import neo4j
import oracledb

from rafal.oracle.load import build_neo4j_graph, load_dependencies


async def main() -> None:
    # assuming the containers defined in dev-compose.yml are up:
    uri, auth = "neo4j://localhost", ("neo4j", "adminadmin")
    async with neo4j.AsyncGraphDatabase.driver(uri, auth=auth) as driver:
        async with oracledb.connect_async(
            user="SYSTEM", password="admin", dsn="localhost:1521/free"
        ) as con:
            async for rows in load_dependencies(
                con, obj_type=["PROCEDURE", "FUNCTION", "PACKAGE"]
            ):
                await build_neo4j_graph(driver, rows)
                print("Added batch of nodes")


asyncio.run(main())
