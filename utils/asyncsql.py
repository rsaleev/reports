import aiomysql
import asyncio
import time


"""[summary]

Returns:
   AsyncDBPool object
     attributes:
       conn  - dictionary object with connection params
       
"""


class AsyncDBPool():

    def __init__(self, conn: dict, loop: None):
        self.conn = conn
        self.pool = None
        self.connected = False
        self.loop = loop

    async def connect(self):
        while self.pool is None:
            try:
                self.pool = await aiomysql.create_pool(**self.conn, loop=self.loop, autocommit=True)
                self.connected = True
                return self
            except aiomysql.OperationalError as e:
                code, description = e.args
                if code == 2003 or 1053:
                    await asyncio.sleep(0.5)
                    continue

    async def callproc(self, procedure: str,  rows: int, values: list = None):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.callproc(procedure, [*values])
                    result = None
                    if rows == 1:
                        result = await cur.fetchone()
                    if rows > 1:
                        result = await cur.fetchmany(rows)
                    elif rows == -1:
                        result = await cur.fetchall()
                    elif rows == 0:
                        pass
                    await cur.close()
                    return result
        except aiomysql.OperationalError as e:
            code, description = e.args
            if code == 2003 or 1053:
                await self.connect()
            else:
                raise

    async def execute(self, stmt: str, rows: None, *args):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute(stmt, *args)
                    result = None
                    if rows == 1:
                        result = await cur.fetchone()
                    elif rows > 1:
                        result = await cur.fetchmany(rows)
                    elif rows == -1:
                        result = await cur.fetchall()
                    elif rows == 0:
                        pass
                    await cur.close()
                    return result
        except aiomysql.OperationalError as e:
            code, description = e.args
            if code == 2003 or 1053:
                await self.connect()
            else:
                raise
