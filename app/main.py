from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
import os
from fastapi import FastAPI, Request, HTTPException
from psycopg_pool import AsyncConnectionPool
from psycopg import DatabaseError


def get_conn_str():
    return f"""
    dbname={os.getenv("POSTGRES_DB", "rinha")}
    user={os.getenv("POSTGRES_USER", "thiago")}
    password={os.getenv("POSTGRES_PASSWORD", "")}
    host={os.getenv("POSTGRES_HOST", "localhost")}
    port=5432
    """

@asynccontextmanager
async def lifespan(app: FastAPI):
    max_connections = os.getenv("MAX_CONNECTIONS", 5)
    min_connections = os.getenv("MIN_CONNECTIONS", 1)
    app.async_pool = AsyncConnectionPool(
        kwargs={"autocommit": True}, 
        # max_size=int(max_connections), 
        # min_size=int(min_connections),
        conninfo=get_conn_str())
    yield
    await app.async_pool.close()


app = FastAPI(lifespan=lifespan)

@dataclass
class TransactionRequest:
    valor: int
    tipo: str
    descricao: str
    
@dataclass
class TransactionResponse:
    valor: float
    tipo: str
    descricao: str
    realizada_em: datetime 
    
def validate_value(value: float):
    absolute_value = int(value)
    if value > absolute_value:
        raise HTTPException(status_code=422)
    return absolute_value


@app.post("/clientes/{id}/transacoes")
async def post_transaction(request: Request, id: int, transaction: TransactionRequest):
    if transaction.descricao == None or len(transaction.descricao) == 0 or len(transaction.descricao) > 10:
        raise HTTPException(status_code=422)

    transaction_value = validate_value(value=transaction.valor)
    
    if transaction.tipo not in ["c", "d"]:
        raise HTTPException(status_code=422)

    async with request.app.async_pool.connection() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute("SELECT * FROM CreateTransaction(%s,%s,%s,%s)", (id, transaction_value, transaction.descricao, transaction.tipo))
                response = await cur.fetchone()
                limite = response[0]
                saldo = response[1]
                return { "limite": limite, "saldo": saldo }
            except (Exception, DatabaseError) as _:
                raise HTTPException(status_code=422)

        
@app.get("/clientes/{id}/extrato")
async def get_balance_and_transactions(request: Request, id: int):
    async with request.app.async_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT 
                    a.balance, 
                    a.account_limit,
                    t.value, 
                    t.transaction_type, 
                    t.description, 
                    t.created_at
                FROM
                    accounts a LEFT JOIN (
                        SELECT * FROM transactions 
                        WHERE account_id = %s 
                        ORDER BY created_at DESC 
                        LIMIT 10
                    ) t on a.id = t.account_id
                WHERE  
                    a.id = %s
                """,
                [id, id],
            )

            response = await cur.fetchall()
            balance_result = 0
            limit_result = 0

            if response is None or len(response) == 0:
                raise HTTPException(status_code=404)

            transactions_response = []
            for transaction in response:
                balance_result = transaction[0]
                limit_result = transaction[1]
                if transaction[2] is None:
                    continue
                transactions_response.append(
                    TransactionResponse(
                        valor=transaction[2],
                        tipo=transaction[3],
                        descricao=transaction[4],
                        realizada_em=transaction[5],
                    )
                )

            return { "saldo": { "total": balance_result, "data_extrato": datetime.now(), "limite": limit_result } , "ultimas_transacoes": transactions_response }
