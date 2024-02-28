from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
import os
from fastapi import FastAPI, Request, HTTPException
from psycopg_pool import AsyncConnectionPool
from psycopg import DatabaseError


def get_conn_str():
    return f"""
    dbname=rinha
    user=rinha
    password=rinha
    host=localhost
    port=5432
    """

@asynccontextmanager
async def lifespan(app: FastAPI):
    max_connections = os.getenv("MAX_CONNECTIONS", 10)
    app.async_pool = AsyncConnectionPool(
        kwargs={"autocommit": True}, 
        max_size=int(max_connections), 
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
            await cur.execute("""
                SELECT balance, account_limit
                FROM accounts
                WHERE id = %s;
            """, (id,))
            account = await cur.fetchone()
            if account == None:
                raise HTTPException(status_code=404)
            
            await cur.execute("""
                SELECT value, transaction_type, description, created_at
                FROM transactions
                WHERE account_id = %s
                ORDER BY created_at DESC
                LIMIT 10;
            """, (id,))
            transactions = await cur.fetchall()
            transactions_response = []
            if len(transactions) > 0: 
                for transaction in transactions:
                    transaction_base = TransactionResponse(valor=transaction[0], tipo=transaction[1], descricao=transaction[2], realizada_em=transaction[3])
                    transactions_response.append(transaction_base)
 
            return { "saldo": { "total": account[0], "data_extrato": datetime.now(), "limite": account[1] } , "ultimas_transacoes": transactions_response }
