from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
import enum
from fastapi import FastAPI, Request, HTTPException
from psycopg_pool import AsyncConnectionPool
from psycopg import DatabaseError


def get_conn_str():
    #local
    # return f"""
    # dbname=rinha
    # user=thiago
    # password=
    # host=127.0.0.1
    # port=5432
    # """
    # docker
    return f"""
    dbname=rinha
    user=admin
    password=123
    host=db
    port=5432
    """

@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"Creating connection pool to {get_conn_str()}")
    app.async_pool = AsyncConnectionPool(kwargs={"autocommit": True}, conninfo=get_conn_str())
    yield
    await app.async_pool.close()


app = FastAPI(lifespan=lifespan)

@dataclass
class TransactionRequest:
    valor: int
    tipo: str
    descricao: str
    
class TransactionType(enum.Enum):
    credit = "c"
    debit = "d"
    
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

def reaching_limit(balance, limit_amount, amount):
    if (balance - amount) > limit_amount:
        return False
    return abs(balance - amount) > limit_amount
    

@app.get("/clientes")
async def get_accounts(request: Request):
    async with request.app.async_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                SELECT * 
                FROM accounts
            """)
            results = await cur.fetchall()
            return results

@app.post("/clientes/{id}/transacoes")
async def post_transaction(request: Request, id: int, transaction: TransactionRequest):
    print(f"transaction: {transaction}")
    async with request.app.async_pool.connection() as conn:
        async with conn.cursor() as cur:
            
            if transaction.descricao == None or len(transaction.descricao) == 0 or len(transaction.descricao) > 10:
                raise HTTPException(status_code=422)

            transaction_value = validate_value(value=transaction.valor)
            
            if transaction.tipo not in ["c", "d"]:
                raise HTTPException(status_code=422)

            try:
                await cur.execute("SELECT * FROM CreateTransaction(%s,%s,%s,%s)", (id, transaction_value, transaction.descricao, transaction.tipo))
                response = await cur.fetchone()
                limite = response[0]
                saldo = response[1]
                return { "limite": limite, "saldo": saldo }
            except (Exception, DatabaseError) as error:
                raise HTTPException(status_code=422, detail=f"Não é possível realizar a transação: Erro: - {error}")


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
                raise HTTPException(status_code=404, detail="Cliente não encontrado")
            
            await cur.execute("""
                SELECT value, transaction_type, description, created_at
                FROM transactions
                WHERE account_id = %s
                ORDER BY created_at DESC
                LIMIT 10;
            """, (id,))
            transactions = await cur.fetchall()
            print(f"transactions: {len(transactions)}")
            transactions_response = []
            if len(transactions) > 0: 
                for transaction in transactions:
                    transaction_base = TransactionResponse(valor=transaction[0], tipo=transaction[1], descricao=transaction[2], realizada_em=transaction[3])
                    transactions_response.append(transaction_base)
 
            return { "saldo": { "total": account[0], "data_extrato": datetime.now(), "limite": account[1], "transacoes": transactions_response } }
