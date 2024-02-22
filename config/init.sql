CREATE TABLE accounts (
    id SERIAL PRIMARY KEY,
    account_limit integer,
    balance integer
);

CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    value integer,
    transaction_type character varying(10),
    description character varying(10),
    created_at timestamp without time zone,
    account_id integer NOT NULL,
    CONSTRAINT fk_accounts_transactions_id
		FOREIGN KEY (account_id) REFERENCES accounts(id)
);

DO $$
BEGIN
	INSERT INTO accounts (account_limit, balance) 
    VALUES (100000, 0), (80000, 0),	(1000000, 0), (10000000, 0), (500000, 0);
END;
$$;

CREATE OR REPLACE FUNCTION CreateTransaction(
    IN accountId INT, 
    IN amount INT,
    IN description VARCHAR,
    IN transactionType VARCHAR
)
RETURNS TABLE (limite INT, saldo INT)
LANGUAGE plpgsql    
AS $$
DECLARE
    actualLimit INT;
    actualBalance INT;
BEGIN
    SELECT ac.account_limit, ac.balance INTO actualLimit, actualBalance  
    FROM accounts ac
    WHERE id = accountId;

    IF transactionType = 'd' THEN
        IF amount > actualBalance + actualLimit THEN
            RAISE EXCEPTION 'Valor é maior que o seu saldo + limite';
        END IF;

        IF amount > actualBalance AND amount < (actualBalance + actualLimit) THEN
            actualBalance = actualBalance - amount;
            actualLimit = actualLimit - abs(actualBalance);
        END IF;

        IF (amount - abs(actualLimit) < 0) THEN
            RAISE EXCEPTION 'Valor ultrapassa os seus limites';
        END IF;

        UPDATE accounts
        SET balance = actualBalance,
        account_limit = actualLimit
        WHERE id = accountId;
    END IF;

    IF transactionType = 'c' THEN
        UPDATE accounts
        SET balance = balance + amount
        WHERE id = accountId;
    END IF;

    INSERT INTO transactions (account_id, value, transaction_type, description, created_at)
    VALUES (accountId, amount, transactionType, description, current_timestamp);

    RETURN QUERY SELECT c.account_limit as limite, c.balance as saldo FROM accounts c WHERE c.id = accountId;
END;$$;