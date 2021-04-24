package erc20

import (
	"context"
	"errors"

	"github.com/ninja-software/terror/v2"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/zap"
)

var log *zap.SugaredLogger

func init() {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	log = l.Sugar()
}

type Address uuid.UUID

const Migration = `
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE account_books (
	id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid ()
)
CREATE TABLE tokens (
	id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid (),
	account_book_id UUID NOT NULL REFERENCES account_books(id),
	name TEXT NOT NULL,
	symbol TEXT UNIQUE NOT NULL,
	decimals INTEGER NOT NULL,
	total_supply INTEGER NOT NULL
);
CREATE INDEX idx_tokens_symbol ON tokens (symbol)
CREATE TABLE addresses (
	id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid ()
	token_id UUID REFERENCES tokens(id),
	balance INTEGER NOT NULL
);
CREATE INDEX idx_addresses_token ON addresses (token_id)
`

// Factory creates a new token
func Factory(conn *pgxpool.Pool, name string, symbol string, decimals int, totalSupply int) error {
	ctx := context.Background()
	err := conn.BeginFunc(ctx, func(tx pgx.Tx) error {
		q := `INSERT INTO tokens (name, symbol, decimals, total_supply) VALUES ($1, $2, $3, $4);`
		tx.Exec(ctx, q, name, symbol, decimals, totalSupply)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// TokenIDBySymbol retrieves the token ID given its unique symbol
func TokenIDBySymbol(conn *pgxpool.Pool, name string) (uuid.UUID, error) {
	ctx := context.Background()
	q := `SELECT id FROM tokens WHERE symbol = $1`
	var tokenID uuid.UUID
	row := conn.QueryRow(ctx, q, name)
	err := row.Scan(&tokenID)
	if err != nil {
		log.Errorw(err.Error(), "id", name)
		return uuid.Nil, terror.Error(err, "Could not fetch from database")
	}
	return tokenID, nil
}

// AddressByAccountBookIDSymbol retrieves the address given a symbol and account book ID
// It will create an address on the fly if not found
func AddressByAccountBookIDSymbol(conn *pgxpool.Pool, symbol string, accountBookID uuid.UUID) (uuid.UUID, error) {
	ctx := context.Background()
	countQ := `
SELECT count(addresses.id) FROM addresses
JOIN tokens ON tokens.id = addresses.id
JOIN account_books ON account_books.id = tokens.account_book_id
WHERE tokens.symbol = $1 AND account_book_id = $2`
	var count int
	row := conn.QueryRow(ctx, countQ, symbol, accountBookID)
	err := row.Scan(&count)
	if err != nil {
		log.Errorw(err.Error(), "symbol", symbol, "accountBookID", accountBookID)
		return uuid.Nil, terror.Error(err, "Could not get count addresses")
	}
	if count == 0 {
		var addressID uuid.UUID
		tokenID, err := TokenIDBySymbol(conn, symbol)
		if err != nil {
			return uuid.Nil, terror.Error(err, "Could not get address")
		}
		insertQ := `INSERT INTO addresses (token_id, balance) VALUES ($1, $2) RETURNING id;`
		err = conn.BeginFunc(ctx, func(tx pgx.Tx) error {
			row, err := tx.Query(ctx, insertQ, tokenID, 0)
			if err != nil {
				return err
			}
			err = row.Scan(&tokenID)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return uuid.Nil, terror.Error(err, "Could not insert new address")
		}
		return addressID, nil
	}

	addressQ := `
SELECT addresses.id FROM addresses
JOIN tokens ON tokens.id = addresses.id
JOIN account_books ON account_books.id = tokens.account_book_id
WHERE tokens.symbol = $1 AND account_book_id = $2`
	var addressID uuid.UUID
	row = conn.QueryRow(ctx, addressQ, symbol, accountBookID)
	err = row.Scan(&addressID)
	if err != nil {
		log.Errorw(err.Error(), "symbol", symbol, "accountBookID", accountBookID)
		return uuid.Nil, terror.Error(err, "Could not get address")
	}
	return addressID, nil
}

// Name returns the name of the token.
// Not unique.
func Name(conn *pgxpool.Pool, tokenID uuid.UUID) (string, error) {
	ctx := context.Background()
	q := `SELECT name FROM tokens WHERE id = $1`
	var name string
	row := conn.QueryRow(ctx, q, tokenID)
	err := row.Scan(&name)
	if err != nil {
		log.Errorw(err.Error(), "id", tokenID)
		return "", terror.Error(err, "Could not get name")
	}
	return name, nil
}

// Symbol returns the shorthand version of the token name
// Unique. Indexed.
func Symbol(conn *pgxpool.Pool, tokenID uuid.UUID) (string, error) {
	ctx := context.Background()
	q := `SELECT symbol FROM tokens WHERE id = $1`
	var symbol string
	row := conn.QueryRow(ctx, q, tokenID)
	err := row.Scan(&symbol)
	if err != nil {
		log.Errorw(err.Error(), "id", tokenID)
		return "", terror.Error(err, "Could not get symbol")
	}
	return symbol, nil
}

// Decimals returns the numbers for user representation
// Default is 18
// Not changable
func Decimals(conn *pgxpool.Pool, tokenID uuid.UUID) (int, error) {
	ctx := context.Background()
	q := `SELECT decimals FROM tokens WHERE id = $1`
	var decimals int
	row := conn.QueryRow(ctx, q, tokenID)
	err := row.Scan(&decimals)
	if err != nil {
		log.Errorw(err.Error(), "id", tokenID)
		return 0, terror.Error(err, "Could not get decimals")
	}
	return decimals, nil
}

// TotalSupply of the token
func TotalSupply(conn *pgxpool.Pool, tokenID uuid.UUID) (int, error) {
	ctx := context.Background()
	q := `SELECT total_supply FROM tokens WHERE id = $1`
	var totalSupply int
	row := conn.QueryRow(ctx, q, tokenID)
	err := row.Scan(&totalSupply)
	if err != nil {
		log.Errorw(err.Error(), "id", tokenID)
		return 0, terror.Error(err, "Could not get total supply")
	}
	return totalSupply, nil
}

// BalanceOf an address
// Creates the address if it doesn't exist
func BalanceOf(conn *pgxpool.Pool, tokenID uuid.UUID, owner Address) (int, error) {
	ctx := context.Background()
	q := `SELECT balance FROM addresses WHERE id = $1`
	var totalSupply int
	row := conn.QueryRow(ctx, q, owner)
	err := row.Scan(&totalSupply)
	if err != nil {
		err := conn.BeginFunc(ctx, func(tx pgx.Tx) error {
			q := `INSERT INTO addresses (token_id, balance) VALUES ($1, $2);`
			_, err := tx.Exec(ctx, q, tokenID, 0)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Errorw(err.Error(), "tokenID", tokenID, "owner", owner)
			return 0, terror.Error(err, "Could not insert address")
		}
		return 0, nil
	}
	return totalSupply, nil
}

// TransferFrom moves balance between accounts
func TransferFrom(conn *pgxpool.Pool, tokenID uuid.UUID, sender Address, recipient Address, amount int) (bool, error) {
	ctx := context.Background()
	senderBal, err := BalanceOf(conn, tokenID, sender)
	if err != nil {
		return false, terror.Error(err, "get balance")
	}
	recipientBal, err := BalanceOf(conn, tokenID, recipient)
	if err != nil {
		return false, terror.Error(err, "get balance")
	}
	err = conn.BeginFunc(ctx, func(tx pgx.Tx) error {
		if senderBal < amount {
			return errors.New("ERC20: transfer amount exceeds balance")
		}
		setBalanceQ := `UPDATE addresses SET balance = $1`
		var err error
		_, err = tx.Exec(ctx, setBalanceQ, recipientBal+amount)
		if err != nil {
			log.Errorw(err.Error(), "sender", sender, "recipient", recipient, "amount", amount)
			return err
		}
		_, err = tx.Exec(ctx, setBalanceQ, senderBal-amount)
		if err != nil {
			log.Errorw(err.Error(), "sender", sender, "recipient", recipient, "amount", amount)
			return err
		}
		return nil
	})
	if err != nil {
		log.Errorw(err.Error(), "sender", sender, "recipient", recipient, "amount", amount)
		return false, terror.Error(err, "Could not update balances")
	}
	return true, nil
}

// Mint new tokens to an address
func Mint(conn *pgxpool.Pool, tokenID uuid.UUID, account Address, amount int) error {
	ctx := context.Background()
	bal, err := BalanceOf(conn, tokenID, account)
	if err != nil {
		return terror.Error(err, "get balance")
	}
	totalSupply, err := TotalSupply(conn, tokenID)
	if err != nil {
		return terror.Error(err, "get total supply")
	}
	err = conn.BeginFunc(ctx, func(tx pgx.Tx) error {
		setBalanceQ := `UPDATE addresses SET balance = $1`
		var err error
		_, err = tx.Exec(ctx, setBalanceQ, bal+amount)
		if err != nil {
			log.Errorw(err.Error(), "account", account, "amount", amount)
			return err
		}
		setTotalSupplyQ := `UPDATE tokens SET total_supply = $1`
		_, err = tx.Exec(ctx, setTotalSupplyQ, totalSupply+amount)
		if err != nil {
			log.Errorw(err.Error(), "account", account, "amount", amount)
			return err
		}
		return nil
	})
	if err != nil {
		log.Errorw(err.Error(), "account", account, "amount", amount)
		return terror.Error(err, "Could not update balances")
	}
	return nil
}

// Burn existing tokens from an address
func Burn(conn *pgxpool.Pool, tokenID uuid.UUID, account Address, amount int) error {
	ctx := context.Background()
	bal, err := BalanceOf(conn, tokenID, account)
	if err != nil {
		return terror.Error(err, "get balance")
	}
	totalSupply, err := TotalSupply(conn, tokenID)
	if err != nil {
		return terror.Error(err, "get total supply")
	}
	err = conn.BeginFunc(ctx, func(tx pgx.Tx) error {
		if bal < amount {
			return errors.New("ERC20: burn amount exceeds balance")
		}
		setBalanceQ := `UPDATE addresses SET balance = $1`
		var err error
		_, err = tx.Exec(ctx, setBalanceQ, bal-amount)
		if err != nil {
			log.Errorw(err.Error(), "account", account, "amount", amount)
			return err
		}
		setTotalSupplyQ := `UPDATE tokens SET total_supply = $1`
		_, err = tx.Exec(ctx, setTotalSupplyQ, totalSupply-amount)
		if err != nil {
			log.Errorw(err.Error(), "account", account, "amount", amount)
			return err
		}
		return nil
	})
	if err != nil {
		log.Errorw(err.Error(), "account", account, "amount", amount)
		return terror.Error(err, "Could not update balances")
	}
	return nil
}
