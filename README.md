- Get all nifty 250 stocks from official nifty website

- Assumptions
  - 1 exchange operates in only 1 country (there are some exchanges that operate throughout europe but ignoring them)
  - all tickers in an exchange are traded in the same currency and the currency doesn't change (it might but very rarely)
  
- Database schema
- exchange
  - id  not null primary key varchar
  - short_name  not null varchar unique
  - name  not null varchar
  - currency  varchar not null enum ('INR', 'USD')
  - country_code  varchar not null enum ('USA', 'IND')
  - other_data  jsonb not null
  - created_on  not null timestampz
  - updated_on  not null timestampz

- ticker
  - id  not null primary key varchar
  - exchange_id  references exchange.id not null
  - symbol  varchar not null
  - yahoo_symbol  varchar not null
  - name  varchar not null
  - type  varchar not null
  - listed_on  timestampz
  - other_data  jsonb not null
  - created_on  not null timestampz
  - updated_on  not null timestampz
UNIQUE(exchange_id, symbol)
UNIQUE(exchange_id, yahoo_symbol)

- ticker_day_trade
  - id  not null primary key varchar
  - ticker_id  references ticker.id not null
  - date (dd-mm-yyyy)  not null date col
  - open  numeric not null
  - high  numeric not null
  - low  numeric not null
  - close  numeric not null
  - volume  numeric not null
  - dividends  numeric not null
  - stock_splits  numeric not null
  
  - share_price  numeric not null
  - float_shares  int not null
  - total_shares  int not null
  - market_cap  numeric not null
  
  - other_data  jsonb not null
  - created_on timestampz not null
  - updated_on timestampz not null
UNIQUE(ticker_id, date)
  