- Get all nifty 250 stocks from official nifty website

- stock market is open for 250 days / year
- assuming 1000 companies, we have 250 * 1000 = 250k records / year
- if I scrape past 10 years, then it becomes 2.5 mil records, with a rate of increase of 250k per year
  postgres should be able to handle this

- Database schema
- ticker_day_trade
  - id  not null primary key varchar
  - exchange_symbol  not null varchar
  - ticker_symbol  varchar not null
  - ticker_symbol_yahoo  not null
  - trading_date (dd-mm-yyyy)  not null date
  - currency  not null varchar
  - country_code  not null varchar
  - type  varchar not null
  - name  varchar not null
  - listed_on  timestampz not null

  - open  numeric not null
  - high  numeric not null
  - low  numeric not null
  - close  numeric not null
  - volume  numeric not null
  - dividends  numeric not null
  - stock_splits  numeric not null
  
  - share_price  numeric not null
  - float_shares  int
  - total_shares  int
  - market_cap  numeric
  
  - other_data  jsonb not null
  - created_on timestampz not null
  - updated_on timestampz not null
UNIQUE(exchange, symbol, trading_date)
UNIQUE(exchange, yahoo_symbol, trading_date)
  