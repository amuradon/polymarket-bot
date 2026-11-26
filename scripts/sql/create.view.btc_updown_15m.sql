USE "data";

CREATE VIEW btc_updown_15m AS 
SELECT * FROM '/data/polymarket/collector/polymarket/btc-updown-15m/*.parquet';
