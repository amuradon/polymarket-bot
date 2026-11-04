# Polymarket Bot Scripts

Adresář obsahuje skripty pro stahování dat z AWS EC2 Data Collectoru, jejich transformaci a měření síťové latence vůči kryptoměnovým burzám. Skripty jsou dostupné jak pro Windows (PowerShell), tak pro Linux (Bash).

---

## 1. Stahování dat z Data Collectoru (`download-collector-data`)

Stahuje denní Zstandard (`.jsonl.zst`) archivy z EC2 instance na lokální disk se zachováním adresářové struktury, verifikuje integritu a maže stažené soubory z EC2.

### Linux (Bash)
```bash
# Stáhnout data pro konkrétní den
./scripts/download-collector-data.sh -d 2026-10-30 -r /mnt/ebs/polymarket-data/collector/polymarket -o /data/polymarket/collector/polymarket

# Náhled (dry-run) bez stahování a mazání
./scripts/download-collector-data.sh -d 2026-10-30 -n

# Ponechat data na EC2 po stažení
./scripts/download-collector-data.sh -d 2026-10-30 -k

# Rekonciliace a promazání již dříve stažených souborů na EC2
./scripts/download-collector-data.sh -c -o /data/polymarket/collector
```

### Windows (PowerShell)
```powershell
# Stáhnout pouze Polymarket data
.\scripts\download-collector-data.ps1 -Date 2026-10-30 -RemoteBaseDir /mnt/ebs/polymarket-data/collector/polymarket -DestinationDir D:\Crypto\data\Polymarket\collector\polymarket

# Náhled (dry-run)
.\scripts\download-collector-data.ps1 -Date 2026-10-30 -DryRun

# Rekonciliace a promazání již dříve stažených souborů na EC2
.\scripts\download-collector-data.ps1 -CleanAlreadyDownloaded -DestinationDir D:\Crypto\data\Polymarket\collector
```

---

## 2. Konverze JSONL.ZST na Apache Parquet (`convert-to-parquet`)

Konvertuje `.jsonl.zst` archivy do typovaných Apache Parquet (`.parquet`) souborů pomocí DuckDB, řadí záznamy podle `ts ASC` a po ověření smaže původní archivy.

### Linux (Bash)
```bash
# Konverze všech archivů v cílovém adresáři
./scripts/convert-to-parquet.sh -t /data/polymarket/collector

# Náhled bez úprav
./scripts/convert-to-parquet.sh -t /data/polymarket/collector -n

# Filtrování podle data a zachování původních archivů
./scripts/convert-to-parquet.sh -d 2026-10-30 -k

# Filtrování podle typu streamu
./scripts/convert-to-parquet.sh -s aggTrades
```

### Windows (PowerShell)
```powershell
# Konverze všech archivů
.\scripts\convert-to-parquet.ps1 -TargetDir D:\Crypto\data\Polymarket\collector

# Náhled bez úprav
.\scripts\convert-to-parquet.ps1 -DryRun

# Filtrování podle data se zachováním originálů
.\scripts\convert-to-parquet.ps1 -Date 2026-10-30 -KeepOriginal
```

---

## 3. Měření WebSocket latence burz (`measure-exchange-ws-latency`)

Měří jednosměrnou WebSocket síťovou latenci (`t_recv - t_exchange_event`) ze 7 kryptoměnových burz (Binance, Coinbase, Kraken, Bitstamp, Gemini, LMAX Digital, Crypto.com) na instanci AWS EC2 v regionu `eu-west-1` (Dublin). Skript před spuštěním ověří synchronizaci systémových hodin přes `chrony` (AWS Time Sync Service).

### Linux (Bash)
```bash
# Měření všech 7 burz pro BTC (výchozí 100 vzorků)
./scripts/measure-exchange-ws-latency.sh

# Měření pro ETH pouze na vybraných burzách
./scripts/measure-exchange-ws-latency.sh -s ETH -e cryptocom,lmax

# Větší počet vzorků s delším časovým limitem
./scripts/measure-exchange-ws-latency.sh -s BTC -c 200 -d 90
```

### Windows (PowerShell)
```powershell
# Měření všech 7 burz pro BTC
.\scripts\measure-exchange-ws-latency.ps1

# Měření vybraných burz pro ETH
.\scripts\measure-exchange-ws-latency.ps1 -Symbol ETH -Exchanges cryptocom,lmax
```
