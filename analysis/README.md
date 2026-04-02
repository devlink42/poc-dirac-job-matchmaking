# Matcher Distribution Analysis

Generic scripts to extract and visualize TaskQueue distributions from a production DIRAC MySQL database. The output informs the synthetic data generator for the DiracX Matcher prototype.

VO-specific data lives in subdirectories (e.g. `lhcb/data/`).

## Usage

### 1. Extract data

```bash
./extract_distributions.sh -h <db_host> -u <user> -p <password> -D TaskQueueDB
```

This runs ~13 SQL queries via `mycli` and writes CSV files to the current directory.
Rename them with the extraction date, e.g. `site_distribution_2026-03-28.csv`, and place them under `<vo>/data/`.

If the Jobs table is in a different database, the `running_by_site` query may fail (it's non-fatal -- it just skips that export).

### 2. Explore interactively

Open `explore_distributions.ipynb` and point `DATA_DIR` to your VO data directory.

The notebook renders all charts inline -- no PNG files need to be committed.
