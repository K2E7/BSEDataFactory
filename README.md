# BSE Data Factory

This repository is a collection of Python notebooks and utilities for collecting various types of data from the Bombay Stock Exchange (BSE).  
Each notebook focuses on a specific data type or API endpoint, providing reusable backend logic and simple interfaces for data extraction, storage, and further analysis.

---

## Notebook: `Announcement.ipynb`

**Purpose:**  
Fetches and stores raw JSON announcement data from the BSE API, organized by ISIN and scrip code.  
This notebook is designed to be importable and can serve as a backend for other data workflows.

### Features

- Downloads all paginated announcement data for specified scrips and date ranges.
- Stores results as newline-delimited, pretty-printed JSON files (one per ISIN) in an output directory.
- Logs progress for each ISIN and date chunk.
- Modular design for easy extension and integration with other BSE data notebooks.

### Usage

1. **Configure the mapping and date range:**
   - `map_str`: Comma-separated `"ISIN=scripCode"` pairs.
   - `start_str`: Start date (`YYYY-MM-DD`).
   - `end_str`: End date (`YYYY-MM-DD`, optional; defaults to today).

2. **Run the downloader:**
   ```python
   log = run(map_str, start_str, end_str)
   ```

3. **Output:**
   - JSON files are saved in the `output/` directory (configurable).
   - Each file contains all announcement payloads for the given ISIN.

---

**Note:**  
The creator `k2e7` is the online alias of Sriparno Ganguly.

---
© 2025 k2e7.   
Released under the MIT License. See [LICENSE](LICENSE) for details.
