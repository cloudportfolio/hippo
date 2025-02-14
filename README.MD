# Data Processor Application

A Python application for processing, validating, and analyzing healthcare-related data (pharmacies, claims, and rollbacks). It handles CSV/JSON files, validates records, and generates analytical insights.

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
- [Functionality](#functionality)
  - [Data Processing](#data-processing)
  - [Analysis](#analysis)
- [File Structure](#file-structure)
- [Logging](#logging)
- [Requirements](#requirements)
- [Future Improvements](#future-improvements)

---

## Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   ```

2. **Navigate to the project directory**:
   ```bash
   cd data-processor
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

---

## Usage

### Running the Application
Execute the main script to process data and generate insights:
```bash
python data_processor.py
```

This performs:
- Loading/validation of datasets (CSV/JSON).
- Processing data and saving results to output files.
- Executing analyses to generate insights.

### Example Script Workflow
1. **Initialize the Data Processor**:
   ```python
   base_path = os.path.join(os.getcwd(), "data")
   processor = DataProcessor(base_path)
   ```

2. **Process Datasets**:
   - Load and validate pharmacy, claims, and rollback data.
   - Invalid records are saved to `invalid_records/`.

3. **Perform Analyses**:
   - **Top Prescribed Quantities**: Most common drug quantities per product.
   - **Top Chains**: Pharmacy chains prescribing the highest-priced drugs.
   - **General Analysis**: Merges claims/rollbacks to calculate metrics like total fills and average price.

---

## Functionality

### Data Processing
- **Loading & Validation**:
  - Loads CSV/JSON files and checks for required columns (e.g., `pharmacy_id`, `claim_id`).
  - Invalid rows are flagged and saved to `invalid_records/`.

- **Folder Processing**:
  - Processes all files in a directory (e.g., `pharmacies/`, `claims/`).

- **Data Saving**:
  - Saves valid/invalid data to appropriate formats (CSV/JSON).

### Analysis
- **General Analysis**:
  - Merges claims and rollbacks data.
  - Calculates metrics: total fills, reverted claims, average price, and total price.

- **Top Prescribed Quantities**:
  - Identifies top 4 prescribed quantities for each drug product.

- **Top Chains**:
  - Ranks top 2 pharmacy chains per drug based on highest prices.

---

## File Structure
```
data/
├── pharmacies/       # Pharmacy data (CSV)
├── claims/           # Claims data (JSON)
├── reverts/          # Rollback data (JSON)
├── invalid_records/  # Invalid data (CSV/JSON)
logs/                 # Application logs
results/              # Analysis outputs (CSV/JSON)
```

---

## Logging
- Logs are stored in `logs/` with timestamps (e.g., `2023-10-01_12-30-45.log`).
- Tracks data loading, validation errors, analysis steps, and warnings.

---

## Requirements
- Python 3.x
- Pandas (see `requirements.txt` for full dependencies)

---

## Future Improvements
- Add advanced data validation rules (e.g., data type checks).
- Implement parallel processing for large datasets.
- Expand reporting capabilities (e.g., PDF/Excel reports).
- Enhance error handling for edge cases.

## Star Schema

![Star Schema](image/star_schema_claim_events.png)

## Code

https://dbdiagram.io/d

```sql
Table dim_pharmacy {
  id integer [primary key]  // Surrogate Key
  npi string [unique]       // Pharmacy NPI (Natural Key)
  chain string             // Pharmacy Chain Name
  valid_from timestamp 
  valid_to timestamp
}


Table dim_drug {
  id integer [primary key]  // Surrogate Key
  ndc string [unique]       // National Drug Code (Natural Key)
  drug_name varchar 
  valid_from timestamp
  valid_to timestamp
}


Table dim_time {
  id integer [primary key]  // Surrogate Key
  date date
  day_of_week integer
  month integer
  year integer
  hour varchar
  quarter integer
}

TABLE (Merged Claims & Reverts)
Table fact_claim_events {
  claim_id integer [primary key]
  pharmacy_id varchar
  drug_id varchar
  price float
  quantity integer
  is_reverted bool
  event_time integer // FK to dim_time
  event_type varchar // 'CLAIMED' or 'REVERTED'
}


// RELATIONSHIPS
Ref: fact_claim_events.pharmacy_id > dim_pharmacy.id
Ref: fact_claim_events.drug_id > dim_drug.id
Ref: fact_claim_events.event_time > dim_time.id
```
### Queries

1. Get the total quantity of claimed drugs for a specific pharmacy within a given time period:
```sql
SELECT
    p.chain AS pharmacy_chain,
    d.drug_name,
    SUM(f.quantity) AS total_claimed_quantity
FROM
    fact_claim_events f
JOIN
    dim_pharmacy p ON f.pharmacy_id = p.id
JOIN
    dim_drug d ON f.drug_id = d.id
JOIN
    dim_time t ON f.event_time = t.id
WHERE
    f.event_type = 'CLAIMED'
    AND p.npi = 'PHARMACY_NPI'  -- Replace with the actual NPI
    AND t.date BETWEEN '2023-01-01' AND '2023-12-31'  -- Adjust date range
GROUP BY
    p.chain, d.drug_name
ORDER BY
    total_claimed_quantity DESC;
```

2. Get a list of pharmacies and the drugs they have claimed, including whether the claim was reverted
```sql
SELECT
    p.chain AS pharmacy_chain,
    d.drug_name,
    f.is_reverted,
    COUNT(f.claim_id) AS total_claims
FROM
    fact_claim_events f
JOIN
    dim_pharmacy p ON f.pharmacy_id = p.id
JOIN
    dim_drug d ON f.drug_id = d.id
WHERE
    f.event_type = 'CLAIMED'  -- or 'REVERTED' if required
GROUP BY
    p.chain, d.drug_name, f.is_reverted
ORDER BY
    total_claims DESC;
```

3. Get the total amount of money claimed for each drug within a specific time period:
```sql
SELECT
    d.drug_name,
    SUM(f.price * f.quantity) AS total_claimed_amount
FROM
    fact_claim_events f
JOIN
    dim_drug d ON f.drug_id = d.id
JOIN
    dim_time t ON f.event_time = t.id
WHERE
    f.event_type = 'CLAIMED'
    AND t.date BETWEEN '2023-01-01' AND '2023-12-31'  -- Adjust date range
GROUP BY
    d.drug_name
ORDER BY
    total_claimed_amount DESC;
```

4. Get the count of reverted claims by pharmacy and drug:
```sql
SELECT
    p.chain AS pharmacy_chain,
    d.drug_name,
    COUNT(f.claim_id) AS total_reverted_claims
FROM
    fact_claim_events f
JOIN
    dim_pharmacy p ON f.pharmacy_id = p.id
JOIN
    dim_drug d ON f.drug_id = d.id
WHERE
    f.event_type = 'REVERTED'
GROUP BY
    p.chain, d.drug_name
ORDER BY
    total_reverted_claims DESC;
```

5. Identify the most claimed drugs in a specific year, including their total claimed quantity and amount:
```sql
SELECT
    d.drug_name,
    SUM(f.quantity) AS total_claimed_quantity,
    SUM(f.price * f.quantity) AS total_claimed_amount
FROM
    fact_claim_events f
JOIN
    dim_drug d ON f.drug_id = d.id
JOIN
    dim_time t ON f.event_time = t.id
WHERE
    f.event_type = 'CLAIMED'
    AND t.year = 2023  -- Adjust the year as needed
GROUP BY
    d.drug_name
ORDER BY
    total_claimed_quantity DESC;
```
6. Get the total quantity claimed by each pharmacy, for each drug, along with the drug's price:
```sql
SELECT
    p.chain AS pharmacy_chain,
    d.drug_name,
    SUM(f.quantity) AS total_claimed_quantity,
    AVG(f.price) AS avg_claimed_price
FROM
    fact_claim_events f
JOIN
    dim_pharmacy p ON f.pharmacy_id = p.id
JOIN
    dim_drug d ON f.drug_id = d.id
WHERE
    f.event_type = 'CLAIMED'
GROUP BY
    p.chain, d.drug_name
ORDER BY
    total_claimed_quantity DESC;
```

7. Get the total claims for a specific drug within a specific time period (e.g., within a specific quarter of the year):
```sql
SELECT
    d.drug_name,
    SUM(f.quantity) AS total_claimed_quantity,
    SUM(f.price * f.quantity) AS total_claimed_amount
FROM
    fact_claim_events f
JOIN
    dim_drug d ON f.drug_id = d.id
JOIN
    dim_time t ON f.event_time = t.id
WHERE
    f.event_type = 'CLAIMED'
    AND t.quarter = 2  -- Replace with the desired quarter (e.g., 1, 2, 3, 4)
    AND t.year = 2023  -- Adjust the year as needed
GROUP BY
    d.drug_name
ORDER BY
    total_claimed_quantity DESC;
```