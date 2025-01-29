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