import os
import pandas as pd
import json
import logging
from collections import Counter
from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)


todays_date = datetime.now().strftime("%d%m%Y_%H%M%S")
base_path = os.path.join(os.getcwd())
path_logs = os.path.join(os.getcwd(), "logs") 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{path_logs}/data_processor_{todays_date}.log", mode="w")
    ]
)

class DataProcessor:
    def __init__(self, base_path):
        self.base_path = base_path
        self.company_dir = os.path.join(base_path, "pharmacies")
        self.claims_dir = os.path.join(base_path, "claims")
        self.rollback_dir = os.path.join(base_path, "reverts")
        self.invalid_records_dir=os.path.join(base_path, "invalid_records")
        
        logging.info(f"DataProcessor initialized with base path: {base_path}")

    def load_and_validate_csv(self, file_path, required_columns):
        """Loads and validates a single CSV file."""
        try:
            logging.info(f"Loading CSV file: {file_path}")
            data = pd.read_csv(file_path)
            if not required_columns.issubset(data.columns):
                logging.warning(f"File {file_path} does not comply with the expected schema.")
                return None, data

            valid_data = data.dropna(subset=required_columns)
            invalid_data = data[~data.index.isin(valid_data.index)]
            logging.info(f"CSV file {file_path} loaded successfully with {len(valid_data)} valid rows and {len(invalid_data)} invalid rows.")
            return valid_data, invalid_data
        except Exception as e:
            logging.error(f"Error loading CSV {file_path}: {e}")
            return None, None

    def load_and_validate_json(self, file_path, required_columns):
        """Loads and validates a single JSON file."""
        try:
            logging.info(f"Loading JSON file: {file_path}")
            with open(file_path, "r") as f:
                data = pd.DataFrame(json.load(f))
            if not required_columns.issubset(data.columns):
                logging.warning(f"File {file_path} does not comply with the expected schema.")
                return None, data

            valid_data = data.dropna(subset=required_columns)
            invalid_data = data[~data.index.isin(valid_data.index)]
            logging.info(f"JSON file {file_path} loaded successfully with {len(valid_data)} valid rows and {len(invalid_data)} invalid rows.")
            return valid_data, invalid_data
        except Exception as e:
            logging.error(f"Error loading JSON {file_path}: {e}")
            return None, None

    def process_folder(self, folder_path, required_columns, file_type, invalid_file_name):
        """Processes all files in a folder."""
        logging.info(f"Processing folder: {folder_path}")
        valid_dataframes = []
        invalid_dataframes = []

        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)

            if file_type == "csv" and file_name.endswith(".csv"):
                valid_data, invalid_data = self.load_and_validate_csv(file_path, required_columns)
            elif file_type == "json" and file_name.endswith(".json"):
                valid_data, invalid_data = self.load_and_validate_json(file_path, required_columns)
            else:
                logging.warning(f"Skipping unsupported file: {file_name}")
                continue

            if valid_data is not None:
                valid_dataframes.append(valid_data)
            if invalid_data is not None:
                invalid_dataframes.append(invalid_data)

        # Combine all valid and invalid dataframes
        valid_data = pd.concat(valid_dataframes, ignore_index=True) if valid_dataframes else pd.DataFrame()
        invalid_data = pd.concat(invalid_dataframes, ignore_index=True) if invalid_dataframes else pd.DataFrame()

        # Save invalid data
        self.save_file(invalid_data, invalid_file_name)
        
        logging.info(f"Finished processing folder {folder_path}. Valid data size: {valid_data.shape}.")
        return valid_data

    def save_file(self,data, file_path):
        """Saves data to a file, handling both CSV and JSON formats."""
        if data is not None:
            try:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                if file_path.endswith(".csv"):
                    data.to_csv(file_path, index=False)
                elif file_path.endswith(".json"):
                    with open(file_path, "w") as f:
                        json.dump(data, f, indent=4)
                logging.info(f"Data saved to {file_path}")
            except Exception as e:
                logging.error(f"Error saving data to {file_path}: {e}")

    def save_invalid_data(self, invalid_data, file_name):
        """Saves invalid data to a specified file."""
        if invalid_data is not None and not invalid_data.empty:
            invalid_path = os.path.join(self.base_path, f"data/invalid_records/{file_name}")
            try:
                if file_name.endswith(".csv"):
                    invalid_data.to_csv(invalid_path, index=False)
                elif file_name.endswith(".json"):
                    invalid_data.to_json(invalid_path, orient="records", lines=True)
                logging.info(f"Invalid data saved to {invalid_path}")
            except Exception as e:
                logging.error(f"Error saving invalid data to {invalid_path}: {e}")

    def process_datasets(self):
        """Processes all datasets from their respective folders."""
        logging.info("Starting dataset processing.")

        # Define required columns
        pharmacies_required_columns = {"chain", "npi"}
        claims_required_columns = {"id", "ndc", "npi", "quantity", "price", "timestamp"}
        reverts_required_columns = {"id", "claim_id", "timestamp"}

        # Process the company (pharmacy) data
        company_data = self.process_folder(
            self.company_dir, pharmacies_required_columns, "csv", "invalid_company.csv"
        )

        # Process the claims data
        claims_data = self.process_folder(
            self.claims_dir, claims_required_columns, "json", "invalid_claims.json"
        )

        # Filter claims data to include only records with valid 'npi' from company_data
        valid_npi_set = set(company_data["npi"].astype(str)) 
        valid_claims = claims_data[claims_data["npi"].astype(str).isin(valid_npi_set)]
        invalid_claims = claims_data[~claims_data["npi"].isin(valid_npi_set)]

        # Save invalid claims into the invalid_records folder
        invalid_claims_path = os.path.join(self.invalid_records_dir, "invalid_claims.json")
        invalid_claims.to_json(invalid_claims_path, orient="records", lines=True)

        # Process the rollback data
        reverts_data = self.process_folder(
            self.rollback_dir, reverts_required_columns, "json", "data/invalid_records/invalid_rollback.json"
        )

        logging.info("Dataset processing completed.")
        return company_data, valid_claims, reverts_data

    
    def perform_analysis(self, pharmacy_data, claims_data, rollbacks_data, save_path=f"results/analysis_result_{todays_date}.json"):
        """
        Performs data analysis on claims and rollbacks, grouped by `npi` and `ndc`, and saves results to a JSON file.
        Excludes `npi` that don't exist in the pharmacy file and rollbacks that don't exist in claims.
        """
        if claims_data is not None and rollbacks_data is not None and pharmacy_data is not None:
            combined_data = claims_data.merge(
            rollbacks_data,
            left_on="id",  # Assuming 'id' is the key in claims
            right_on="claim_id",  # Assuming 'claim_id' is the key in rollbacks
            how="left",
            suffixes=("", "_rollback")
        )
    
            # Add a flag for rollbacks (1 if rollback exists, 0 otherwise)
            combined_data["rollback_flag"] = combined_data["claim_id"].notna().astype(int)

            # Group data by `npi` and `ndc` and calculate metrics
            aggregated_claims = combined_data.groupby(["npi", "ndc"]).agg(
                fills=("quantity", "sum"),
                reverted=("rollback_flag", "sum"),
                avg_price=("price", "mean"),
                total_price=("price", lambda x: (x * combined_data.loc[x.index, "quantity"]).sum())
            ).reset_index()

            # Format results as a list of dictionaries
            analysis_result = aggregated_claims.to_dict(orient="records")

            # Save results to JSON
            self.save_file(analysis_result, save_path)

            return analysis_result
        else:
            logging.info("Claims data, rollbacks data, or pharmacy data is missing.")
            return []

    def calculate_top_prescribed_quantities(self, claims_data, save_path=f"results/top_prescribed_quantities_{todays_date}.json"):
    
        required_columns = ["npi", "quantity"]
        for col in required_columns:
            if col not in claims_data.columns:
                raise KeyError(f"Column '{col}' not found in claims_data. Available columns: {claims_data.columns.tolist()}")

        # Ensure data types are consistent
        claims_data["npi"] = claims_data["npi"].astype(str)
        claims_data["quantity"] = claims_data["quantity"].astype(float)

        grouped_data = claims_data.groupby("npi")["quantity"].apply(list).reset_index()

        # Calculate the top 4 most prescribed quantities for each product
        result = []
        for _, row in grouped_data.iterrows():
            product_id = row["npi"]
            quantities = row["quantity"]

            # Count the occurrences of each quantity
            quantity_counts = Counter(quantities)

            # Get the top 4 most common quantities
            top_4_quantities = [qty for qty, _ in quantity_counts.most_common(4)]

            # Append the result for this product
            result.append({
                "id": product_id,
                "most_prescribed_quantity": top_4_quantities
            })

        # Save the result to a JSON file
        self.save_file(result, save_path)

        return result

    def calculate_top_chains(self, pharmacy_data, claims_data, save_path=f"results/top_chains_{todays_date}.json"):
       
        # Ensure data types are consistent
        pharmacy_data["npi"] = pharmacy_data["npi"].astype(str)
        claims_data["npi"] = claims_data["npi"].astype(str)
        claims_data["ndc"] = claims_data["ndc"].astype(str)
        
        # Merge claims data with pharmacy data to include chain information
        merged_data = claims_data.merge(
            pharmacy_data[["npi", "chain"]],
            on="npi",
            how="inner"
        )

        # Group by `ndc` and `chain` to calculate average price
        grouped_data = merged_data.groupby(["ndc", "chain"]).agg(
            avg_price=("price", "mean")
        ).reset_index()

        # Get the top 2 chains for each drug (ndc) based on average price
        top_chains = (
            grouped_data.sort_values(by=["ndc", "avg_price"], ascending=[True, False])
            .groupby("ndc")
            .head(2)
        )

        # Format the result as a list of dictionaries
        result = (
            top_chains.groupby("ndc")
            .apply(
                lambda group: {
                    "ndc": group.name,
                    "chain": group[["chain", "avg_price"]]
                    .rename(columns={"chain": "name"})
                    .to_dict(orient="records"),
                }
            )
            .tolist()
        )

        # Save the result to a JSON file
        self.save_file(result, save_path)
        
        return result
    
    def run(self):
        """Execute the pipeline."""
        logging.info("Starting...")
        
        # Process datasets
        company_data, claims_data, revert_data = self.process_datasets()
        
        # Analysis
        self.perform_analysis(company_data, claims_data, revert_data)
        
        # top chains
        self.calculate_top_chains(company_data, claims_data)
        
        # top quantities prescribed
        self.calculate_top_prescribed_quantities(claims_data)
        
        logging.info("Concluded!!!")

# Main
if __name__ == "__main__":
    base_path = os.path.join(os.getcwd(), "data") 
    processor = DataProcessor(base_path)
    
    # Rodar o pipeline completo
    processor.run()