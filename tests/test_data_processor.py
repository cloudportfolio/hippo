import os
import pytest
import pandas as pd
import json
from scripts.read_data import DataProcessor
from unittest.mock import patch

@pytest.fixture
def sample_data(tmp_path):
    # Create project structure
    dirs = [
        "pharmacies",
        "claims", 
        "reverts",
        "invalid_records",
        "logs",
        "results"
    ]
    
    for d in dirs:
        (tmp_path / d).mkdir()
    
    # Sample pharmacy data
    pharmacy_csv = tmp_path / "pharmacies" / "pharmacy1.csv"
    pd.DataFrame({
        "chain": ["CVS", "Walgreens"],
        "npi": ["123", "456"]
    }).to_csv(pharmacy_csv, index=False)
    
    # Sample claims data
    claims_json = tmp_path / "claims" / "claim1.json"
    with open(claims_json, "w") as f:
        json.dump([{
            "id": "1", "ndc": "drug1", "npi": "123",
            "quantity": 30, "price": 50, "timestamp": "2023-01-01"
        }, {
            "id": "2", "ndc": "drug2", "npi": "456",
            "quantity": 60, "price": 100, "timestamp": "2023-01-02"
        }], f)
    
    # Sample rollback data
    reverts_json = tmp_path / "reverts" / "revert1.json"
    with open(reverts_json, "w") as f:
        json.dump([{
            "id": "r1", 
            "claim_id": "1", 
            "timestamp": "2023-01-02"
        }], f)
    
    return tmp_path

@pytest.fixture
def processor(sample_data):
    return DataProcessor(str(sample_data))

def test_folder_structure(processor, sample_data):
    assert os.path.exists(processor.company_dir)
    assert os.path.exists(processor.claims_dir)
    assert os.path.exists(processor.rollback_dir)
    assert os.path.exists(processor.invalid_records_dir)

def test_csv_loading(processor):
    valid, invalid = processor.load_and_validate_csv(
        os.path.join(processor.company_dir, "pharmacy1.csv"),
        {"chain", "npi"}
    )
    assert len(valid) == 2
    assert invalid.empty

def test_json_loading(processor):
    valid, invalid = processor.load_and_validate_json(
        os.path.join(processor.claims_dir, "claim1.json"),
        {"id", "ndc", "npi"}
    )
    assert len(valid) == 2
    assert invalid.empty

def test_data_processing(processor):
    company, claims, reverts = processor.process_datasets()
    assert len(company) == 2
    assert len(claims) == 2  
    assert len(reverts) == 1

def test_analysis_outputs(processor):
    company, claims, reverts = processor.process_datasets()
    
    # Test main analysis
    analysis = processor.perform_analysis(company, claims, reverts)
    assert len(analysis) == 2  # One for each claim (grouped by npi/ndc)
    
    # Test top quantities
    top_quantities = processor.calculate_top_prescribed_quantities(claims)
    assert len(top_quantities) == 2  # One for each npi in claims
    
    # Test top chains
    top_chains = processor.calculate_top_chains(company, claims)
    assert len(top_chains) == 2  # One for each drug


