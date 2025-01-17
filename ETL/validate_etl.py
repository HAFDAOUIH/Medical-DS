#!/usr/bin/env python3

'''import pandas as pd
import json
import os
from pathlib import Path
import random
from typing import Dict, List, Tuple
import logging
from tqdm import tqdm

class ETLValidator:
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.setup_logging()
        
        # Initialize counters for original data
        self.original_counts = {
            'allergyintolerance': 0,
            'careplan': 0,
            'careteam': 0,
            'claim': 0,
            'condition': 0,
            'device': 0,
            'diagnosticreport': 0,
            'encounter': 0,
            'explanationofbenefit': 0,
            'goal': 0,
            'imagingstudy': 0,
            'immunization': 0,
            'medicationadministration': 0,
            'medicationrequest': 0,
            'observation': 0,
            'organization': 0,
            'patient': 0,
            'practitioner': 0,
            'procedure': 0
        }

    def setup_logging(self):
        """Set up logging configuration."""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Create handlers
        file_handler = logging.FileHandler('validation_report.log')
        console_handler = logging.StreamHandler()
        
        # Create formatters and add it to handlers
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(log_format)
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def count_original_resources(self) -> None:
        """Count resources in original JSON files."""
        self.logger.info("Counting resources in original JSON files...")
        
        for file_name in tqdm(os.listdir(self.input_dir)):
            if not file_name.endswith('.json'):
                continue
                
            try:
                with open(self.input_dir / file_name) as f:
                    bundle = json.load(f)
                    
                if not isinstance(bundle, dict) or 'entry' not in bundle:
                    continue
                    
                for entry in bundle['entry']:
                    if 'resource' in entry and 'resourceType' in entry['resource']:
                        resource_type = entry['resource']['resourceType'].lower()
                        if resource_type in self.original_counts:
                            self.original_counts[resource_type] += 1
                            
            except Exception as e:
                self.logger.error(f"Error processing {file_name}: {str(e)}")

    def validate_csv_files(self) -> Dict[str, Dict]:
        """Validate processed CSV files and return statistics."""
        results = {}
        
        for resource_type in self.original_counts.keys():
            csv_path = self.output_dir / f"{resource_type}.csv"
            if not csv_path.exists():
                if self.original_counts[resource_type] > 0:
                    self.logger.warning(f"Missing CSV file for {resource_type} which has {self.original_counts[resource_type]} original records")
                continue
                
            try:
                df = pd.read_csv(csv_path)
                
                # Calculate statistics
                stats = {
                    'row_count': len(df),
                    'original_count': self.original_counts[resource_type],
                    'empty_fields': df.isna().sum().to_dict(),
                    'completeness_ratio': (1 - df.isna().mean()).mean() * 100
                }
                
                # Compare counts
                count_diff = stats['row_count'] - stats['original_count']
                if count_diff != 0:
                    self.logger.warning(
                        f"{resource_type}: Count mismatch! CSV: {stats['row_count']}, "
                        f"Original: {stats['original_count']}, Difference: {count_diff}"
                    )
                
                results[resource_type] = stats
                
            except Exception as e:
                self.logger.error(f"Error validating {resource_type}.csv: {str(e)}")
                continue
        
        return results

    def sample_check_encounter(self) -> None:
        """Detailed validation of sample encounter records."""
        csv_path = self.output_dir / "encounter.csv"
        if not csv_path.exists():
            self.logger.error("Encounter.csv not found")
            return
            
        try:
            df = pd.read_csv(csv_path)
            sample_size = min(5, len(df))
            sample_records = df.sample(n=sample_size)
            
            self.logger.info(f"\nValidating {sample_size} sample encounter records:")
            
            for _, record in sample_records.iterrows():
                # Extract encounter ID from the compound ID field
                encounter_id = record['id'].split(',')[0] if ',' in record['id'] else record['id']
                
                self.logger.info(f"\nChecking encounter ID: {encounter_id}")
                self.logger.info(f"Start Date: {record['start_date']}")
                self.logger.info(f"End Date: {record['end_date']}")
                self.logger.info(f"Status: {record['status']}")
                
                # Validate date format
                if pd.notna(record['start_date']):
                    try:
                        pd.to_datetime(record['start_date'])
                    except:
                        self.logger.warning(f"Invalid start_date format: {record['start_date']}")
                
                if pd.notna(record['end_date']):
                    try:
                        pd.to_datetime(record['end_date'])
                    except:
                        self.logger.warning(f"Invalid end_date format: {record['end_date']}")
                
                # Check status values
                if pd.notna(record['status']) and record['status'] not in ['finished', 'cancelled', 'in-progress']:
                    self.logger.warning(f"Unexpected status value: {record['status']}")
                    
        except Exception as e:
            self.logger.error(f"Error in sample validation: {str(e)}")

    def generate_report(self, validation_results: Dict[str, Dict]) -> None:
        """Generate a comprehensive validation report."""
        self.logger.info("\n=== ETL Validation Report ===\n")
        
        # Overall statistics
        total_processed = sum(stats['row_count'] for stats in validation_results.values())
        total_original = sum(stats['original_count'] for stats in validation_results.values())
        
        self.logger.info(f"Total processed records: {total_processed:,}")
        self.logger.info(f"Total original records: {total_original:,}")
        self.logger.info(f"Overall difference: {total_processed - total_original:,}\n")
        
        # Per-resource statistics
        self.logger.info("Resource-level Statistics:")
        for resource_type, stats in validation_results.items():
            self.logger.info(f"\n{resource_type.upper()}:")
            self.logger.info(f"  Processed records: {stats['row_count']:,}")
            self.logger.info(f"  Original records: {stats['original_count']:,}")
            self.logger.info(f"  Data completeness: {stats['completeness_ratio']:.2f}%")
            
            # Report fields with high number of empty values
            empty_fields = {k: v for k, v in stats['empty_fields'].items() if v > 0}
            if empty_fields:
                self.logger.info("  Fields with missing values:")
                for field, count in empty_fields.items():
                    percentage = (count / stats['row_count']) * 100
                    self.logger.info(f"    - {field}: {count:,} ({percentage:.1f}%)")

def main():
    input_dir = "fhir"
    output_dir = "processed_data"
    
    validator = ETLValidator(input_dir, output_dir)
    
    # Count original resources
    validator.count_original_resources()
    
    # Validate processed files
    validation_results = validator.validate_csv_files()
    
    # Generate validation report
    validator.generate_report(validation_results)
    
    # Perform detailed encounter validation
    validator.sample_check_encounter()

if __name__ == "__main__":
    main()
'''
"""
validate_etl.py

A validation script to compare the number of original FHIR resources
with the number of CSV rows produced by the ETL, and to check basic data
completeness. Also does sample checks on Encounter data.

Usage:
  python3 validate_etl.py --input_dir fhir --output_dir processed_data
"""
import argparse
import json
import logging
import os
from pathlib import Path
from typing import Dict, List
import pandas as pd
import random

from tqdm import tqdm

class ETLValidator:
    """
    Validates that the ETL output CSVs match the FHIR JSON input in resource counts
    and performs a few basic data integrity checks.
    """

    RESOURCE_TYPES = [
        'allergyintolerance', 'careplan', 'careteam', 'claim', 'condition', 'device',
        'diagnosticreport', 'encounter', 'explanationofbenefit', 'goal',
        'imagingstudy', 'immunization', 'medicationadministration', 'medicationrequest',
        'observation', 'organization', 'patient', 'practitioner', 'procedure'
    ]

    def __init__(self, input_dir: Path, output_dir: Path):
        self.input_dir = input_dir
        self.output_dir = output_dir

        # Original resource counts from JSON
        self.original_counts: Dict[str, int] = {r: 0 for r in self.RESOURCE_TYPES}

        # Set up logging
        self.logger = self._setup_logging()

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # Remove existing handlers (if any)
        logger.handlers = []

        file_handler = logging.FileHandler("validation_report.log", mode='w')
        console_handler = logging.StreamHandler()

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def count_original_resources(self) -> None:
        """Iterate over all .json files in input_dir and count resources by resourceType."""
        self.logger.info("Counting resources in original JSON files...")
        json_files = [f for f in self.input_dir.iterdir() if f.suffix == '.json']

        for file_path in tqdm(json_files):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    bundle = json.load(f)

                if not isinstance(bundle, dict):
                    continue
                entries = bundle.get('entry', [])
                if not isinstance(entries, list):
                    continue

                for entry in entries:
                    resource = entry.get('resource', {})
                    rtype = resource.get('resourceType', '').lower()
                    if rtype in self.original_counts:
                        self.original_counts[rtype] += 1
            except Exception as e:
                self.logger.error(f"Error counting in file {file_path}: {e}")

    def validate_csv_files(self) -> Dict[str, Dict[str, float]]:
        """
        Validate the CSV files produced by the ETL. Returns a dict of
        { resource_type: { 'row_count': int, 'completeness_ratio': float, ... } }.
        """
        results = {}
        for rtype in self.RESOURCE_TYPES:
            csv_file = self.output_dir / f"{rtype}.csv"
            original_count = self.original_counts.get(rtype, 0)

            if not csv_file.exists():
                if original_count > 0:
                    self.logger.warning(
                        f"CSV for {rtype} is missing, but original had {original_count} records!"
                    )
                continue

            try:
                df = pd.read_csv(csv_file)
                row_count = len(df)
                diff = row_count - original_count

                # Calculate data completeness as average non-nullness
                completeness_ratio = (1 - df.isna().mean()).mean() * 100

                results[rtype] = {
                    'row_count': row_count,
                    'original_count': original_count,
                    'difference': diff,
                    'completeness_ratio': completeness_ratio,
                    'na_counts': df.isna().sum().to_dict(),
                }

            except Exception as e:
                self.logger.error(f"Error reading {csv_file}: {e}")
                continue

        return results

    def sample_check_encounter(self, n_samples: int = 5) -> None:
        """
        Read encounter.csv, pick a random sample of records, and log
        some basic fields for manual inspection.
        """
        csv_file = self.output_dir / "encounter.csv"
        if not csv_file.exists():
            self.logger.warning("No encounter.csv found for sample check.")
            return

        try:
            df = pd.read_csv(csv_file)
            if df.empty:
                self.logger.warning("encounter.csv is empty, skipping sample checks.")
                return

            sample_size = min(n_samples, len(df))
            sample_records = df.sample(n=sample_size, random_state=42)

            self.logger.info(f"\nValidating {sample_size} sample encounter records:\n")
            for _, row in sample_records.iterrows():
                enc_id = row.get('id')
                self.logger.info(f"Encounter ID: {enc_id}")
                self.logger.info(f"  Start Date: {row.get('start_date')}")
                self.logger.info(f"  End Date:   {row.get('end_date')}")
                self.logger.info(f"  Status:     {row.get('status')}")
                self.logger.info("")

                # Example: Check if start_date is parseable
                # (just a simple check—pandas will raise if invalid)
                start_date = row.get('start_date')
                if pd.notna(start_date):
                    try:
                        pd.to_datetime(start_date)
                    except ValueError:
                        self.logger.warning(f"Invalid start_date: {start_date}")

        except Exception as e:
            self.logger.error(f"Error during encounter sample check: {e}")

    def generate_report(self, results: Dict[str, Dict[str, float]]) -> None:
        """
        Print a summary of resource counts and differences
        to both console and the validation_report.log.
        """
        self.logger.info("\n=== ETL Validation Report ===\n")

        total_processed = sum(r['row_count'] for r in results.values() if 'row_count' in r)
        total_original = sum(r['original_count'] for r in results.values() if 'original_count' in r)
        overall_diff = total_processed - total_original

        self.logger.info(f"Total processed records: {total_processed}")
        self.logger.info(f"Total original records: {total_original}")
        self.logger.info(f"Overall difference: {overall_diff}\n")

        self.logger.info("Resource-level Statistics:")
        for rtype, stats in results.items():
            row_count = stats['row_count']
            orig_count = stats['original_count']
            comp_ratio = stats['completeness_ratio']
            diff = stats['difference']
            self.logger.info(f"\n{rtype.upper()}:")
            self.logger.info(f"  Processed records: {row_count}")
            self.logger.info(f"  Original records:  {orig_count}")
            self.logger.info(f"  Difference:        {diff}")
            self.logger.info(f"  Data completeness: {comp_ratio:.2f}%")

            # If you want to highlight fields with missing values:
            na_counts = stats['na_counts']
            missing_fields = {k: v for k, v in na_counts.items() if v > 0}
            if missing_fields:
                self.logger.info("  Fields with missing values:")
                for field, count in missing_fields.items():
                    perc = (count / row_count) * 100 if row_count else 0
                    self.logger.info(f"    - {field}: {count} ({perc:.1f}%)")

    def run_validation(self) -> None:
        """High-level method to run the entire validation workflow."""
        # 1. Count original resources
        self.count_original_resources()

        # 2. Validate CSV files
        results = self.validate_csv_files()

        # 3. Generate final report
        self.generate_report(results)

        # 4. Optional: additional checks on specific resource types
        self.sample_check_encounter()


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate FHIR ETL output.")
    parser.add_argument("--input_dir", required=True, help="Path to original FHIR JSON files.")
    parser.add_argument("--output_dir", required=True, help="Path to ETL CSV output.")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    if not input_dir.exists() or not input_dir.is_dir():
        print(f"Error: input_dir {input_dir} does not exist or is not a directory.")
        return 1

    if not output_dir.exists() or not output_dir.is_dir():
        print(f"Error: output_dir {output_dir} does not exist or is not a directory.")
        return 1

    try:
        validator = ETLValidator(input_dir, output_dir)
        validator.run_validation()
        return 0
    except Exception as e:
        print(f"Validation failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
