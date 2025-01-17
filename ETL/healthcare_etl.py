#!/usr/bin/env python3

import argparse
import json
import logging
import multiprocessing as mp
import os
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from tqdm import tqdm

import pandas as pd
from sqlalchemy import create_engine, Engine


# ------------------------------------------------------------------------------
# 1. Resource extractors
# ------------------------------------------------------------------------------

def _clean_id(resource_id: Optional[str]) -> Optional[str]:
    """Clean and standardize a FHIR resource ID."""
    if not resource_id:
        return None
    # Remove any 'urn:uuid:' prefix
    if 'urn:uuid:' in resource_id:
        resource_id = resource_id.split('urn:uuid:')[-1]
    return resource_id.strip()

def _extract_reference_id(reference: Optional[str]) -> Optional[str]:
    """Extract the resource ID portion from a reference string like 'Patient/123'."""
    if not reference:
        return None
    parts = reference.split('/')
    return parts[-1] if len(parts) > 1 else reference

def extract_patient(resource: dict) -> Optional[dict]:
    """
    Extract fields specific to a Patient resource.
    https://www.hl7.org/fhir/patient.html
    """
    if 'id' not in resource:
        return None
    patient_id = _clean_id(resource['id'])
    name = resource.get('name', [{}])[0]
    extracted = {
        'id': patient_id,
        'family_name': name.get('family'),
        'given_name': next(iter(name.get('given', [])), None),
        'birth_date': resource.get('birthDate'),
        'gender': resource.get('gender'),
        'deceased_datetime': resource.get('deceasedDateTime')
    }
    return {k: v for k, v in extracted.items() if v is not None}

import logging
from typing import Optional
import dateutil.parser

logger = logging.getLogger(__name__)

def _clean_id(resource_id: Optional[str]) -> Optional[str]:
    """Clean and standardize a FHIR resource ID."""
    if not resource_id:
        return None
    if 'urn:uuid:' in resource_id:
        resource_id = resource_id.split('urn:uuid:')[-1]
    return resource_id.strip()

def _extract_reference_id(reference: Optional[str]) -> Optional[str]:
    """Extract the resource ID portion from a reference string like 'Patient/123'."""
    if not reference:
        return None
    parts = reference.split('/')
    return parts[-1] if len(parts) > 1 else reference

def _try_parse_date(date_str: Optional[str]) -> Optional[str]:
    """
    Attempt to parse date/time string to a standardized ISO-8601 string.
    If parsing fails (invalid format), returns None and logs a warning.
    """
    if not date_str:
        return None
    
    try:
        dt = dateutil.parser.isoparse(date_str)
        # Return as ISO 8601 string, e.g. "2025-01-15T12:34:56-05:00"
        return dt.isoformat()
    except (ValueError, TypeError) as e:
        logger.warning(f"Encounter date parsing failed for '{date_str}': {e}")
        return None

def extract_encounter(resource: dict) -> Optional[dict]:
    """
    Extract and validate fields specific to an Encounter resource.
    https://www.hl7.org/fhir/encounter.html

    - If required fields (like ID) are missing, returns None.
    - Missing optional fields get a default value or None.
    - Attempts to parse start/end dates. Logs a warning if invalid.
    - Checks that start_date <= end_date if both exist. Logs a warning if violated.
    """
    if 'id' not in resource:
        logger.warning("Encounter resource missing 'id'; skipping.")
        return None
    
    encounter_id = _clean_id(resource['id'])

    # Period object may have .start and/or .end
    period = resource.get('period', {})
    raw_start = period.get('start')
    raw_end = period.get('end')

    # Safely parse the dates
    start_date = _try_parse_date(raw_start)
    end_date = _try_parse_date(raw_end)

    # Basic semantic check: start <= end
    if start_date and end_date:
        try:
            dt_start = dateutil.parser.isoparse(start_date)
            dt_end = dateutil.parser.isoparse(end_date)
            if dt_start > dt_end:
                logger.warning(
                    f"Encounter period invalid: start ({start_date}) "
                    f"is after end ({end_date})."
                )
        except Exception:
            # If we can’t parse for comparison, we’ve already logged a warning.
            pass

    # FHIR 'subject.reference' typically references the patient, but it may be missing
    patient_ref = _extract_reference_id(
        resource.get('subject', {}).get('reference')
    )

    # If 'status' is missing, we can default it (or set to None). 
    # Here we use "UNKNOWN" as a fallback.
    status = resource.get('status', 'UNKNOWN')

    extracted = {
        'id': encounter_id,
        'patient_reference': patient_ref,
        'start_date': start_date,
        'end_date': end_date,
        'status': status
    }

    # Return only keys that are not None. 
    # (If you prefer to keep them as None explicitly, remove this dict comprehension.)
    return {k: v for k, v in extracted.items() if v is not None}


def extract_condition(resource: dict) -> Optional[dict]:
    """Extract fields specific to a Condition resource."""
    if 'id' not in resource:
        return None
    condition_id = _clean_id(resource['id'])
    extracted = {
        'id': condition_id,
        'patient_reference': _extract_reference_id(
            resource.get('subject', {}).get('reference')
        ),
        'code_text': resource.get('code', {}).get('text'),
        'onset_datetime': resource.get('onsetDateTime'),
        'abatement_datetime': resource.get('abatementDateTime'),
        'recorded_date': resource.get('recordedDate'),
        'verification_status': (
            resource.get('verificationStatus', {})
            .get('coding', [{}])[0]
            .get('code')
        )
    }
    return {k: v for k, v in extracted.items() if v is not None}

def extract_observation(resource: dict) -> Optional[dict]:
    """Extract fields specific to an Observation resource."""
    if 'id' not in resource:
        return None
    obs_id = _clean_id(resource['id'])
    value_quantity = resource.get('valueQuantity', {})
    extracted = {
        'id': obs_id,
        'patient_reference': _extract_reference_id(
            resource.get('subject', {}).get('reference')
        ),
        'encounter_reference': _extract_reference_id(
            resource.get('encounter', {}).get('reference')
        ),
        'effective_datetime': resource.get('effectiveDateTime'),
        'issued': resource.get('issued'),
        'value_quantity': value_quantity.get('value'),
        'value_unit': value_quantity.get('unit'),
        'value_code': value_quantity.get('code'),
        'status': resource.get('status')
    }
    return {k: v for k, v in extracted.items() if v is not None}

def extract_procedure(resource: dict) -> Optional[dict]:
    """Extract fields specific to a Procedure resource."""
    if 'id' not in resource:
        return None
    proc_id = _clean_id(resource['id'])
    period = resource.get('performedPeriod', {})
    extracted = {
        'id': proc_id,
        'patient_reference': _extract_reference_id(
            resource.get('subject', {}).get('reference')
        ),
        'encounter_reference': _extract_reference_id(
            resource.get('encounter', {}).get('reference')
        ),
        'start_date': period.get('start'),
        'end_date': period.get('end'),
        'status': resource.get('status'),
        'code_text': resource.get('code', {}).get('text')
    }
    return {k: v for k, v in extracted.items() if v is not None}

def extract_claim(resource: dict) -> Optional[dict]:
    """
    Extract fields specific to a Claim resource.
    https://www.hl7.org/fhir/claim.html
    """
    if 'id' not in resource:
        return None
    claim_id = _clean_id(resource['id'])
    extracted = {
        'id': claim_id,
        'patient_reference': _extract_reference_id(
            resource.get('patient', {}).get('reference')
        ),
        'status': resource.get('status'),  # e.g., active, cancelled, draft, entered-in-error
        'type_code': resource.get('type', {}).get('coding', [{}])[0].get('code'),
        'use': resource.get('use'),  # e.g., claim, preauthorization, predetermination
        'created': resource.get('created'),
        'provider_reference': _extract_reference_id(
            resource.get('provider', {}).get('reference')
        ),
        'insurer_reference': _extract_reference_id(
            resource.get('insurer', {}).get('reference')
        ),
        'priority_code': resource.get('priority', {}).get('coding', [{}])[0].get('code'),
    }
    return {k: v for k, v in extracted.items() if v is not None}


def extract_careplan(resource: dict) -> Optional[dict]:
    """
    Extract fields specific to a CarePlan resource.
    https://www.hl7.org/fhir/careplan.html
    """
    if 'id' not in resource:
        return None
    careplan_id = _clean_id(resource['id'])
    period = resource.get('period', {})
    extracted = {
        'id': careplan_id,
        'patient_reference': _extract_reference_id(
            resource.get('subject', {}).get('reference')
        ),
        'status': resource.get('status'),         # e.g., draft, active, completed
        'intent': resource.get('intent'),         # e.g., proposal, plan, order
        'title': resource.get('title'),
        'description': resource.get('description'),
        'category_code': (
            resource.get('category', [{}])[0]
            .get('coding', [{}])[0]
            .get('code')
            if resource.get('category') else None
        ),
        'start_date': period.get('start'),
        'end_date': period.get('end'),
    }
    return {k: v for k, v in extracted.items() if v is not None}


def extract_careteam(resource: dict) -> Optional[dict]:
    """
    Extract fields specific to a CareTeam resource.
    https://www.hl7.org/fhir/careteam.html
    """
    if 'id' not in resource:
        return None
    careteam_id = _clean_id(resource['id'])
    extracted = {
        'id': careteam_id,
        'patient_reference': _extract_reference_id(
            resource.get('subject', {}).get('reference')
        ),
        'status': resource.get('status'),   # e.g., proposed, active, suspended
        'name': resource.get('name'),
        'category_code': (
            resource.get('category', [{}])[0]
            .get('coding', [{}])[0]
            .get('code')
            if resource.get('category') else None
        ),
    }
    return {k: v for k, v in extracted.items() if v is not None}


def extract_immunization(resource: dict) -> Optional[dict]:
    """
    Extract fields specific to an Immunization resource.
    https://www.hl7.org/fhir/immunization.html
    """
    if 'id' not in resource:
        return None
    immun_id = _clean_id(resource['id'])
    extracted = {
        'id': immun_id,
        'patient_reference': _extract_reference_id(
            resource.get('patient', {}).get('reference')
        ),
        'status': resource.get('status'),  # e.g., completed, entered-in-error, not-done
        'vaccine_code': (
            resource.get('vaccineCode', {}).get('coding', [{}])[0].get('code')
        ),
        'occurrence_date': resource.get('occurrenceDateTime'),
        'primary_source': resource.get('primarySource'),
    }
    return {k: v for k, v in extracted.items() if v is not None}


def extract_medicationrequest(resource: dict) -> Optional[dict]:
    """
    Extract fields specific to a MedicationRequest resource.
    https://www.hl7.org/fhir/medicationrequest.html
    """
    if 'id' not in resource:
        return None
    medreq_id = _clean_id(resource['id'])
    extracted = {
        'id': medreq_id,
        'patient_reference': _extract_reference_id(
            resource.get('subject', {}).get('reference')
        ),
        'encounter_reference': _extract_reference_id(
            resource.get('encounter', {}).get('reference')
        ),
        'status': resource.get('status'),  # e.g., active, completed
        'intent': resource.get('intent'),  # e.g., proposal, plan, order
        'medication_code': (
            resource.get('medicationCodeableConcept', {})
            .get('coding', [{}])[0]
            .get('code')
        ),
        'authored_on': resource.get('authoredOn'),
    }
    return {k: v for k, v in extracted.items() if v is not None}


def extract_medicationadministration(resource: dict) -> Optional[dict]:
    """
    Extract fields specific to a MedicationAdministration resource.
    https://www.hl7.org/fhir/medicationadministration.html
    """
    if 'id' not in resource:
        return None
    medadm_id = _clean_id(resource['id'])
    extracted = {
        'id': medadm_id,
        'patient_reference': _extract_reference_id(
            resource.get('subject', {}).get('reference')
        ),
        'status': resource.get('status'),  # e.g., in-progress, completed
        'medication_code': (
            resource.get('medicationCodeableConcept', {})
            .get('coding', [{}])[0]
            .get('code')
        ),
        # Note: FHIR has multiple ways to specify the time of administration.
        # You might see resource['effectiveTimeDateTime'] or an 'effectivePeriod'.
        'effective_datetime': resource.get('effectiveDateTime'),
    }
    return {k: v for k, v in extracted.items() if v is not None}

RESOURCE_EXTRACTORS = {
    'patient': extract_patient,
    'encounter': extract_encounter,
    'condition': extract_condition,
    'observation': extract_observation,
    'procedure': extract_procedure,
    'claim': extract_claim,
    'careplan': extract_careplan,
    'careteam': extract_careteam,
    'immunization': extract_immunization,
    'medicationrequest': extract_medicationrequest,
    'medicationadministration': extract_medicationadministration,
}



# ------------------------------------------------------------------------------
# 2. ETL Pipeline Class
# ------------------------------------------------------------------------------

'''class HealthcareETL:
    """ETL class that processes FHIR JSON files in parallel and writes CSV outputs."""

    def __init__(self, input_dir: Path, output_dir: Path):
        """
        :param input_dir: Directory containing input FHIR JSON files.
        :param output_dir: Directory to write CSV output.
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging
        self.logger = self._setup_logging()

        # Resource accumulators in memory
        self.resource_data: Dict[str, List[dict]] = {
            rt: [] for rt in RESOURCE_EXTRACTORS.keys()
        }
        
        # Additional resource types you want to capture
        # (that might not have explicit extractors but you still want a CSV for).
        additional_types = [
            'allergyintolerance', 'careplan', 'careteam', 'claim', 'device',
            'diagnosticreport', 'encounter', 'explanationofbenefit', 'goal',
            'imagingstudy', 'immunization', 'medicationadministration',
            'medicationrequest', 'organization', 'practitioner',
        ]
        for rt in additional_types:
            if rt not in self.resource_data:
                self.resource_data[rt] = []

        # Statistics
        self.processed_resources = 0
        self.n_workers = mp.cpu_count()

    def _setup_logging(self) -> logging.Logger:
        """Set up logging to both console and a file in the output directory."""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # Remove existing handlers if any (avoid duplicate logs in notebook environments)
        logger.handlers = []

        # File handler
        file_handler = logging.FileHandler(self.output_dir / "etl.log", mode='w')
        file_handler.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Format
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        return logger

    def _extract_resource(self, resource: dict) -> Tuple[str, Optional[dict]]:
        """
        Determine resource type and use the appropriate extraction function
        if it exists in RESOURCE_EXTRACTORS.

        :param resource: JSON dictionary for one FHIR resource
        :return: (resource_type, extracted_dict) or (resource_type, None) on failure
        """
        resource_type = resource.get('resourceType', '').lower()
        extractor = RESOURCE_EXTRACTORS.get(resource_type, None)

        if extractor is not None:
            # Use the dedicated extractor
            extracted = extractor(resource)
            return resource_type, extracted
        else:
            # If no dedicated extractor, at least capture an ID if present
            resource_id = _clean_id(resource.get('id', ''))
            # Return a small dictionary or None
            return resource_type, {'id': resource_id} if resource_id else None

    def _process_file(self, file_path: Path) -> Dict[str, List[dict]]:
        """
        Process a single FHIR JSON file (Bundle of resources). Return
        a dictionary of resource_type -> list of extracted dictionaries.
        """
        results: Dict[str, List[dict]] = {}
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                bundle = json.load(f)
            
            entries = bundle.get('entry', [])
            if not isinstance(entries, list):
                return results

            for entry in entries:
                resource = entry.get('resource')
                if resource:
                    rtype, extracted = self._extract_resource(resource)
                    if extracted:
                        results.setdefault(rtype, []).append(extracted)

        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
        return results

    def _process_file_batch(self, file_paths: List[Path]) -> Dict[str, List[dict]]:
        """
        Process a batch of files in the current process and accumulate their results.
        """
        batch_results: Dict[str, List[dict]] = {}
        for path in file_paths:
            file_results = self._process_file(path)
            for rtype, records in file_results.items():
                batch_results.setdefault(rtype, []).extend(records)
        return batch_results

    def _save_resource_csv(self, resource_type: str, data: List[dict]) -> None:
        """
        Save a list of dictionaries for a single resource type to CSV.
        Append mode is off by default here (i.e. we overwrite each time).
        If you do chunk-based writes, switch to mode='a' and handle headers carefully.
        """
        if not data:
            return
        try:
            df = pd.DataFrame(data)
            if 'id' in df.columns:
                df = df.sort_values('id')
            output_path = self.output_dir / f"{resource_type}.csv"
            df.to_csv(output_path, index=False)
            self.logger.info(f"Saved {len(df)} {resource_type} records to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving {resource_type} to CSV: {e}")

    def run_pipeline(self) -> None:
        """Main entry point to run the pipeline in parallel batches."""
        start_time = datetime.now()
        self.logger.info(f"Starting ETL pipeline with {self.n_workers} workers")

        # Gather all JSON files
        input_files = [p for p in self.input_dir.iterdir() if p.suffix == '.json']
        total_files = len(input_files)

        if total_files == 0:
            self.logger.warning("No JSON files found in input directory.")
            return

        # Build batches for parallel processing
        batch_size = max(1, total_files // (self.n_workers * 4) or 1)
        batches = [
            input_files[i : i + batch_size]
            for i in range(0, total_files, batch_size)
        ]

        # Parallel execution
        with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
            futures = [executor.submit(self._process_file_batch, b) for b in batches]

            with tqdm(total=len(batches), desc="Processing batches") as pbar:
                for future in futures:
                    try:
                        batch_result = future.result()
                        for rtype, recs in batch_result.items():
                            self.resource_data[rtype].extend(recs)
                            self.processed_resources += len(recs)
                        pbar.update(1)
                    except Exception as e:
                        self.logger.error(f"Error in future result: {e}")

        # After parallel processing, write out CSVs
        for rtype, data in self.resource_data.items():
            self._save_resource_csv(rtype, data)

        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(
            f"""
            ETL Pipeline completed:
            - Duration: {duration:.2f} seconds
            - Total files processed: {total_files}
            - Total resources processed: {self.processed_resources:,}
            - Resources by type:
            {self._format_resource_counts()}
            """
        )

    def _format_resource_counts(self) -> str:
        """Helper to format resource counts in the log."""
        lines = []
        for rtype, data in self.resource_data.items():
            if data:
                lines.append(f"  - {rtype}: {len(data):,} records")
        return "\n".join(lines)'''

class HealthcareETL:
    """ETL class that extracts FHIR JSON data in parallel, then loads into MySQL."""

    def __init__(self, input_dir: Path):
        """
        :param input_dir: Directory containing input FHIR JSON files.
        """
        self.input_dir = input_dir

        # We do NOT create the engine here, because it can't be pickled.
        # We'll create it later in run_pipeline() after the parallel step.

        # Setup logging, resource data accumulators, etc.
        self.output_dir = Path("etl_logs")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = self._setup_logging()
        self.n_workers = mp.cpu_count()
        self.processed_resources = 0

        # Resource accumulators in memory
        self.resource_data: Dict[str, List[dict]] = {}
        for rtype in RESOURCE_EXTRACTORS.keys():
            self.resource_data[rtype] = []

        # Any additional resource types you want to capture but
        # don't have explicit extractors for
        additional_types = [
            "allergyintolerance",
            "careplan",
            "careteam",
            "claim",
            "device",
            "diagnosticreport",
            "encounter",
            "explanationofbenefit",
            "goal",
            "imagingstudy",
            "immunization",
            "medicationadministration",
            "medicationrequest",
            "organization",
            "practitioner",
            "procedure",  # or any others
        ]
        for rt in additional_types:
            if rt not in self.resource_data:
                self.resource_data[rt] = []

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        logger.handlers = []

        file_handler = logging.FileHandler(self.output_dir / "etl.log", mode="w")
        file_handler.setLevel(logging.INFO)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        return logger

    @staticmethod
    def _extract_resource(resource: dict) -> Tuple[str, Optional[dict]]:
        """
        Determine resource type and use the appropriate extraction function
        if it exists in RESOURCE_EXTRACTORS.
        """
        resource_type = resource.get("resourceType", "").lower()
        extractor = RESOURCE_EXTRACTORS.get(resource_type)
        if extractor:
            extracted = extractor(resource)
            return resource_type, extracted
        else:
            # If no dedicated extractor, try to store at least an 'id'
            rid = _clean_id(resource.get("id", ""))
            return resource_type, {"id": rid} if rid else None

    @staticmethod
    def _process_file(file_path: Path) -> Dict[str, List[dict]]:
        """
        Process a single JSON file, returning a dict of resource_type -> list of extracted rows.
        """
        results: Dict[str, List[dict]] = {}
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                bundle = json.load(f)

            entries = bundle.get("entry", [])
            if not isinstance(entries, list):
                return results

            for entry in entries:
                resource = entry.get("resource")
                if resource:
                    rtype, extracted = HealthcareETL._extract_resource(resource)
                    if extracted:
                        results.setdefault(rtype, []).append(extracted)

        except Exception as exc:
            # We won't have self.logger in a static method, but you could
            # print an error or raise
            print(f"Error in file {file_path}: {exc}")

        return results

    def _process_file_batch(self, file_paths: List[Path]) -> Dict[str, List[dict]]:
        """
        Process a batch of files in the current process, accumulate results in a local dict.
        """
        batch_results: Dict[str, List[dict]] = {}
        for path in file_paths:
            file_results = self._process_file(path)
            for rtype, records in file_results.items():
                batch_results.setdefault(rtype, []).extend(records)
        return batch_results

    def run_pipeline(self, mysql_url: str) -> None:
        """
        Main entry point:
        1. Parallel extraction of JSON into Python dicts.
        2. Single-process creation of DB engine.
        3. Single-process insertion into MySQL.
        """
        start_time = datetime.now()
        self.logger.info(f"Starting ETL pipeline with {self.n_workers} workers")

        # 1. Identify input files
        input_files = [p for p in self.input_dir.iterdir() if p.suffix == ".json"]
        total_files = len(input_files)
        if total_files == 0:
            self.logger.warning("No JSON files found.")
            return

        # 2. Parallel extraction
        batch_size = max(1, total_files // (self.n_workers * 4) or 1)
        batches = [
            input_files[i : i + batch_size]
            for i in range(0, total_files, batch_size)
        ]

        # We'll extract in parallel, but engine is NOT created yet
        with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
            futures = [executor.submit(self._process_file_batch, b) for b in batches]
            with tqdm(total=len(batches), desc="Processing batches") as pbar:
                for future in futures:
                    try:
                        batch_result = future.result()
                        # Merge batch_result into self.resource_data
                        for rtype, recs in batch_result.items():
                            self.resource_data[rtype].extend(recs)
                            self.processed_resources += len(recs)
                        pbar.update(1)
                    except Exception as e:
                        self.logger.error(f"Error in future result: {e}")

        # 3. Create the engine in the MAIN process (not pickled)
        self.logger.info("Creating MySQL engine in the main process...")
        try:
            engine = create_engine(mysql_url, echo=False)
        except Exception as e:
            self.logger.error(f"Could not create engine: {e}")
            return

        # 4. Insert data into MySQL
        for rtype, data in self.resource_data.items():
            self._save_resource_mysql(engine, rtype, data)

        duration = (datetime.now() - start_time).total_seconds()
        self.logger.info(
            f"""
            ETL Pipeline completed:
            - Duration: {duration:.2f} seconds
            - Total files processed: {total_files}
            - Total resources processed: {self.processed_resources:,}
            - Resources by type:
            {self._format_resource_counts()}
            """
        )

    def _save_resource_mysql(self, engine: Engine, resource_type: str, data: List[dict]):
        """Insert a list of dicts for `resource_type` into MySQL using pandas."""
        if not data:
            return
        try:
            df = pd.DataFrame(data)
            if "id" in df.columns:
                df.sort_values("id", inplace=True)

            df.to_sql(
                name=resource_type,
                con=engine,
                if_exists="append",  # or "replace" if you want to overwrite
                index=False
            )
            self.logger.info(
                f"Inserted {len(df)} {resource_type} records into MySQL table '{resource_type}'."
            )
        except Exception as e:
            self.logger.error(f"Error inserting {resource_type} into MySQL: {e}")

    def _format_resource_counts(self) -> str:
        """Helper for logging resource counts."""
        lines = []
        for rtype, data in self.resource_data.items():
            if data:
                lines.append(f"  - {rtype}: {len(data):,} records")
        return "\n".join(lines)
# ------------------------------------------------------------------------------
# 3. Main (Command-Line)
# ------------------------------------------------------------------------------

'''def main() -> int:
    parser = argparse.ArgumentParser(description="Run FHIR ETL pipeline.")
    parser.add_argument("--input_dir", required=True, help="Path to input FHIR JSON files.")
    parser.add_argument("--output_dir", required=True, help="Path to output CSV files.")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    # Validate directories
    if not input_dir.exists() or not input_dir.is_dir():
        print(f"Error: input_dir {input_dir} does not exist or is not a directory.")
        return 1

    try:
        etl = HealthcareETL(input_dir, output_dir)
        etl.run_pipeline()
        return 0
    except Exception as e:
        print(f"ETL pipeline failed: {e}")
        return 1'''
def main():
    parser = argparse.ArgumentParser(description="Run FHIR ETL pipeline to MySQL, with parallel extraction.")
    parser.add_argument("--input_dir", required=True, help="Path to input FHIR JSON files.")
    parser.add_argument("--mysql_url", required=True, help="MySQL URL e.g. mysql+pymysql://user:pass@localhost/db")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        print(f"Error: input_dir {input_dir} is invalid.")
        return 1

    etl = HealthcareETL(input_dir)
    etl.run_pipeline(args.mysql_url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
