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
import sqlalchemy as sa
from sqlalchemy import create_engine, Engine, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import ddl
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, String

# ------------------------------------------------------------------------------
# 1. Resource extractors
# ------------------------------------------------------------------------------

def _clean_id(resource_id: Optional[str]) -> Optional[str]:
    """Strip off 'Patient/', 'Encounter/', 'urn:uuid:', etc. to get the bare ID."""
    if not resource_id:
        return None
    # Always remove everything before the final slash (if present):
    if '/' in resource_id:
        resource_id = resource_id.split('/')[-1]
    # If urn:uuid: is present, remove it:
    if 'urn:uuid:' in resource_id:
        resource_id = resource_id.replace('urn:uuid:', '')
    return resource_id.strip()



def _extract_reference_id(reference: Optional[str]) -> Optional[str]:
    """Extract the resource ID portion from a reference string like 'Patient/123'."""
    if not reference:
        return None
    return _clean_id(reference)

def extract_patient(resource: dict) -> Optional[dict]:
    if 'id' not in resource:
        logger.warning("Patient resource missing 'id'; skipping.")
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
    logger.debug(f"Extracted patient: {extracted}")
    return {k: v for k, v in extracted.items() if v is not None}


import logging
from typing import Optional
import dateutil.parser

logger = logging.getLogger(__name__)

def _clean_id(resource_id: Optional[str]) -> Optional[str]:
    """Clean and standardize a FHIR resource ID."""
    if not resource_id:
        return None
    if isinstance(resource_id, str) and 'urn:uuid:' in resource_id:
        resource_id = resource_id.split('urn:uuid:')[-1]
    elif isinstance(resource_id, dict) and 'reference' in resource_id:
        resource_id = resource_id['reference'].split('/')[-1]
    return resource_id.strip() if resource_id else None

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
    extracted = {
        'id': obs_id,
        'patient_reference': _clean_id(_extract_reference_id(
            resource.get('subject', {}).get('reference')
        )),
        'encounter_reference': _clean_id(_extract_reference_id(
            resource.get('encounter', {}).get('reference')
        )),
        'effective_datetime': resource.get('effectiveDateTime'),
        'issued': resource.get('issued'),
        'value_quantity': resource.get('valueQuantity', {}).get('value'),
        'value_unit': resource.get('valueQuantity', {}).get('unit'),
        'value_code': resource.get('valueQuantity', {}).get('code'),
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
    'medical_condition': extract_condition,
    'medical_observation': extract_observation,
    'medical_procedure': extract_procedure,
    'claim': extract_claim,
    'careplan': extract_careplan,
    'careteam': extract_careteam,
    'immunization': extract_immunization,
    'medicationrequest': extract_medicationrequest,
    'medicationadministration': extract_medicationadministration,
}

def create_tables(engine) -> None:
    """
    Create all tables with proper relationships before loading data.
    Adjust or expand columns/foreign keys as needed for your project.
    """
    metadata = MetaData()

    # --------------------------------------------------
    # Patient table - core entity
    # --------------------------------------------------
    patient = Table('patient', metadata,
                    Column('id', String(255), primary_key=True),
                    Column('family_name', String(255)),
                    Column('given_name', String(255)),
                    Column('birth_date', String(50)),
                    Column('gender', String(50)),
                    Column('deceased_datetime', String(50))
                    )

    # --------------------------------------------------
    # Encounter table - references patient
    # --------------------------------------------------
    encounter = Table('encounter', metadata,
                      Column('id', String(255), primary_key=True),
                      Column('patient_reference', String(255), ForeignKey('patient.id')),
                      Column('start_date', String(50)),
                      Column('end_date', String(50)),
                      Column('status', String(50))
                      )

    # --------------------------------------------------
    # Observation table - references patient and encounter
    # --------------------------------------------------
    medical_observation = Table('medical_observation', metadata,
                        Column('id', String(255), primary_key=True),
                        Column('patient_reference', String(255), ForeignKey('patient.id')),
                        Column('encounter_reference', String(255), ForeignKey('encounter.id')),
                        Column('effective_datetime', String(50)),
                        Column('issued', String(50)),
                        Column('value_quantity', Float),
                        Column('value_unit', String(50)),
                        Column('value_code', String(50)),
                        Column('status', String(50))
                        )

    # --------------------------------------------------
    # Condition table - references patient
    # --------------------------------------------------
    medical_condition = Table('medical_condition', metadata,
                      Column('id', String(255), primary_key=True),
                      Column('patient_reference', String(255), ForeignKey('patient.id')),
                      Column('code_text', String(255)),
                      Column('onset_datetime', String(50)),
                      Column('abatement_datetime', String(50)),
                      Column('recorded_date', String(50)),
                      Column('verification_status', String(50))
                      )

    # --------------------------------------------------
    # Procedure table - references patient and encounter
    # --------------------------------------------------
    medical_procedure = Table('medical_procedure', metadata,
                      Column('id', String(255), primary_key=True),
                      Column('patient_reference', String(255), ForeignKey('patient.id')),
                      Column('encounter_reference', String(255), ForeignKey('encounter.id')),
                      Column('start_date', String(50)),
                      Column('end_date', String(50)),
                      Column('status', String(50)),
                      Column('code_text', String(255))
                      )

    # --------------------------------------------------
    # CarePlan table - references patient
    # --------------------------------------------------
    careplan = Table('careplan', metadata,
                     Column('id', String(255), primary_key=True),
                     Column('patient_reference', String(255), ForeignKey('patient.id')),
                     Column('status', String(50)),
                     Column('intent', String(50)),
                     Column('title', String(255)),
                     Column('description', String(500)),
                     Column('category_code', String(50)),
                     Column('start_date', String(50)),
                     Column('end_date', String(50))
                     )

    # --------------------------------------------------
    # CareTeam table - references patient
    # --------------------------------------------------
    careteam = Table('careteam', metadata,
                     Column('id', String(255), primary_key=True),
                     Column('patient_reference', String(255), ForeignKey('patient.id')),
                     Column('status', String(50)),
                     Column('name', String(255)),
                     Column('category_code', String(50))
                     )

    # --------------------------------------------------
    # Immunization - references patient
    # --------------------------------------------------
    immunization = Table('immunization', metadata,
                         Column('id', String(255), primary_key=True),
                         Column('patient_reference', String(255), ForeignKey('patient.id')),
                         Column('status', String(50)),
                         Column('vaccine_code', String(50)),
                         Column('occurrence_date', String(50)),
                         Column('primary_source', String(50))
                         )

    # --------------------------------------------------
    # MedicationRequest - references patient and (optionally) encounter
    # --------------------------------------------------
    medicationrequest = Table('medicationrequest', metadata,
                              Column('id', String(255), primary_key=True),
                              Column('patient_reference', String(255), ForeignKey('patient.id')),
                              Column('encounter_reference', String(255), ForeignKey('encounter.id')),
                              Column('status', String(50)),
                              Column('intent', String(50)),
                              Column('medication_code', String(50)),
                              Column('authored_on', String(50))
                              )

    # --------------------------------------------------
    # MedicationAdministration - references patient
    # --------------------------------------------------
    medicationadministration = Table('medicationadministration', metadata,
                                     Column('id', String(255), primary_key=True),
                                     Column('patient_reference', String(255), ForeignKey('patient.id')),
                                     Column('status', String(50)),
                                     Column('medication_code', String(50)),
                                     Column('effective_datetime', String(50))
                                     )

    # --------------------------------------------------
    # Claim - references patient (and possibly insurer, provider, etc.)
    # --------------------------------------------------
    claim = Table('claim', metadata,
                  Column('id', String(255), primary_key=True),
                  Column('patient_reference', String(255), ForeignKey('patient.id')),
                  Column('status', String(50)),
                  Column('type_code', String(50)),
                  Column('use', String(50)),
                  Column('created', String(50)),
                  Column('provider_reference', String(255)),  # Not referencing a table yet
                  Column('insurer_reference', String(255)),   # same
                  Column('priority_code', String(50))
                  )

    # --------------------------------------------------
    # Organization - might be referenced by claims, etc.
    # --------------------------------------------------
    organization = Table('organization', metadata,
                         Column('id', String(255), primary_key=True),
                         Column('name', String(255)),
                         Column('type', String(50)),
                         Column('active', String(50))
                         # Add more fields if needed
                         )

    # --------------------------------------------------
    # Practitioner - might be referenced by claims, encounters, etc.
    # --------------------------------------------------
    practitioner = Table('practitioner', metadata,
                         Column('id', String(255), primary_key=True),
                         Column('active', String(50)),
                         Column('family_name', String(255)),
                         Column('given_name', String(255)),
                         Column('gender', String(50)),
                         Column('birth_date', String(50))
                         # Add more fields if needed
                         )

    # --------------------------------------------------
    # Device - references patient (optional)
    # --------------------------------------------------
    device = Table('device', metadata,
                   Column('id', String(255), primary_key=True),
                   Column('patient_reference', String(255), ForeignKey('patient.id')),
                   Column('type', String(255)),
                   Column('udi', String(255)),
                   Column('status', String(50))
                   )

    # --------------------------------------------------
    # DiagnosticReport - references patient, maybe encounter
    # --------------------------------------------------
    diagnosticreport = Table('diagnosticreport', metadata,
                             Column('id', String(255), primary_key=True),
                             Column('patient_reference', String(255), ForeignKey('patient.id')),
                             Column('encounter_reference', String(255), ForeignKey('encounter.id')),
                             Column('status', String(50)),
                             Column('code', String(255)),
                             Column('effective_datetime', String(50)),
                             Column('issued', String(50))
                             )

    # --------------------------------------------------
    # ExplanationOfBenefit - references patient, claim?
    # --------------------------------------------------
    explanationofbenefit = Table('explanationofbenefit', metadata,
                                 Column('id', String(255), primary_key=True),
                                 Column('patient_reference', String(255), ForeignKey('patient.id')),
                                 Column('status', String(50)),
                                 Column('type_code', String(50)),
                                 Column('use', String(50)),
                                 Column('claim_reference', String(255)),  # Could reference claim.id if needed
                                 Column('created', String(50))
                                 )

    # --------------------------------------------------
    # Goal - references patient
    # --------------------------------------------------
    goal = Table('goal', metadata,
                 Column('id', String(255), primary_key=True),
                 Column('patient_reference', String(255), ForeignKey('patient.id')),
                 Column('lifecycle_status', String(50)),
                 Column('description', String(255)),
                 Column('start_date', String(50))
                 )

    # --------------------------------------------------
    # ImagingStudy - references patient, encounter
    # --------------------------------------------------
    imagingstudy = Table('imagingstudy', metadata,
                         Column('id', String(255), primary_key=True),
                         Column('patient_reference', String(255), ForeignKey('patient.id')),
                         Column('encounter_reference', String(255), ForeignKey('encounter.id')),
                         Column('started', String(50)),
                         Column('number_of_series', Integer),
                         Column('number_of_instances', Integer)
                         )

    # --------------------------------------------------
    # AllergyIntolerance - references patient
    # --------------------------------------------------
    allergyintolerance = Table('allergyintolerance', metadata,
                               Column('id', String(255), primary_key=True),
                               Column('patient_reference', String(255), ForeignKey('patient.id')),
                               Column('clinical_status', String(50)),
                               Column('verification_status', String(50)),
                               Column('category', String(50)),
                               Column('criticality', String(50))
                               )

    # Finally, create all tables
    metadata.create_all(engine)


@compiles(DropTable, "mysql")
def _compile_drop_table(element, compiler, **kwargs):
    # Ensures "DROP TABLE ... CASCADE" syntax on MySQL
    return compiler.visit_drop_table(element) + " CASCADE"

def get_table_definitions(metadata: MetaData) -> None:
    """
    Define all tables (FHIR resources) with the desired columns and
    foreign key relationships.  No tables are created here; just define them.
    Use metadata.create_all(engine) afterward to create them in the DB.
    """

    # --------------------------------------------------
    # PATIENT (core/parent)
    # --------------------------------------------------
    patient = Table('patient', metadata,
                    Column('id', String(255), primary_key=True),
                    Column('family_name', String(255)),
                    Column('given_name', String(255)),
                    Column('birth_date', String(50)),
                    Column('gender', String(50)),
                    Column('deceased_datetime', String(50))
                    )

    # --------------------------------------------------
    # ENCOUNTER (references patient)
    # --------------------------------------------------
    encounter = Table('encounter', metadata,
                      Column('id', String(255), primary_key=True),
                      Column('patient_reference', String(255),
                             ForeignKey('patient.id', ondelete='CASCADE')),
                      Column('start_date', String(50)),
                      Column('end_date', String(50)),
                      Column('status', String(50))
                      )

    # --------------------------------------------------
    # OBSERVATION (references patient, encounter)
    # --------------------------------------------------
    medical_observation = Table('medical_observation', metadata,
                        Column('id', String(255), primary_key=True),
                        Column('patient_reference', String(255),
                               ForeignKey('patient.id', ondelete='CASCADE')),
                        Column('encounter_reference', String(255),
                               ForeignKey('encounter.id', ondelete='CASCADE')),
                        Column('effective_datetime', String(50)),
                        Column('issued', String(50)),
                        Column('value_quantity', Float),
                        Column('value_unit', String(50)),
                        Column('value_code', String(50)),
                        Column('status', String(50))
                        )

    # --------------------------------------------------
    # PROCEDURE (references patient, encounter)
    # --------------------------------------------------
    medical_procedure = Table('medical_procedure', metadata,
                      Column('id', String(255), primary_key=True),
                      Column('patient_reference', String(255),
                             ForeignKey('patient.id', ondelete='CASCADE')),
                      Column('encounter_reference', String(255),
                             ForeignKey('encounter.id', ondelete='CASCADE')),
                      Column('start_date', String(50)),
                      Column('end_date', String(50)),
                      Column('status', String(50)),
                      Column('code_text', String(255))
                      )

    # --------------------------------------------------
    # CONDITION (references patient)
    # --------------------------------------------------
    medical_condition = Table('medical_condition', metadata,
                      Column('id', String(255), primary_key=True),
                      Column('patient_reference', String(255),
                             ForeignKey('patient.id', ondelete='CASCADE')),
                      Column('code_text', String(255)),
                      Column('onset_datetime', String(50)),
                      Column('abatement_datetime', String(50)),
                      Column('recorded_date', String(50)),
                      Column('verification_status', String(50))
                      )

    # --------------------------------------------------
    # ALLERGYINTOLERANCE (references patient)
    # --------------------------------------------------
    allergyintolerance = Table('allergyintolerance', metadata,
                               Column('id', String(255), primary_key=True),
                               Column('patient_reference', String(255),
                                      ForeignKey('patient.id', ondelete='CASCADE')),
                               Column('clinical_status', String(50)),
                               Column('verification_status', String(50)),
                               Column('category', String(50)),
                               Column('criticality', String(50))
                               )

    # --------------------------------------------------
    # CAREPLAN (references patient)
    # --------------------------------------------------
    careplan = Table('careplan', metadata,
                     Column('id', String(255), primary_key=True),
                     Column('patient_reference', String(255),
                            ForeignKey('patient.id', ondelete='CASCADE')),
                     Column('status', String(50)),
                     Column('intent', String(50)),
                     Column('title', String(255)),
                     Column('description', String(500)),
                     Column('category_code', String(50)),
                     Column('start_date', String(50)),
                     Column('end_date', String(50))
                     )

    # --------------------------------------------------
    # CARETEAM (references patient)
    # --------------------------------------------------
    careteam = Table('careteam', metadata,
                     Column('id', String(255), primary_key=True),
                     Column('patient_reference', String(255),
                            ForeignKey('patient.id', ondelete='CASCADE')),
                     Column('status', String(50)),
                     Column('name', String(255)),
                     Column('category_code', String(50))
                     )

    # --------------------------------------------------
    # IMMUNIZATION (references patient)
    # --------------------------------------------------
    immunization = Table('immunization', metadata,
                         Column('id', String(255), primary_key=True),
                         Column('patient_reference', String(255),
                                ForeignKey('patient.id', ondelete='CASCADE')),
                         Column('status', String(50)),
                         Column('vaccine_code', String(50)),
                         Column('occurrence_date', String(50)),
                         Column('primary_source', String(50))
                         )

    # --------------------------------------------------
    # MEDICATIONREQUEST (references patient, encounter)
    # --------------------------------------------------
    medicationrequest = Table('medicationrequest', metadata,
                              Column('id', String(255), primary_key=True),
                              Column('patient_reference', String(255),
                                     ForeignKey('patient.id', ondelete='CASCADE')),
                              Column('encounter_reference', String(255),
                                     ForeignKey('encounter.id', ondelete='CASCADE')),
                              Column('status', String(50)),
                              Column('intent', String(50)),
                              Column('medication_code', String(50)),
                              Column('authored_on', String(50))
                              )

    # --------------------------------------------------
    # MEDICATIONADMINISTRATION (references patient)
    # --------------------------------------------------
    medicationadministration = Table('medicationadministration', metadata,
                                     Column('id', String(255), primary_key=True),
                                     Column('patient_reference', String(255),
                                            ForeignKey('patient.id', ondelete='CASCADE')),
                                     Column('status', String(50)),
                                     Column('medication_code', String(50)),
                                     Column('effective_datetime', String(50))
                                     )

    # --------------------------------------------------
    # CLAIM (references patient) - could also reference organization or practitioner
    # --------------------------------------------------
    claim = Table('claim', metadata,
                  Column('id', String(255), primary_key=True),
                  Column('patient_reference', String(255),
                         ForeignKey('patient.id', ondelete='CASCADE')),
                  Column('status', String(50)),
                  Column('type_code', String(50)),
                  Column('use', String(50)),
                  Column('created', String(50)),
                  Column('provider_reference', String(255)),  # optional
                  Column('insurer_reference', String(255)),   # optional
                  Column('priority_code', String(50))
                  )

    # --------------------------------------------------
    # DEVICE (references patient)
    # --------------------------------------------------
    device = Table('device', metadata,
                   Column('id', String(255), primary_key=True),
                   Column('patient_reference', String(255),
                          ForeignKey('patient.id', ondelete='CASCADE')),
                   Column('type', String(255)),
                   Column('udi', String(255)),
                   Column('status', String(50))
                   )

    # --------------------------------------------------
    # DIAGNOSTICREPORT (references patient, encounter)
    # --------------------------------------------------
    diagnosticreport = Table('diagnosticreport', metadata,
                             Column('id', String(255), primary_key=True),
                             Column('patient_reference', String(255),
                                    ForeignKey('patient.id', ondelete='CASCADE')),
                             Column('encounter_reference', String(255),
                                    ForeignKey('encounter.id', ondelete='CASCADE')),
                             Column('status', String(50)),
                             Column('code', String(255)),
                             Column('effective_datetime', String(50)),
                             Column('issued', String(50))
                             )

    # --------------------------------------------------
    # EXPLANATIONOFBENEFIT (references patient, claim?)
    # --------------------------------------------------
    explanationofbenefit = Table('explanationofbenefit', metadata,
                                 Column('id', String(255), primary_key=True),
                                 Column('patient_reference', String(255),
                                        ForeignKey('patient.id', ondelete='CASCADE')),
                                 Column('status', String(50)),
                                 Column('type_code', String(50)),
                                 Column('use', String(50)),
                                 Column('claim_reference', String(255)),  # could reference claim.id if you like
                                 Column('created', String(50))
                                 )

    # --------------------------------------------------
    # GOAL (references patient)
    # --------------------------------------------------
    goal = Table('goal', metadata,
                 Column('id', String(255), primary_key=True),
                 Column('patient_reference', String(255),
                        ForeignKey('patient.id', ondelete='CASCADE')),
                 Column('lifecycle_status', String(50)),
                 Column('description', String(255)),
                 Column('start_date', String(50))
                 )

    # --------------------------------------------------
    # IMAGINGSTUDY (references patient, encounter)
    # --------------------------------------------------
    imagingstudy = Table('imagingstudy', metadata,
                         Column('id', String(255), primary_key=True),
                         Column('patient_reference', String(255),
                                ForeignKey('patient.id', ondelete='CASCADE')),
                         Column('encounter_reference', String(255),
                                ForeignKey('encounter.id', ondelete='CASCADE')),
                         Column('started', String(50)),
                         Column('number_of_series', Integer),
                         Column('number_of_instances', Integer)
                         )

    # --------------------------------------------------
    # ORGANIZATION - may not reference patient
    # --------------------------------------------------
    organization = Table('organization', metadata,
                         Column('id', String(255), primary_key=True),
                         Column('name', String(255)),
                         Column('type', String(50)),
                         Column('active', String(50))
                         )

    # --------------------------------------------------
    # PRACTITIONER - typically not referencing patient
    # --------------------------------------------------
    practitioner = Table('practitioner', metadata,
                         Column('id', String(255), primary_key=True),
                         Column('active', String(50)),
                         Column('family_name', String(255)),
                         Column('given_name', String(255)),
                         Column('gender', String(50)),
                         Column('birth_date', String(50))
                         )

def create_database_schema(engine: Engine, logger) -> None:
    """Create database schema with proper relationship handling."""
    metadata = MetaData()

    # Get all table definitions
    get_table_definitions(metadata)

    try:
        # Drop all tables in reverse dependency order
        logger.info("Dropping existing tables...")
        metadata.reflect(bind=engine)
        metadata.drop_all(bind=engine)

        # Create all tables in correct dependency order
        logger.info("Creating new tables with relationships...")
        metadata.create_all(bind=engine)

    except Exception as e:
        logger.error(f"Error in schema creation: {e}")
        raise
# ------------------------------------------------------------------------------
# 2. ETL Pipeline Class
# ------------------------------------------------------------------------------


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
            "medical_procedure",  # or any others
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
        if resource_type == "condition":
            resource_type = "medical_condition"
        elif resource_type == "observation":
            resource_type = "medical_observation"
        elif resource_type == 'procedure':
            resource_type = 'medical_procedure'
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



    def modified_save_resource_mysql(self, engine, resource_type: str, data: list):
        if not data:
            self.logger.warning(f"No data provided for {resource_type}. Skipping.")
            return

        try:
            # Handle the patient resource differently
            if resource_type == 'patient':
                self.logger.info(f"Loading {len(data)} patient records.")
                df = pd.DataFrame(data)
                self.logger.debug(f"Patient DataFrame columns: {df.columns}")
            else:
                # Convert data to DataFrame for other resources
                df = pd.DataFrame(data)

                # Log DataFrame info
                self.logger.debug(f"{resource_type} DataFrame columns: {df.columns}")

                # Check if 'patient_reference' exists
                if 'patient_reference' in df.columns:
                    self.logger.debug(f"Sample patient_reference values: {df['patient_reference'].head()}")
                else:
                    self.logger.warning(f"Column 'patient_reference' is missing in {resource_type}")

                # Clean ID and reference columns
                for col in df.columns:
                    if 'id' in col.lower() or 'reference' in col.lower():
                        df[col] = df[col].apply(lambda x: _clean_id(str(x)) if pd.notnull(x) else x)

                # Validate patient references
                if resource_type != 'patient' and 'patient_reference' in df.columns:
                    valid_patient_ids = set(pd.read_sql('SELECT id FROM patient', engine)['id'])
                    self.logger.debug(f"Valid patient IDs: {list(valid_patient_ids)[:5]} (Total: {len(valid_patient_ids)})")
                    before_count = len(df)
                    df = df[df['patient_reference'].isin(valid_patient_ids)]
                    after_count = len(df)
                    self.logger.info(f"Filtered {resource_type} records: {before_count} -> {after_count}")

                # Validate encounter references
                if 'encounter_reference' in df.columns:
                    valid_encounter_ids = set(pd.read_sql('SELECT id FROM encounter', engine)['id'])
                    self.logger.debug(f"Valid encounter IDs: {list(valid_encounter_ids)[:5]} (Total: {len(valid_encounter_ids)})")
                    before_count = len(df)
                    df = df[df['encounter_reference'].isin(valid_encounter_ids)]
                    after_count = len(df)
                    self.logger.info(f"Filtered {resource_type} records: {before_count} -> {after_count}")

            # Insert valid data into MySQL
            if len(df) > 0:
                df.to_sql(
                    name=resource_type,
                    con=engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=1000
                )
                self.logger.info(f"Successfully inserted {len(df)} {resource_type} records")
            else:
                self.logger.warning(f"No valid {resource_type} records to insert")
        except Exception as e:
            self.logger.error(f"Error inserting {resource_type}: {str(e)}")


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
            engine.connect()
            self.logger.info("Engine created and connected.")
            self.logger.info("Creating tables with relationships...")
            create_database_schema(engine, self.logger)

            # Define loading order based on dependencies
            loading_order = [
                'patient',          # Load core entity first
                'encounter',        # Depends on patient
                'medical_observation',
                'medical_condition', # Depends on patient and encounter
                'medical_procedure',        # Depends on patient and encounter        # Depends on patient
                'careplan',        # Depends on patient
                'careteam',        # Depends on patient
                'immunization',    # Depends on patient
                'medicationrequest', # Depends on patient
                'medicationadministration'  # Depends on patient
            ]

            # Load data in correct order
            for resource_type in loading_order:
                if resource_type in self.resource_data and self.resource_data[resource_type]:
                    self.logger.info(f"Loading {resource_type} data...")
                    # Add count logging
                    before_count = len(self.resource_data[resource_type])
                    self.modified_save_resource_mysql(engine, resource_type,
                                                      self.resource_data[resource_type])

                    # Verify loaded count
                    actual_count = pd.read_sql(
                        f'SELECT COUNT(*) as cnt FROM {resource_type}',
                        engine
                    ).iloc[0]['cnt']
                    self.logger.info(
                        f"{resource_type}: Attempted={before_count}, Loaded={actual_count}"
                    )
        except Exception as e:
            self.logger.error(f"Could not create engine: {e}")
            return

        '''# 4. Insert data into MySQL
        for rtype, data in self.resource_data.items():
            self._save_resource_mysql(engine, rtype, data)'''

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
