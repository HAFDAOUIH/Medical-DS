import json
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, Set, Any

class FHIRExplorer:
    def __init__(self, input_dir: str):
        self.input_dir = Path(input_dir)
        self.resource_types = defaultdict(int)
        self.field_patterns = defaultdict(set)
        
    def explore_file(self, file_path: str) -> None:
        """Analyze a single FHIR JSON file."""
        try:
            with open(file_path) as f:
                data = json.load(f)
                
            if 'entry' in data:
                for entry in data['entry']:
                    if 'resource' in entry:
                        resource = entry['resource']
                        if 'resourceType' in resource:
                            # Count resource types
                            resource_type = resource['resourceType']
                            self.resource_types[resource_type] += 1
                            
                            # Collect field patterns
                            self._collect_fields(resource_type, resource)
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    def _collect_fields(self, resource_type: str, data: Dict, prefix: str = '') -> None:
        """Recursively collect field patterns from a resource."""
        for key, value in data.items():
            if prefix:
                full_key = f"{prefix}.{key}"
            else:
                full_key = key
                
            if isinstance(value, dict):
                self.field_patterns[resource_type].add(f"{full_key} (object)")
                self._collect_fields(resource_type, value, full_key)
            elif isinstance(value, list):
                if value:
                    type_desc = f"array of {type(value[0]).__name__}s"
                else:
                    type_desc = "empty array"
                self.field_patterns[resource_type].add(f"{full_key} ({type_desc})")
            else:
                self.field_patterns[resource_type].add(f"{full_key} ({type(value).__name__})")
    
    def analyze_directory(self) -> None:
        """Analyze all JSON files in the directory."""
        for file_name in os.listdir(self.input_dir):
            if file_name.endswith('.json'):
                self.explore_file(self.input_dir / file_name)
    
    def print_summary(self) -> None:
        """Print summary of the analysis."""
        print("\n=== FHIR Resource Analysis ===\n")
        
        print("Resource Types Found:")
        print("-" * 40)
        for resource_type, count in sorted(self.resource_types.items()):
            print(f"{resource_type}: {count} instances")
        
        print("\nField Patterns by Resource Type:")
        print("-" * 40)
        for resource_type, fields in sorted(self.field_patterns.items()):
            print(f"\n{resource_type}:")
            for field in sorted(fields):
                print(f"  - {field}")

def main():
    # Initialize explorer
    explorer = FHIRExplorer("fhir")
    
    # Analyze files
    print("Analyzing FHIR JSON files...")
    explorer.analyze_directory()
    
    # Print results
    explorer.print_summary()

if __name__ == "__main__":
    main()
