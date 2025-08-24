"""Model utilities and validation helpers."""
from typing import Any, Dict, List
import json
import jsonschema
from .ir import IRPackage, MigrationReport


def validate_ir_package(data: Dict[str, Any]) -> IRPackage:
    """Validate and create IRPackage from dictionary data.
    
    Args:
        data: Dictionary representation of IR package
        
    Returns:
        Validated IRPackage instance
        
    Raises:
        ValidationError: If data doesn't match schema
    """
    return IRPackage.model_validate(data)


def ir_to_json(package: IRPackage, indent: int = 2) -> str:
    """Convert IRPackage to JSON string.
    
    Args:
        package: IRPackage instance
        indent: JSON indentation level
        
    Returns:
        JSON string representation
    """
    return package.model_dump_json(indent=indent)


def ir_from_json(json_str: str) -> IRPackage:
    """Create IRPackage from JSON string.
    
    Args:
        json_str: JSON representation
        
    Returns:
        IRPackage instance
        
    Raises:
        ValidationError: If JSON doesn't match schema
    """
    data = json.loads(json_str)
    return validate_ir_package(data)


def generate_ir_schema() -> Dict[str, Any]:
    """Generate JSON schema for IR package validation.
    
    Returns:
        JSON schema dictionary
    """
    return IRPackage.model_json_schema()


class IRValidator:
    """JSON schema validator for IR packages."""
    
    def __init__(self):
        self.schema = generate_ir_schema()
        self.validator = jsonschema.Draft7Validator(self.schema)
    
    def validate(self, data: Dict[str, Any]) -> List[str]:
        """Validate IR data against schema.
        
        Args:
            data: Dictionary to validate
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        for error in self.validator.iter_errors(data):
            errors.append(f"{'.'.join(str(p) for p in error.path)}: {error.message}")
        return errors
    
    def is_valid(self, data: Dict[str, Any]) -> bool:
        """Check if data is valid IR package.
        
        Args:
            data: Dictionary to validate
            
        Returns:
            True if valid, False otherwise
        """
        return len(self.validate(data)) == 0