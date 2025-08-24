"""Security handler for encrypted SSIS packages.

Handles package protection levels and sensitive data according to 
the security requirements in the migration plan.
"""
import logging
from typing import Dict, Optional, List, Any
from models.ir import ProtectionLevel

logger = logging.getLogger(__name__)


class SSISSecurityHandler:
    """Handler for SSIS package security and encryption."""
    
    def __init__(self, password: Optional[str] = None):
        """Initialize security handler.
        
        Args:
            password: Optional package password for encrypted packages
        """
        self.password = password
        self.sensitive_properties = {
            'password', 'connectionstring', 'userpassword', 'servername',
            'userid', 'username', 'initialcatalog', 'database',
            'sqlstatement', 'commandtext'
        }
    
    def handle_protection_level(self, protection_level: ProtectionLevel, 
                               properties: Dict[str, Any]) -> Dict[str, Any]:
        """Handle package protection level and secure sensitive properties.
        
        Args:
            protection_level: Package protection level
            properties: Properties dictionary to secure
            
        Returns:
            Secured properties dictionary
        """
        logger.info(f"Handling protection level: {protection_level.value}")
        
        if protection_level == ProtectionLevel.DONT_SAVE_SENSITIVE:
            return self._redact_sensitive_properties(properties)
        
        elif protection_level in {
            ProtectionLevel.ENCRYPT_SENSITIVE_WITH_PASSWORD,
            ProtectionLevel.ENCRYPT_SENSITIVE_WITH_USER_KEY
        }:
            if not self.password and protection_level == ProtectionLevel.ENCRYPT_SENSITIVE_WITH_PASSWORD:
                logger.warning("Encrypted package requires password - sensitive properties will be redacted")
                return self._redact_sensitive_properties(properties)
            else:
                return self._decrypt_sensitive_properties(properties, protection_level)
        
        elif protection_level in {
            ProtectionLevel.ENCRYPT_ALL_WITH_PASSWORD,
            ProtectionLevel.ENCRYPT_ALL_WITH_USER_KEY
        }:
            if not self.password and protection_level == ProtectionLevel.ENCRYPT_ALL_WITH_PASSWORD:
                logger.error("Fully encrypted package requires password - cannot process")
                raise ValueError("Password required for encrypted package")
            else:
                return self._decrypt_all_properties(properties, protection_level)
        
        return properties
    
    def _redact_sensitive_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Redact sensitive properties from dictionary.
        
        Args:
            properties: Properties to redact
            
        Returns:
            Properties with sensitive values redacted
        """
        redacted = properties.copy()
        
        for key, value in properties.items():
            if self._is_sensitive_property(key):
                redacted[key] = "[REDACTED]"
                logger.debug(f"Redacted sensitive property: {key}")
        
        return redacted
    
    def _decrypt_sensitive_properties(self, properties: Dict[str, Any], 
                                    protection_level: ProtectionLevel) -> Dict[str, Any]:
        """Decrypt sensitive properties (placeholder implementation).
        
        Note: This is a placeholder. In a real implementation, you would need
        to use the SSIS decryption APIs or equivalent libraries.
        
        Args:
            properties: Properties to decrypt
            protection_level: Protection level
            
        Returns:
            Properties with sensitive values decrypted or redacted
        """
        logger.warning("Decryption not implemented - redacting sensitive properties")
        return self._redact_sensitive_properties(properties)
    
    def _decrypt_all_properties(self, properties: Dict[str, Any],
                              protection_level: ProtectionLevel) -> Dict[str, Any]:
        """Decrypt all properties (placeholder implementation).
        
        Note: This is a placeholder. Full package decryption requires
        SSIS runtime or specialized decryption tools.
        
        Args:
            properties: Properties to decrypt
            protection_level: Protection level
            
        Returns:
            Decrypted properties or raises exception
        """
        logger.error("Full package decryption not implemented")
        raise NotImplementedError(
            "Full package decryption requires SSIS runtime or specialized tools"
        )
    
    def _is_sensitive_property(self, property_name: str) -> bool:
        """Check if a property name is considered sensitive.
        
        Args:
            property_name: Property name to check
            
        Returns:
            True if sensitive, False otherwise
        """
        return property_name.lower() in self.sensitive_properties
    
    def generate_connection_placeholders(self, connections: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """Generate connection placeholders for secured connections.
        
        Args:
            connections: List of connection manager dictionaries
            
        Returns:
            List of connection placeholder dictionaries
        """
        placeholders = []
        
        for conn in connections:
            conn_id = conn.get('id', 'unknown')
            conn_type = conn.get('type', 'OLEDB')
            
            placeholder = {
                'connection_id': conn_id,
                'airflow_conn_id': f"ssis_{conn_id.lower().replace(' ', '_')}",
                'type': conn_type,
                'description': f"Migrated from SSIS connection: {conn.get('name', conn_id)}",
                'setup_required': True
            }
            
            placeholders.append(placeholder)
        
        return placeholders
    
    def create_airflow_connection_setup(self, connections: List[Dict[str, Any]]) -> str:
        """Create Airflow connection setup script.
        
        Args:
            connections: List of connection dictionaries
            
        Returns:
            Python script for setting up Airflow connections
        """
        script_lines = [
            "#!/usr/bin/env python3",
            "\"\"\"",
            "Setup script for Airflow connections migrated from SSIS.",
            "Run this script to create the required connections in Airflow.",
            "\"\"\"",
            "",
            "from airflow.models import Connection",
            "from airflow import settings",
            "",
            "def create_connections():",
            "    \"\"\"Create migrated SSIS connections in Airflow.\"\"\"",
            "    session = settings.Session()",
            ""
        ]
        
        for conn in connections:
            conn_id = conn.get('id', 'unknown') 
            conn_name = conn.get('name', conn_id)
            conn_type = conn.get('type', 'OLEDB')
            airflow_conn_id = f"ssis_{conn_id.lower().replace(' ', '_')}"
            
            # Map SSIS connection type to Airflow connection type
            airflow_type = self._map_to_airflow_conn_type(conn_type)
            
            script_lines.extend([
                f"    # Connection: {conn_name}",
                f"    conn_{len(script_lines)} = Connection(",
                f"        conn_id='{airflow_conn_id}',",
                f"        conn_type='{airflow_type}',",
                f"        description='Migrated from SSIS: {conn_name}',",
                f"        host='TODO_UPDATE_HOST',", 
                f"        schema='TODO_UPDATE_DATABASE',",
                f"        login='TODO_UPDATE_USERNAME',",
                f"        password='TODO_UPDATE_PASSWORD'",
                "    )",
                "",
                f"    # Delete existing connection if it exists",
                f"    existing = session.query(Connection).filter(Connection.conn_id == '{airflow_conn_id}').first()",
                "    if existing:",
                "        session.delete(existing)",
                "",
                f"    session.add(conn_{len(script_lines)})",
                "    session.commit()",
                f"    print(f'Created connection: {airflow_conn_id}')",
                ""
            ])
        
        script_lines.extend([
            "if __name__ == '__main__':",
            "    create_connections()",
            "    print('Connection setup complete!')",
            ""
        ])
        
        return "\n".join(script_lines)
    
    def create_dbt_profiles_template(self, connections: List[Dict[str, Any]]) -> str:
        """Create dbt profiles.yml template.
        
        Args:
            connections: List of connection dictionaries
            
        Returns:
            YAML template for dbt profiles
        """
        # For dbt, we typically use one main connection (usually Snowflake)
        snowflake_connections = [c for c in connections if 'snowflake' in c.get('type', '').lower()]
        
        if snowflake_connections:
            template = """# dbt profiles.yml template
# Generated from SSIS migration
# Update with your actual Snowflake connection details

ssis_migration:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('DBT_SNOWFLAKE_USER') }}"
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      database: "{{ env_var('DBT_SNOWFLAKE_DATABASE') }}"
      warehouse: "{{ env_var('DBT_SNOWFLAKE_WAREHOUSE') }}"
      schema: "{{ env_var('DBT_SNOWFLAKE_SCHEMA') }}"
      role: "{{ env_var('DBT_SNOWFLAKE_ROLE') }}"
      threads: 4
      keepalives_idle: 0
      
    prod:
      type: snowflake
      account: "{{ env_var('DBT_SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('DBT_SNOWFLAKE_USER') }}"
      password: "{{ env_var('DBT_SNOWFLAKE_PASSWORD') }}"
      database: "{{ env_var('DBT_SNOWFLAKE_DATABASE_PROD') }}"
      warehouse: "{{ env_var('DBT_SNOWFLAKE_WAREHOUSE') }}"
      schema: "{{ env_var('DBT_SNOWFLAKE_SCHEMA_PROD') }}"
      role: "{{ env_var('DBT_SNOWFLAKE_ROLE') }}"
      threads: 8
      keepalives_idle: 0
"""
        else:
            template = """# dbt profiles.yml template
# Generated from SSIS migration
# No Snowflake connections detected - update with your target warehouse

ssis_migration:
  target: dev
  outputs:
    dev:
      type: snowflake  # or your target warehouse type
      # Add connection details here
      threads: 4
"""
        
        return template
    
    def _map_to_airflow_conn_type(self, ssis_conn_type: str) -> str:
        """Map SSIS connection type to Airflow connection type.
        
        Args:
            ssis_conn_type: SSIS connection type
            
        Returns:
            Airflow connection type string
        """
        mapping = {
            'OLEDB': 'mssql',
            'ADO.NET': 'mssql',
            'SNOWFLAKE': 'snowflake',
            'HTTP': 'http',
            'FTP': 'ftp',
            'SMTP': 'email',
            'FLATFILE': 'file',
            'FILE': 'file'
        }
        
        return mapping.get(ssis_conn_type.upper(), 'generic')
    
    def validate_security_compliance(self, package_data: Dict[str, Any]) -> List[str]:
        """Validate security compliance of migrated package.
        
        Args:
            package_data: Package IR data
            
        Returns:
            List of security warnings/recommendations
        """
        warnings = []
        
        # Check for redacted connections
        connections = package_data.get('connection_managers', [])
        redacted_connections = [
            c for c in connections 
            if any('[REDACTED]' in str(v) for v in c.get('properties', {}).values())
        ]
        
        if redacted_connections:
            warnings.append(
                f"Found {len(redacted_connections)} connections with redacted sensitive data - "
                "manual configuration required in target environment"
            )
        
        # Check for hardcoded credentials in SQL/expressions
        executables = package_data.get('executables', [])
        for exe in executables:
            sql = exe.get('sql', '')
            if sql and any(keyword in sql.lower() for keyword in ['password=', 'pwd=', 'user id=']):
                warnings.append(
                    f"Task '{exe.get('object_name')}' may contain hardcoded credentials in SQL"
                )
        
        # Check protection level
        protection_level = package_data.get('protection_level')
        if protection_level == 'DontSaveSensitive':
            warnings.append(
                "Package uses 'DontSaveSensitive' protection - ensure all connections "
                "are properly configured in target environment"
            )
        
        return warnings