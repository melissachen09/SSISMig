"""Migration reporting functionality.

Generates comprehensive reports about the migration process, analysis,
and recommendations for manual review.
"""
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

from models.ir import IRPackage, MigrationReport


@dataclass
class ComponentMapping:
    """Mapping information for SSIS component to target platform."""
    ssis_component: str
    target_component: str
    confidence: str  # High, Medium, Low
    notes: str
    manual_review: bool = False


class MigrationReporter:
    """Generator for migration reports and analysis."""
    
    def __init__(self):
        """Initialize migration reporter."""
        self.component_mappings = self._load_component_mappings()
    
    def _load_component_mappings(self) -> Dict[str, ComponentMapping]:
        """Load component mapping definitions."""
        mappings = {}
        
        # SSIS Task to Airflow mappings
        airflow_mappings = [
            ComponentMapping("ExecuteSQL", "SnowflakeOperator", "High", "Direct SQL execution"),
            ComponentMapping("DataFlow", "TaskGroup + Multiple Operators", "Medium", "Complex data pipeline"),
            ComponentMapping("ScriptTask", "PythonOperator", "Low", "Requires manual code conversion"),
            ComponentMapping("SequenceContainer", "TaskGroup", "High", "Direct container mapping"),
            ComponentMapping("ForEachLoop", "Dynamic Task Mapping", "Medium", "Uses @task decorator"),
            ComponentMapping("BulkInsert", "SnowflakeOperator COPY", "High", "Use COPY INTO command"),
            ComponentMapping("FileSystem", "BashOperator", "Medium", "File operations"),
            ComponentMapping("FTP", "FTPHook", "Medium", "May need custom implementation"),
            ComponentMapping("SendMail", "EmailOperator", "High", "Built-in email support"),
        ]
        
        # Data Flow Component to dbt mappings  
        dbt_mappings = [
            ComponentMapping("OLEDBSource", "source() reference", "High", "Map to dbt source"),
            ComponentMapping("FlatFileSource", "External table or stage", "Medium", "File ingestion needed"),
            ComponentMapping("DerivedColumn", "SELECT with expressions", "High", "Direct SQL conversion"),
            ComponentMapping("Lookup", "JOIN to reference table", "High", "Use ref() for lookups"),
            ComponentMapping("ConditionalSplit", "Multiple models + WHERE", "Medium", "Split logic into models"),
            ComponentMapping("UnionAll", "UNION ALL in SQL", "High", "Direct SQL conversion"),
            ComponentMapping("Aggregate", "GROUP BY with aggregations", "High", "Direct SQL conversion"),
            ComponentMapping("Sort", "ORDER BY clause", "High", "For incremental models"),
            ComponentMapping("OLEDBDestination", "Model materialization", "High", "Table/incremental model"),
        ]
        
        all_mappings = airflow_mappings + dbt_mappings
        
        for mapping in all_mappings:
            mappings[mapping.ssis_component] = mapping
        
        return mappings
    
    def generate_package_report(self, ir_package: IRPackage, 
                               migration_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive package migration report.
        
        Args:
            ir_package: IR package data
            migration_results: Results from migration generators
            
        Returns:
            Complete migration report
        """
        report = {
            "package_info": self._analyze_package_info(ir_package),
            "component_analysis": self._analyze_components(ir_package),
            "complexity_analysis": self._analyze_complexity(ir_package),
            "migration_recommendations": self._generate_recommendations(ir_package),
            "generated_artifacts": migration_results,
            "manual_review_items": self._identify_manual_review_items(ir_package),
            "risk_assessment": self._assess_migration_risks(ir_package),
            "next_steps": self._generate_next_steps(ir_package, migration_results),
            "generated_at": datetime.now().isoformat()
        }
        
        return report
    
    def _analyze_package_info(self, ir_package: IRPackage) -> Dict[str, Any]:
        """Analyze basic package information."""
        return {
            "name": ir_package.package_name,
            "protection_level": ir_package.protection_level.value,
            "total_tasks": len(ir_package.executables),
            "total_connections": len(ir_package.connection_managers),
            "total_variables": len(ir_package.variables),
            "total_parameters": len(ir_package.parameters),
            "total_constraints": len(ir_package.edges),
            "total_expressions": len(ir_package.expressions),
            "is_transformation_only": ir_package.is_transformation_only(),
            "creation_info": {
                "version_build": ir_package.version_build,
                "creator_name": ir_package.creator_name,
                "creation_date": ir_package.creation_date
            }
        }
    
    def _analyze_components(self, ir_package: IRPackage) -> Dict[str, Any]:
        """Analyze SSIS components and their mappings."""
        task_types = {}
        component_types = {}
        mappings = []
        
        # Analyze task types
        for exe in ir_package.executables:
            task_type = exe.type.value
            task_types[task_type] = task_types.get(task_type, 0) + 1
            
            # Get mapping info
            if task_type in self.component_mappings:
                mapping = self.component_mappings[task_type]
                mappings.append({
                    "ssis_component": task_type,
                    "target_component": mapping.target_component,
                    "confidence": mapping.confidence,
                    "notes": mapping.notes,
                    "manual_review": mapping.manual_review,
                    "occurrences": 1
                })
        
        # Analyze data flow components
        for exe in ir_package.executables:
            if exe.type.value == "DataFlow":
                for comp in exe.components:
                    comp_type = comp.component_type.value
                    component_types[comp_type] = component_types.get(comp_type, 0) + 1
                    
                    # Get mapping info
                    if comp_type in self.component_mappings:
                        mapping = self.component_mappings[comp_type]
                        mappings.append({
                            "ssis_component": comp_type,
                            "target_component": mapping.target_component,
                            "confidence": mapping.confidence,
                            "notes": mapping.notes,
                            "manual_review": mapping.manual_review,
                            "occurrences": 1
                        })
        
        return {
            "task_types": task_types,
            "component_types": component_types,
            "mappings": mappings,
            "total_unique_components": len(set(list(task_types.keys()) + list(component_types.keys())))
        }
    
    def _analyze_complexity(self, ir_package: IRPackage) -> Dict[str, Any]:
        """Analyze package complexity."""
        complexity_score = 0
        factors = []
        
        # Task count factor
        task_count = len(ir_package.executables)
        if task_count > 20:
            complexity_score += 3
            factors.append("High task count (>20)")
        elif task_count > 10:
            complexity_score += 2
            factors.append("Medium task count (10-20)")
        else:
            complexity_score += 1
            factors.append("Low task count (<10)")
        
        # Data flow complexity
        data_flows = ir_package.get_data_flows()
        total_components = sum(len(df.components) for df in data_flows)
        if total_components > 50:
            complexity_score += 3
            factors.append("High data flow complexity (>50 components)")
        elif total_components > 20:
            complexity_score += 2
            factors.append("Medium data flow complexity (20-50 components)")
        
        # Precedence constraint complexity
        if len(ir_package.edges) > 15:
            complexity_score += 2
            factors.append("Complex control flow (>15 constraints)")
        
        # Expression complexity
        complex_expressions = sum(1 for expr in ir_package.expressions if len(expr.expression) > 100)
        if complex_expressions > 5:
            complexity_score += 2
            factors.append(f"Multiple complex expressions ({complex_expressions})")
        
        # Connection complexity
        if len(ir_package.connection_managers) > 5:
            complexity_score += 1
            factors.append("Multiple connections")
        
        # Determine overall complexity
        if complexity_score >= 8:
            overall_complexity = "High"
        elif complexity_score >= 5:
            overall_complexity = "Medium"
        else:
            overall_complexity = "Low"
        
        return {
            "overall_complexity": overall_complexity,
            "complexity_score": complexity_score,
            "contributing_factors": factors,
            "metrics": {
                "total_tasks": task_count,
                "total_dataflow_components": total_components,
                "total_constraints": len(ir_package.edges),
                "complex_expressions": complex_expressions,
                "connection_count": len(ir_package.connection_managers)
            }
        }
    
    def _generate_recommendations(self, ir_package: IRPackage) -> List[Dict[str, str]]:
        """Generate migration recommendations."""
        recommendations = []
        
        # Recommend migration approach
        if ir_package.is_transformation_only():
            recommendations.append({
                "category": "Migration Strategy",
                "recommendation": "Use dbt for transformation-only package",
                "priority": "High",
                "rationale": "Package contains only data transformations, ideal for dbt"
            })
        else:
            recommendations.append({
                "category": "Migration Strategy", 
                "recommendation": "Use Airflow for orchestration with dbt for transformations",
                "priority": "High",
                "rationale": "Package contains both ingestion and transformation logic"
            })
        
        # Security recommendations
        if ir_package.protection_level.value != "DontSaveSensitive":
            recommendations.append({
                "category": "Security",
                "recommendation": "Review encrypted properties and connection strings",
                "priority": "High", 
                "rationale": "Package uses encryption - verify all sensitive data is handled properly"
            })
        
        # Data flow recommendations
        data_flows = ir_package.get_data_flows()
        if len(data_flows) > 5:
            recommendations.append({
                "category": "Architecture",
                "recommendation": "Consider breaking large data flows into smaller, focused models",
                "priority": "Medium",
                "rationale": "Large data flows can be difficult to maintain and debug"
            })
        
        # Expression recommendations
        complex_expressions = [expr for expr in ir_package.expressions if len(expr.expression) > 50]
        if complex_expressions:
            recommendations.append({
                "category": "Code Quality",
                "recommendation": "Review and simplify complex expressions",
                "priority": "Medium", 
                "rationale": f"Found {len(complex_expressions)} complex expressions that may need refactoring"
            })
        
        return recommendations
    
    def _identify_manual_review_items(self, ir_package: IRPackage) -> List[Dict[str, str]]:
        """Identify items requiring manual review."""
        review_items = []
        
        # Script tasks always need review
        script_tasks = [exe for exe in ir_package.executables if exe.type.value == "ScriptTask"]
        for task in script_tasks:
            review_items.append({
                "item": f"Script Task: {task.object_name}",
                "reason": "Script task code needs manual conversion to Python",
                "priority": "High"
            })
        
        # Complex expressions
        for expr in ir_package.expressions:
            if len(expr.expression) > 100 or "SCRIPT" in expr.expression.upper():
                review_items.append({
                    "item": f"Expression in {expr.scope}.{expr.property}",
                    "reason": "Complex expression requires manual review",
                    "priority": "Medium"
                })
        
        # Encrypted connections
        encrypted_conns = [cm for cm in ir_package.connection_managers if cm.sensitive]
        for conn in encrypted_conns:
            review_items.append({
                "item": f"Connection: {conn.name}",
                "reason": "Connection contains encrypted/sensitive properties",
                "priority": "High"
            })
        
        return review_items
    
    def _assess_migration_risks(self, ir_package: IRPackage) -> Dict[str, Any]:
        """Assess migration risks."""
        risks = []
        risk_score = 0
        
        # High-risk components
        high_risk_components = ["ScriptTask", "ScriptComponent", "WebService"]
        for exe in ir_package.executables:
            if exe.type.value in high_risk_components:
                risks.append({
                    "risk": f"High-risk component: {exe.type.value}",
                    "impact": "High",
                    "mitigation": "Manual conversion and thorough testing required"
                })
                risk_score += 3
        
        # Encryption risks
        if ir_package.protection_level.value != "DontSaveSensitive":
            risks.append({
                "risk": "Encrypted package properties",
                "impact": "Medium",
                "mitigation": "Verify all sensitive data is properly migrated"
            })
            risk_score += 2
        
        # Complexity risks
        if len(ir_package.executables) > 20:
            risks.append({
                "risk": "Large package size",
                "impact": "Medium", 
                "mitigation": "Consider breaking into smaller, focused packages"
            })
            risk_score += 1
        
        # Determine overall risk level
        if risk_score >= 6:
            risk_level = "High"
        elif risk_score >= 3:
            risk_level = "Medium"
        else:
            risk_level = "Low"
        
        return {
            "overall_risk": risk_level,
            "risk_score": risk_score,
            "identified_risks": risks
        }
    
    def _generate_next_steps(self, ir_package: IRPackage, 
                           migration_results: Dict[str, Any]) -> List[str]:
        """Generate next steps for migration."""
        steps = []
        
        # Generated artifacts steps
        if migration_results.get("airflow_dag"):
            steps.extend([
                "Set up Airflow connections using the generated connection script",
                "Deploy the generated DAG to your Airflow environment",
                "Test the DAG in a development environment"
            ])
        
        if migration_results.get("dbt_project"):
            steps.extend([
                "Configure dbt profiles.yml with your Snowflake credentials",
                "Run 'dbt compile' to validate the generated models",
                "Run 'dbt test' to execute data quality tests"
            ])
        
        # Manual review steps
        manual_items = self._identify_manual_review_items(ir_package)
        if manual_items:
            steps.append(f"Review and address {len(manual_items)} manual review items")
        
        # Testing steps
        steps.extend([
            "Perform data validation between source and target",
            "Execute end-to-end testing with representative data",
            "Document any differences in behavior or results"
        ])
        
        # Production steps
        steps.extend([
            "Plan production deployment strategy",
            "Set up monitoring and alerting",
            "Create runbooks for operational support"
        ])
        
        return steps
    
    def export_to_csv(self, report_data: Dict[str, Any], output_file: str):
        """Export report data to CSV format."""
        csv_data = []
        
        # Package info
        package_info = report_data.get("package_info", {})
        csv_data.append(["Section", "Metric", "Value"])
        csv_data.append(["Package", "Name", package_info.get("name", "")])
        csv_data.append(["Package", "Total Tasks", package_info.get("total_tasks", 0)])
        csv_data.append(["Package", "Total Connections", package_info.get("total_connections", 0)])
        csv_data.append(["Package", "Transformation Only", package_info.get("is_transformation_only", False)])
        
        # Component analysis
        component_analysis = report_data.get("component_analysis", {})
        task_types = component_analysis.get("task_types", {})
        for task_type, count in task_types.items():
            csv_data.append(["Task Types", task_type, count])
        
        # Complexity
        complexity = report_data.get("complexity_analysis", {})
        csv_data.append(["Complexity", "Overall", complexity.get("overall_complexity", "")])
        csv_data.append(["Complexity", "Score", complexity.get("complexity_score", 0)])
        
        # Write CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(csv_data)
    
    def export_to_html(self, report_data: Dict[str, Any], output_file: str):
        """Export report to HTML format."""
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>SSIS Migration Report - {report_data.get('package_info', {}).get('name', 'Unknown')}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .section {{ margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
        .high-priority {{ background-color: #ffe6e6; }}
        .medium-priority {{ background-color: #fff9e6; }}
        .low-priority {{ background-color: #e6ffe6; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .metric {{ display: inline-block; margin: 10px; padding: 10px; background-color: #f8f8f8; border-radius: 3px; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>SSIS Migration Report</h1>
        <h2>Package: {report_data.get('package_info', {}).get('name', 'Unknown')}</h2>
        <p>Generated: {report_data.get('generated_at', 'Unknown')}</p>
    </div>
    
    <div class="section">
        <h3>Package Overview</h3>
        <div class="metric">Tasks: {report_data.get('package_info', {}).get('total_tasks', 0)}</div>
        <div class="metric">Connections: {report_data.get('package_info', {}).get('total_connections', 0)}</div>
        <div class="metric">Complexity: {report_data.get('complexity_analysis', {}).get('overall_complexity', 'Unknown')}</div>
        <div class="metric">Risk Level: {report_data.get('risk_assessment', {}).get('overall_risk', 'Unknown')}</div>
    </div>
    
    <div class="section">
        <h3>Manual Review Items</h3>
        <table>
            <tr><th>Item</th><th>Reason</th><th>Priority</th></tr>
"""
        
        # Add manual review items
        for item in report_data.get('manual_review_items', []):
            priority_class = f"{item.get('priority', 'low').lower()}-priority"
            html_content += f"""
            <tr class="{priority_class}">
                <td>{item.get('item', '')}</td>
                <td>{item.get('reason', '')}</td>
                <td>{item.get('priority', '')}</td>
            </tr>"""
        
        html_content += """
        </table>
    </div>
    
    <div class="section">
        <h3>Recommendations</h3>
        <ul>
"""
        
        # Add recommendations
        for rec in report_data.get('migration_recommendations', []):
            html_content += f"<li><strong>{rec.get('category', '')}:</strong> {rec.get('recommendation', '')}</li>"
        
        html_content += """
        </ul>
    </div>
    
    <div class="section">
        <h3>Next Steps</h3>
        <ol>
"""
        
        # Add next steps
        for step in report_data.get('next_steps', []):
            html_content += f"<li>{step}</li>"
        
        html_content += """
        </ol>
    </div>
    
</body>
</html>"""
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)