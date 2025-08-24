"""SSIS expression parser and converter.

Handles parsing and converting SSIS expressions to equivalent formats
for Airflow and dbt according to section 8 of the migration plan.
"""
import logging
import re
from typing import Dict, List, Optional, Tuple, Any
from lxml import etree
from models.ir import Expression

logger = logging.getLogger(__name__)


class SSISExpressionParser:
    """Parser for SSIS expressions and property overrides."""
    
    def __init__(self, namespaces: Dict[str, str]):
        """Initialize parser with XML namespaces.
        
        Args:
            namespaces: XML namespace mappings
        """
        self.namespaces = namespaces
        self.variable_refs = {}
        self.parameter_refs = {}
    
    def parse_expressions(self, root: etree._Element) -> List[Expression]:
        """Parse all property expressions from package root.
        
        Args:
            root: Package root XML element
            
        Returns:
            List of Expression objects
        """
        expressions = []
        
        # Find all property expression elements
        expr_elements = root.xpath(".//DTS:PropertyExpression", namespaces=self.namespaces)
        
        logger.info(f"Found {len(expr_elements)} property expressions")
        
        for expr_elem in expr_elements:
            expression = self._parse_single_expression(expr_elem)
            if expression:
                expressions.append(expression)
        
        # Also look for expressions in specific task contexts
        expressions.extend(self._parse_task_expressions(root))
        
        return expressions
    
    def _parse_single_expression(self, expr_elem: etree._Element) -> Optional[Expression]:
        """Parse a single property expression element.
        
        Args:
            expr_elem: PropertyExpression XML element
            
        Returns:
            Expression object or None if parsing fails
        """
        property_name = expr_elem.get(f"{{{self.namespaces['DTS']}}}Name")
        expression_text = expr_elem.text
        
        if not property_name or not expression_text:
            return None
        
        # Find the scope (parent executable)
        scope = self._find_expression_scope(expr_elem)
        
        return Expression(
            scope=scope or "Package",
            property=property_name,
            expression=expression_text.strip()
        )
    
    def _parse_task_expressions(self, root: etree._Element) -> List[Expression]:
        """Parse expressions from specific task contexts.
        
        Args:
            root: Package root XML element
            
        Returns:
            List of Expression objects
        """
        expressions = []
        
        # Find expressions in ExecuteSQL tasks
        sql_tasks = root.xpath(".//SQLTask:SqlTaskData", namespaces=self.namespaces)
        for sql_task in sql_tasks:
            scope = self._find_task_scope(sql_task)
            
            # Check for SQL statement expressions
            sql_expr = sql_task.get("SqlStatementSourceType")
            if sql_expr == "Variable" or sql_expr == "DirectInput":
                sql_stmt = sql_task.get("SqlStatementSource")
                if sql_stmt and self._contains_expression(sql_stmt):
                    expressions.append(Expression(
                        scope=scope or "Unknown",
                        property="SqlStatementSource", 
                        expression=sql_stmt
                    ))
        
        return expressions
    
    def _find_expression_scope(self, expr_elem: etree._Element) -> Optional[str]:
        """Find the scope (parent task) of an expression element.
        
        Args:
            expr_elem: Expression XML element
            
        Returns:
            Scope string or None
        """
        # Walk up the XML tree to find the parent executable
        parent = expr_elem.getparent()
        
        while parent is not None:
            if parent.tag == f"{{{self.namespaces['DTS']}}}Executable":
                ref_id = parent.get(f"{{{self.namespaces['DTS']}}}refId")
                if ref_id:
                    return ref_id
            parent = parent.getparent()
        
        return None
    
    def _find_task_scope(self, task_elem: etree._Element) -> Optional[str]:
        """Find the scope (task ID) for a task element.
        
        Args:
            task_elem: Task XML element
            
        Returns:
            Task scope string or None
        """
        # Look for parent ObjectData and then Executable
        parent = task_elem.getparent()
        
        while parent is not None:
            if parent.tag == f"{{{self.namespaces['DTS']}}}Executable":
                ref_id = parent.get(f"{{{self.namespaces['DTS']}}}refId")
                if ref_id:
                    return ref_id
            parent = parent.getparent()
        
        return None
    
    def _contains_expression(self, text: str) -> bool:
        """Check if text contains SSIS expression syntax.
        
        Args:
            text: Text to check
            
        Returns:
            True if contains expressions, False otherwise
        """
        # SSIS expressions use @[scope::variable] syntax
        return bool(re.search(r'@\[.*?\]', text))
    
    def extract_variable_references(self, expressions: List[Expression]) -> List[str]:
        """Extract all variable references from expressions.
        
        Args:
            expressions: List of Expression objects
            
        Returns:
            List of unique variable references
        """
        variables = set()
        
        for expr in expressions:
            # Find variable references: @[User::variable] or @[System::variable]
            var_matches = re.findall(r'@\[(User|System)::(.*?)\]', expr.expression)
            for scope, var_name in var_matches:
                variables.add(f"{scope}::{var_name}")
        
        return sorted(list(variables))
    
    def extract_parameter_references(self, expressions: List[Expression]) -> List[str]:
        """Extract all parameter references from expressions.
        
        Args:
            expressions: List of Expression objects
            
        Returns:
            List of unique parameter references
        """
        parameters = set()
        
        for expr in expressions:
            # Find parameter references: @[$Package::parameter]
            param_matches = re.findall(r'@\[\$Package::(.*?)\]', expr.expression)
            for param_name in param_matches:
                parameters.add(param_name)
        
        return sorted(list(parameters))
    
    def convert_to_airflow_template(self, expression: str, variable_map: Dict[str, str] = None) -> str:
        """Convert SSIS expression to Airflow template syntax.
        
        Args:
            expression: SSIS expression string
            variable_map: Optional mapping of SSIS variables to Airflow variables
            
        Returns:
            Airflow template string
        """
        if not variable_map:
            variable_map = {}
        
        result = expression
        
        # Convert variable references
        def replace_variable(match):
            scope = match.group(1) 
            var_name = match.group(2)
            full_var = f"{scope}::{var_name}"
            
            # Map to Airflow equivalent
            if full_var in variable_map:
                return f"{{{{ var.value.{variable_map[full_var]} }}}}"
            else:
                # Default mapping - use dag_run.conf for User variables
                if scope == "User":
                    return f"{{{{ dag_run.conf.get('{var_name}', '') }}}}"
                else:
                    return f"{{{{ var.value.{var_name.lower()} }}}}"
        
        # Replace variable references
        result = re.sub(r'@\[(User|System)::(.*?)\]', replace_variable, result)
        
        # Convert parameter references
        def replace_parameter(match):
            param_name = match.group(1)
            return f"{{{{ dag_run.conf.get('{param_name}', '') }}}}"
        
        result = re.sub(r'@\[\$Package::(.*?)\]', replace_parameter, result)
        
        return result
    
    def convert_to_dbt_jinja(self, expression: str, variable_map: Dict[str, str] = None) -> str:
        """Convert SSIS expression to dbt Jinja syntax.
        
        Args:
            expression: SSIS expression string
            variable_map: Optional mapping of SSIS variables to dbt variables
            
        Returns:
            dbt Jinja template string
        """
        if not variable_map:
            variable_map = {}
        
        result = expression
        
        # Convert variable references to dbt vars
        def replace_variable(match):
            scope = match.group(1)
            var_name = match.group(2) 
            full_var = f"{scope}::{var_name}"
            
            if full_var in variable_map:
                return f"{{{{ var('{variable_map[full_var]}') }}}}"
            else:
                return f"{{{{ var('{var_name.lower()}') }}}}"
        
        result = re.sub(r'@\[(User|System)::(.*?)\]', replace_variable, result)
        
        # Convert parameter references
        def replace_parameter(match):
            param_name = match.group(1)
            return f"{{{{ var('{param_name.lower()}') }}}}"
        
        result = re.sub(r'@\[\$Package::(.*?)\]', replace_parameter, result)
        
        return result
    
    def analyze_expression_complexity(self, expression: str) -> Dict[str, Any]:
        """Analyze expression complexity and required conversion.
        
        Args:
            expression: SSIS expression string
            
        Returns:
            Dictionary with analysis results
        """
        analysis = {
            "variable_count": len(re.findall(r'@\[(User|System)::(.*?)\]', expression)),
            "parameter_count": len(re.findall(r'@\[\$Package::(.*?)\]', expression)),
            "has_functions": False,
            "has_operators": False,
            "complexity": "simple",
            "conversion_notes": []
        }
        
        # Check for SSIS functions
        ssis_functions = [
            'DATEADD', 'DATEDIFF', 'DATEPART', 'GETDATE', 'YEAR', 'MONTH', 'DAY',
            'LEN', 'SUBSTRING', 'UPPER', 'LOWER', 'LTRIM', 'RTRIM', 'REPLACE',
            'ISNULL', 'FINDSTRING', 'TOKEN', 'TOKENCOUNT'
        ]
        
        for func in ssis_functions:
            if func in expression.upper():
                analysis["has_functions"] = True
                analysis["conversion_notes"].append(f"Contains SSIS function: {func}")
                break
        
        # Check for operators
        operators = ['&&', '||', '==', '!=', '>=', '<=']
        for op in operators:
            if op in expression:
                analysis["has_operators"] = True
                break
        
        # Determine complexity
        if analysis["variable_count"] > 3 or analysis["parameter_count"] > 3:
            analysis["complexity"] = "high"
        elif analysis["has_functions"] or analysis["has_operators"]:
            analysis["complexity"] = "medium"
        
        return analysis
    
    def validate_expression_syntax(self, expression: str) -> List[str]:
        """Validate SSIS expression syntax.
        
        Args:
            expression: SSIS expression string
            
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check for unmatched brackets
        bracket_count = expression.count('[') - expression.count(']')
        if bracket_count != 0:
            errors.append("Unmatched brackets in expression")
        
        # Check for incomplete variable references
        if '@[' in expression and not re.search(r'@\[[^]]+\]', expression):
            errors.append("Incomplete variable reference syntax")
        
        # Check for common syntax issues
        if expression.count('"') % 2 != 0:
            errors.append("Unmatched quotes in expression")
        
        return errors