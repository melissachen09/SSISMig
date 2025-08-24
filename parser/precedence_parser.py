"""Precedence constraint parser for SSIS control flow dependencies.

Handles parsing precedence constraints that define task execution order
according to section 8.4 of the migration plan.
"""
import logging
from typing import Dict, List, Optional
from lxml import etree
from models.ir import PrecedenceEdge, PrecedenceCondition

logger = logging.getLogger(__name__)


class PrecedenceParser:
    """Parser for SSIS precedence constraints."""
    
    def __init__(self, namespaces: Dict[str, str]):
        """Initialize parser with XML namespaces.
        
        Args:
            namespaces: XML namespace mappings
        """
        self.namespaces = namespaces
    
    def parse_precedence_constraints(self, root: etree._Element) -> List[PrecedenceEdge]:
        """Parse all precedence constraints from package root.
        
        Args:
            root: Package root XML element
            
        Returns:
            List of PrecedenceEdge objects
        """
        edges = []
        
        # Find all precedence constraint elements
        constraints = root.xpath(".//DTS:PrecedenceConstraint", namespaces=self.namespaces)
        
        logger.info(f"Found {len(constraints)} precedence constraints")
        
        for constraint in constraints:
            edge = self._parse_single_constraint(constraint)
            if edge:
                edges.append(edge)
        
        return edges
    
    def _parse_single_constraint(self, constraint: etree._Element) -> Optional[PrecedenceEdge]:
        """Parse a single precedence constraint.
        
        Args:
            constraint: PrecedenceConstraint XML element
            
        Returns:
            PrecedenceEdge object or None if parsing fails
        """
        # Extract basic constraint properties
        from_task = constraint.get(f"{{{self.namespaces['DTS']}}}From")
        to_task = constraint.get(f"{{{self.namespaces['DTS']}}}To") 
        value = constraint.get(f"{{{self.namespaces['DTS']}}}Value")
        logical_and = constraint.get(f"{{{self.namespaces['DTS']}}}LogicalAnd", "True").lower() == "true"
        
        if not from_task or not to_task:
            logger.warning("Precedence constraint missing From or To task reference")
            return None
        
        # Map constraint value to condition enum
        condition = self._map_precedence_condition(value)
        if not condition:
            logger.warning(f"Unknown precedence condition: {value}")
            return None
        
        # Check for expression-based constraints
        expression = None
        if condition == PrecedenceCondition.EXPRESSION:
            expression = self._extract_expression(constraint)
        
        edge = PrecedenceEdge(
            from_task=from_task,
            to_task=to_task,
            condition=condition,
            expression=expression,
            logical_and=logical_and
        )
        
        logger.debug(f"Parsed precedence: {from_task} -> {to_task} ({condition.value})")
        return edge
    
    def _map_precedence_condition(self, value: Optional[str]) -> Optional[PrecedenceCondition]:
        """Map SSIS precedence value to PrecedenceCondition enum.
        
        Args:
            value: SSIS precedence constraint value
            
        Returns:
            PrecedenceCondition enum value or None
        """
        if not value:
            # Default to Success if no value specified
            return PrecedenceCondition.SUCCESS
        
        mapping = {
            "Success": PrecedenceCondition.SUCCESS,
            "Failure": PrecedenceCondition.FAILURE, 
            "Completion": PrecedenceCondition.COMPLETION,
            "Expression": PrecedenceCondition.EXPRESSION,
            
            # Alternative formats
            "0": PrecedenceCondition.SUCCESS,
            "1": PrecedenceCondition.FAILURE,
            "2": PrecedenceCondition.COMPLETION,
            "3": PrecedenceCondition.EXPRESSION,
        }
        
        return mapping.get(value)
    
    def _extract_expression(self, constraint: etree._Element) -> Optional[str]:
        """Extract expression from expression-based precedence constraint.
        
        Args:
            constraint: PrecedenceConstraint XML element
            
        Returns:
            Expression string or None
        """
        # Look for expression in various possible locations
        
        # Check Expression property
        expr_prop = constraint.find(".//DTS:Property[@DTS:Name='Expression']", 
                                   namespaces=self.namespaces)
        if expr_prop is not None and expr_prop.text:
            return expr_prop.text.strip()
        
        # Check nested expression elements  
        expr_elem = constraint.find(".//DTS:Expression", namespaces=self.namespaces)
        if expr_elem is not None and expr_elem.text:
            return expr_elem.text.strip()
        
        # Check in ObjectData
        obj_data = constraint.find("DTS:ObjectData", namespaces=self.namespaces)
        if obj_data is not None:
            expr_text = obj_data.get("Expression")
            if expr_text:
                return expr_text.strip()
        
        logger.warning("Expression-based precedence constraint found but no expression extracted")
        return None
    
    def build_dependency_graph(self, edges: List[PrecedenceEdge]) -> Dict[str, List[str]]:
        """Build a dependency graph from precedence edges.
        
        Args:
            edges: List of precedence edges
            
        Returns:
            Dictionary mapping task ID to list of dependent task IDs
        """
        graph = {}
        
        for edge in edges:
            if edge.from_task not in graph:
                graph[edge.from_task] = []
            graph[edge.from_task].append(edge.to_task)
        
        return graph
    
    def get_root_tasks(self, edges: List[PrecedenceEdge], all_task_ids: List[str]) -> List[str]:
        """Get tasks with no incoming dependencies (root tasks).
        
        Args:
            edges: List of precedence edges
            all_task_ids: All task IDs in the package
            
        Returns:
            List of root task IDs
        """
        # Find tasks that are never the target of a precedence constraint
        target_tasks = {edge.to_task for edge in edges}
        root_tasks = [task_id for task_id in all_task_ids if task_id not in target_tasks]
        
        return root_tasks
    
    def get_leaf_tasks(self, edges: List[PrecedenceEdge], all_task_ids: List[str]) -> List[str]:
        """Get tasks with no outgoing dependencies (leaf tasks).
        
        Args:
            edges: List of precedence edges  
            all_task_ids: All task IDs in the package
            
        Returns:
            List of leaf task IDs
        """
        # Find tasks that are never the source of a precedence constraint
        source_tasks = {edge.from_task for edge in edges}
        leaf_tasks = [task_id for task_id in all_task_ids if task_id not in source_tasks]
        
        return leaf_tasks
    
    def validate_constraints(self, edges: List[PrecedenceEdge], all_task_ids: List[str]) -> List[str]:
        """Validate precedence constraints for common issues.
        
        Args:
            edges: List of precedence edges
            all_task_ids: All task IDs in the package
            
        Returns:
            List of validation warnings/errors
        """
        issues = []
        
        # Check for references to non-existent tasks
        task_id_set = set(all_task_ids)
        for edge in edges:
            if edge.from_task not in task_id_set:
                issues.append(f"Precedence constraint references unknown source task: {edge.from_task}")
            if edge.to_task not in task_id_set:
                issues.append(f"Precedence constraint references unknown target task: {edge.to_task}")
        
        # Check for potential cycles (basic detection)
        graph = self.build_dependency_graph(edges)
        visited = set()
        rec_stack = set()
        
        def has_cycle(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
            
            rec_stack.remove(node)
            return False
        
        for task_id in all_task_ids:
            if task_id not in visited:
                if has_cycle(task_id):
                    issues.append("Potential cycle detected in precedence constraints")
                    break
        
        # Check for isolated tasks (no constraints)
        constrained_tasks = set()
        for edge in edges:
            constrained_tasks.add(edge.from_task)
            constrained_tasks.add(edge.to_task)
        
        isolated_tasks = [task_id for task_id in all_task_ids if task_id not in constrained_tasks]
        if isolated_tasks:
            issues.append(f"Tasks with no precedence constraints: {', '.join(isolated_tasks)}")
        
        return issues
    
    def group_parallel_tasks(self, edges: List[PrecedenceEdge]) -> Dict[str, List[str]]:
        """Group tasks that can run in parallel.
        
        Args:
            edges: List of precedence edges
            
        Returns:
            Dictionary mapping parent task to list of parallel child tasks
        """
        parallel_groups = {}
        
        # Group by source task
        by_source = {}
        for edge in edges:
            if edge.from_task not in by_source:
                by_source[edge.from_task] = []
            by_source[edge.from_task].append(edge.to_task)
        
        # Find parallel groups (multiple tasks depending on same source)
        for source, targets in by_source.items():
            if len(targets) > 1:
                # Check if these tasks have the same condition (can run in parallel)
                source_edges = [e for e in edges if e.from_task == source]
                success_edges = [e for e in source_edges if e.condition == PrecedenceCondition.SUCCESS]
                
                if len(success_edges) > 1:
                    parallel_groups[source] = [e.to_task for e in success_edges]
        
        return parallel_groups