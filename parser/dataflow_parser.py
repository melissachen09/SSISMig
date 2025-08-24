"""Data flow pipeline component parser.

Handles parsing SSIS Data Flow tasks and their constituent components
according to section 8.3 of the migration plan.
"""
import logging
from typing import Dict, List, Optional, Set
from lxml import etree
from models.ir import DataFlowComponent, ComponentType, Executable

logger = logging.getLogger(__name__)


class DataFlowParser:
    """Parser for SSIS Data Flow pipeline components."""
    
    def __init__(self, namespaces: Dict[str, str]):
        """Initialize parser with XML namespaces.
        
        Args:
            namespaces: XML namespace mappings
        """
        self.namespaces = namespaces
        self.component_id_map: Dict[str, str] = {}
        self.path_connections: List[Dict[str, str]] = []
    
    def parse_dataflow(self, obj_data: etree._Element, executable: Executable) -> None:
        """Parse data flow task from ObjectData element.
        
        Args:
            obj_data: ObjectData XML element
            executable: Executable to populate with components
        """
        pipeline = obj_data.find("Pipeline:Pipeline", namespaces=self.namespaces)
        
        if pipeline is None:
            logger.warning(f"No pipeline found in data flow task: {executable.object_name}")
            return
        
        logger.info(f"Parsing data flow: {executable.object_name}")
        
        # Parse components first
        components = self._parse_components(pipeline)
        
        # Parse paths (connections between components)
        self._parse_paths(pipeline)
        
        # Wire up component inputs/outputs based on paths
        self._wire_components(components)
        
        executable.components = components
        logger.info(f"Parsed {len(components)} components in data flow")
    
    def _parse_components(self, pipeline: etree._Element) -> List[DataFlowComponent]:
        """Parse individual pipeline components.
        
        Args:
            pipeline: Pipeline XML element
            
        Returns:
            List of parsed DataFlowComponent objects
        """
        components = []
        comp_elements = pipeline.xpath(".//Pipeline:component", namespaces=self.namespaces)
        
        for comp_elem in comp_elements:
            component = self._parse_single_component(comp_elem)
            if component:
                components.append(component)
                
        return components
    
    def _parse_single_component(self, comp_elem: etree._Element) -> Optional[DataFlowComponent]:
        """Parse a single pipeline component.
        
        Args:
            comp_elem: Component XML element
            
        Returns:
            DataFlowComponent object or None if unsupported
        """
        comp_id = comp_elem.get("id")
        comp_name = comp_elem.get("name") 
        class_id = comp_elem.get("componentClassID")
        
        if not comp_id or not comp_name or not class_id:
            logger.warning("Component missing required attributes")
            return None
        
        # Map class ID to component type
        comp_type = self._map_component_type(class_id)
        if not comp_type:
            logger.warning(f"Unsupported component type: {class_id}")
            return None
        
        component = DataFlowComponent(
            id=comp_id,
            component_type=comp_type,
            name=comp_name
        )
        
        # Store ID mapping for path resolution
        self.component_id_map[comp_id] = comp_name
        
        # Extract component-specific properties
        self._extract_component_properties(comp_elem, component)
        
        return component
    
    def _map_component_type(self, class_id: str) -> Optional[ComponentType]:
        """Map SSIS component class ID to ComponentType enum.
        
        Args:
            class_id: SSIS component class identifier
            
        Returns:
            ComponentType enum value or None if unsupported
        """
        mapping = {
            # Sources
            "Microsoft.OLEDBSource": ComponentType.OLEDB_SOURCE,
            "Microsoft.ADONETSource": ComponentType.ADONET_SOURCE, 
            "Microsoft.FlatFileSource": ComponentType.FLAT_FILE_SOURCE,
            "Microsoft.ScriptSource": ComponentType.SCRIPT_SOURCE,
            
            # Destinations  
            "Microsoft.OLEDBDestination": ComponentType.OLEDB_DESTINATION,
            "Microsoft.ADONETDestination": ComponentType.ADONET_DESTINATION,
            "Microsoft.FlatFileDestination": ComponentType.FLAT_FILE_DESTINATION,
            
            # Transformations
            "Microsoft.DerivedColumn": ComponentType.DERIVED_COLUMN,
            "Microsoft.Lookup": ComponentType.LOOKUP,
            "Microsoft.ConditionalSplit": ComponentType.CONDITIONAL_SPLIT,
            "Microsoft.UnionAll": ComponentType.UNION_ALL,
            "Microsoft.Sort": ComponentType.SORT,
            "Microsoft.Aggregate": ComponentType.AGGREGATE,
            "Microsoft.MergeJoin": ComponentType.MERGE_JOIN,
            "Microsoft.Multicast": ComponentType.MULTICAST,
            "Microsoft.RowCount": ComponentType.ROW_COUNT,
            "Microsoft.ScriptComponent": ComponentType.SCRIPT_COMPONENT,
            
            # Abbreviated forms
            "DTSTransform.OLEDBSource": ComponentType.OLEDB_SOURCE,
            "DTSTransform.OLEDBDestination": ComponentType.OLEDB_DESTINATION,
            "DTSTransform.DerivedColumn": ComponentType.DERIVED_COLUMN,
            "DTSTransform.Lookup": ComponentType.LOOKUP,
        }
        
        return mapping.get(class_id)
    
    def _extract_component_properties(self, comp_elem: etree._Element, component: DataFlowComponent) -> None:
        """Extract component-specific properties.
        
        Args:
            comp_elem: Component XML element
            component: DataFlowComponent to populate
        """
        # Get component properties
        props = comp_elem.xpath(".//Pipeline:properties/Pipeline:property", namespaces=self.namespaces)
        
        for prop in props:
            prop_name = prop.get("name")
            prop_value = prop.text
            
            if not prop_name or not prop_value:
                continue
                
            # Handle specific properties based on component type
            if component.component_type in {ComponentType.OLEDB_SOURCE, ComponentType.ADONET_SOURCE}:
                self._extract_source_properties(prop_name, prop_value, component)
            elif component.component_type in {ComponentType.OLEDB_DESTINATION, ComponentType.ADONET_DESTINATION}:
                self._extract_destination_properties(prop_name, prop_value, component)
            elif component.component_type == ComponentType.DERIVED_COLUMN:
                self._extract_derived_column_properties(prop_name, prop_value, component)
            elif component.component_type == ComponentType.LOOKUP:
                self._extract_lookup_properties(prop_name, prop_value, component)
            
            # Store all properties for reference
            component.properties[prop_name] = prop_value
    
    def _extract_source_properties(self, prop_name: str, prop_value: str, component: DataFlowComponent) -> None:
        """Extract source component properties."""
        if prop_name == "SqlCommand":
            component.sql = prop_value.strip()
        elif prop_name == "TableOrViewName":
            component.table = prop_value.strip()
        elif prop_name == "AccessMode":
            component.properties["access_mode"] = prop_value
    
    def _extract_destination_properties(self, prop_name: str, prop_value: str, component: DataFlowComponent) -> None:
        """Extract destination component properties."""
        if prop_name == "TableOrViewName":
            component.table = prop_value.strip()
        elif prop_name == "FastLoadOptions":
            component.properties["load_options"] = prop_value
        elif prop_name == "FastLoadKeepIdentity":
            component.properties["keep_identity"] = prop_value.lower() == "true"
        elif prop_name == "FastLoadKeepNulls":
            component.properties["keep_nulls"] = prop_value.lower() == "true"
        elif prop_name == "AccessMode":
            # Map access mode to our mode field
            if "fastload" in prop_value.lower():
                component.mode = "append"
            else:
                component.mode = "insert"
    
    def _extract_derived_column_properties(self, prop_name: str, prop_value: str, component: DataFlowComponent) -> None:
        """Extract derived column properties."""
        if prop_name == "Expression":
            component.expression = prop_value.strip()
        elif prop_name == "FriendlyExpression":
            component.properties["friendly_expression"] = prop_value
    
    def _extract_lookup_properties(self, prop_name: str, prop_value: str, component: DataFlowComponent) -> None:
        """Extract lookup component properties."""
        if prop_name == "SqlCommand":
            component.sql = prop_value.strip()
        elif prop_name == "SqlCommandParam":
            component.properties["sql_params"] = prop_value
        elif prop_name == "JoinKeys":
            # Parse join keys (format varies)
            component.join_on = [k.strip() for k in prop_value.split(",") if k.strip()]
    
    def _parse_paths(self, pipeline: etree._Element) -> None:
        """Parse data flow paths (connections between components).
        
        Args:
            pipeline: Pipeline XML element
        """
        path_elements = pipeline.xpath(".//Pipeline:path", namespaces=self.namespaces)
        
        for path_elem in path_elements:
            start_id = path_elem.get("startId")
            end_id = path_elem.get("endId")
            
            if start_id and end_id:
                self.path_connections.append({
                    "start": start_id,
                    "end": end_id,
                    "path_id": path_elem.get("id", "")
                })
    
    def _wire_components(self, components: List[DataFlowComponent]) -> None:
        """Wire up component inputs and outputs based on parsed paths.
        
        Args:
            components: List of components to wire
        """
        # Create component lookup by ID
        comp_by_id = {comp.id: comp for comp in components}
        
        # Process path connections
        for path in self.path_connections:
            start_comp = comp_by_id.get(path["start"])
            end_comp = comp_by_id.get(path["end"])
            
            if start_comp and end_comp:
                # Add output to start component
                if path["path_id"] not in start_comp.outputs:
                    start_comp.outputs.append(path["path_id"])
                
                # Add input to end component  
                if path["path_id"] not in end_comp.inputs:
                    end_comp.inputs.append(path["path_id"])
        
        # Log wiring summary
        for comp in components:
            if comp.inputs or comp.outputs:
                logger.debug(f"Component {comp.name}: {len(comp.inputs)} inputs, {len(comp.outputs)} outputs")
    
    def get_source_components(self, components: List[DataFlowComponent]) -> List[DataFlowComponent]:
        """Get all source components from the list.
        
        Args:
            components: List of components to filter
            
        Returns:
            List of source components
        """
        source_types = {
            ComponentType.OLEDB_SOURCE,
            ComponentType.ADONET_SOURCE,
            ComponentType.FLAT_FILE_SOURCE,
            ComponentType.SCRIPT_SOURCE,
        }
        return [comp for comp in components if comp.component_type in source_types]
    
    def get_destination_components(self, components: List[DataFlowComponent]) -> List[DataFlowComponent]:
        """Get all destination components from the list.
        
        Args:
            components: List of components to filter
            
        Returns:
            List of destination components
        """
        dest_types = {
            ComponentType.OLEDB_DESTINATION,
            ComponentType.ADONET_DESTINATION,
            ComponentType.FLAT_FILE_DESTINATION,
            ComponentType.SNOWFLAKE_DEST,
        }
        return [comp for comp in components if comp.component_type in dest_types]
    
    def get_transformation_components(self, components: List[DataFlowComponent]) -> List[DataFlowComponent]:
        """Get all transformation components from the list.
        
        Args:
            components: List of components to filter
            
        Returns:
            List of transformation components
        """
        transform_types = {
            ComponentType.DERIVED_COLUMN,
            ComponentType.LOOKUP,
            ComponentType.CONDITIONAL_SPLIT,
            ComponentType.UNION_ALL,
            ComponentType.SORT,
            ComponentType.AGGREGATE,
            ComponentType.MERGE_JOIN,
            ComponentType.MULTICAST,
            ComponentType.ROW_COUNT,
            ComponentType.SCRIPT_COMPONENT,
        }
        return [comp for comp in components if comp.component_type in transform_types]