"""
Data models for Alteryx workflow parsing.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any
from enum import Enum


class ToolCategory(Enum):
    """Categories of Alteryx tools."""
    INPUT = "input"
    OUTPUT = "output"
    PREPARATION = "preparation"
    JOIN = "join"
    TRANSFORM = "transform"
    PARSE = "parse"
    REPORTING = "reporting"
    SPATIAL = "spatial"
    PREDICTIVE = "predictive"
    DEVELOPER = "developer"
    IN_DATABASE = "in_database"
    MACRO = "macro"
    CONTAINER = "container"  # Tool containers for organization
    UNKNOWN = "unknown"


class MedallionLayer(Enum):
    """Medallion architecture layers."""
    BRONZE = "bronze"  # Raw/staging
    SILVER = "silver"  # Intermediate/cleaned
    GOLD = "gold"      # Business-ready/marts


@dataclass
class AlteryxNode:
    """Represents a single tool/node in an Alteryx workflow."""
    tool_id: int
    tool_type: str                          # Full plugin name from GuiSettings
    plugin_name: str                        # Simplified tool name
    category: ToolCategory = ToolCategory.UNKNOWN
    configuration: Dict[str, Any] = field(default_factory=dict)
    position: tuple = (0, 0)                # x, y coordinates
    is_macro: bool = False
    macro_path: Optional[str] = None
    annotation: Optional[str] = None        # Tool annotation/comment

    # Container relationships
    container_id: Optional[int] = None      # ID of parent container (if inside one)
    child_tool_ids: List[int] = field(default_factory=list)  # For containers: IDs of child tools

    # Extracted details based on tool type
    source_path: Optional[str] = None       # For input tools
    target_path: Optional[str] = None       # For output tools
    connection_string: Optional[str] = None # Database connection
    table_name: Optional[str] = None        # Database table
    sql_query: Optional[str] = None         # SQL query if present
    expression: Optional[str] = None        # Formula/filter expression
    join_keys: List[str] = field(default_factory=list)
    join_type: Optional[str] = None
    group_by_fields: List[str] = field(default_factory=list)
    aggregations: List[Dict[str, str]] = field(default_factory=list)
    selected_fields: List[str] = field(default_factory=list)

    def get_display_name(self) -> str:
        """Get a human-readable name for the tool."""
        if self.annotation:
            return f"{self.plugin_name}: {self.annotation}"
        return self.plugin_name


@dataclass
class AlteryxConnection:
    """Represents a connection between two tools."""
    origin_id: int
    origin_anchor: str      # Output anchor name (e.g., "Output", "True", "False", "Left", "Right")
    destination_id: int
    destination_anchor: str # Input anchor name (e.g., "Input", "Left", "Right")

    def __repr__(self) -> str:
        return f"Connection({self.origin_id}:{self.origin_anchor} -> {self.destination_id}:{self.destination_anchor})"


@dataclass
class WorkflowMetadata:
    """Metadata about the workflow."""
    name: str
    file_path: str
    alteryx_version: Optional[str] = None
    description: Optional[str] = None
    author: Optional[str] = None
    created_date: Optional[str] = None
    modified_date: Optional[str] = None


@dataclass
class AlteryxWorkflow:
    """Complete parsed Alteryx workflow."""
    metadata: WorkflowMetadata
    nodes: List[AlteryxNode] = field(default_factory=list)
    connections: List[AlteryxConnection] = field(default_factory=list)

    # Derived data
    sources: List[AlteryxNode] = field(default_factory=list)
    targets: List[AlteryxNode] = field(default_factory=list)
    macros_used: List[str] = field(default_factory=list)
    missing_macros: List[str] = field(default_factory=list)

    def get_node_by_id(self, tool_id: int) -> Optional[AlteryxNode]:
        """Get a node by its tool ID."""
        for node in self.nodes:
            if node.tool_id == tool_id:
                return node
        return None

    def get_downstream_nodes(self, tool_id: int) -> List[AlteryxNode]:
        """Get all nodes that receive input from the given tool."""
        downstream_ids = [c.destination_id for c in self.connections if c.origin_id == tool_id]
        return [n for n in self.nodes if n.tool_id in downstream_ids]

    def get_upstream_nodes(self, tool_id: int) -> List[AlteryxNode]:
        """Get all nodes that provide input to the given tool."""
        upstream_ids = [c.origin_id for c in self.connections if c.destination_id == tool_id]
        return [n for n in self.nodes if n.tool_id in upstream_ids]

    def get_upstream_connections(self, tool_id: int) -> List[AlteryxConnection]:
        """Get all connections that feed into the given tool.

        Returns connections with full anchor information for tools like Join
        that need to distinguish Left vs Right inputs (HIGH-02 fix).
        """
        return [c for c in self.connections if c.destination_id == tool_id]

    def get_upstream_node_by_anchor(self, tool_id: int, anchor: str) -> Optional[AlteryxNode]:
        """Get the upstream node connected to a specific input anchor.

        Args:
            tool_id: The tool ID to find upstream for
            anchor: The destination anchor name (e.g., 'Left', 'Right', 'Input')

        Returns:
            The upstream node connected to that anchor, or None
        """
        for conn in self.connections:
            if conn.destination_id == tool_id and conn.destination_anchor.lower() == anchor.lower():
                return self.get_node_by_id(conn.origin_id)
        return None


@dataclass
class MacroInfo:
    """Information about a macro."""
    name: str
    file_path: str
    found: bool = False
    resolved_path: Optional[str] = None
    workflow: Optional[AlteryxWorkflow] = None  # Parsed macro workflow
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    description: Optional[str] = None


@dataclass
class TransformationStep:
    """A single transformation step in the workflow."""
    order: int
    tool_id: int
    tool_name: str
    category: ToolCategory
    description: str
    expression: Optional[str] = None
    medallion_layer: MedallionLayer = MedallionLayer.SILVER
    dbt_hint: Optional[str] = None  # SQL/DBT translation hint


@dataclass
class DataLineage:
    """Data lineage from source to target."""
    source: AlteryxNode
    target: AlteryxNode
    path: List[AlteryxNode]  # Ordered list of nodes from source to target
    transformations: List[TransformationStep] = field(default_factory=list)


@dataclass
class SourceInventory:
    """Inventory of all data sources across workflows."""
    name: str
    source_type: str  # file, database, api, etc.
    path_or_connection: str
    workflows_using: List[str] = field(default_factory=list)
    suggested_dbt_source: Optional[str] = None


@dataclass
class TargetInventory:
    """Inventory of all output targets across workflows."""
    name: str
    target_type: str
    path_or_connection: str
    workflows_using: List[str] = field(default_factory=list)
    suggested_dbt_model: Optional[str] = None


@dataclass
class DBTModel:
    """Represents a suggested DBT model."""
    name: str
    layer: MedallionLayer
    sql_template: str
    source_models: List[str] = field(default_factory=list)
    description: Optional[str] = None
    original_tools: List[int] = field(default_factory=list)  # Tool IDs this model represents
