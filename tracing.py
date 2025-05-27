"""
Path-Aware Workflow Tracing Module

This module provides the core tracing infrastructure for path-aware workflow nodes.
It includes the @traced_node decorator and utilities for extracting execution path
information from both state and OpenTelemetry span context.

Key Components:
- traced_node: Decorator for automatic node tracing with path awareness
- SpanPathExtractor: Utility class for path introspection from spans and state
- PathTrackingState: TypedDict for workflow state structure

Usage:
    from tracing import traced_node, SpanPathExtractor
    from opentelemetry import trace
    
    class MyNodes:
        def __init__(self, tracer):
            self.tracer = tracer
        
        @traced_node("my_node", "processor")
        async def my_node_method(self, state: PathTrackingState) -> PathTrackingState:
            # Your business logic here
            previous_node = SpanPathExtractor.get_previous_node_from_state(state)
            return state
"""

import functools
from typing import Callable, Dict, Any, List, Optional
from datetime import datetime
from typing_extensions import TypedDict

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode


class PathTrackingState(TypedDict):
    """
    State structure for path-aware workflow tracking.
    
    Attributes:
        data: Arbitrary workflow data dictionary
        execution_path: Ordered list of node names showing execution sequence
        routing_decisions: Dictionary storing routing decision metadata by node
        node_metadata: Dictionary storing arbitrary metadata by node
    """
    data: Dict[str, Any]
    execution_path: List[str]
    routing_decisions: Dict[str, Dict]
    node_metadata: Dict[str, Dict]


def traced_node(node_name: str, node_type: str = "processor"):
    """
    Decorator to automatically handle tracing for workflow nodes with path awareness.
    
    This decorator wraps workflow node methods to provide:
    - Automatic OpenTelemetry span creation with structured attributes
    - Path tracking and introspection capabilities
    - Execution path state management
    - Standardized error handling and status reporting
    - Consistent logging output for debugging
    
    Args:
        node_name (str): Unique identifier for the node (used in span names and path tracking)
        node_type (str): Category of node (e.g., "processor", "router", "analyzer")
                        Default: "processor"
    
    Returns:
        Callable: Decorated function with automatic tracing and path awareness
    
    Example:
        @traced_node("validation_node", "validator")
        async def validate_data(self, state: PathTrackingState) -> PathTrackingState:
            # Your validation logic here
            previous_node = SpanPathExtractor.get_previous_node_from_state(state)
            # Adapt behavior based on previous_node
            return state
    
    Span Attributes:
        - node.name: The node identifier
        - node.type: The node category
        - node.timestamp: ISO format timestamp of node execution
        - path.previous_node: Name of the immediately previous node
        - path.previous_path: String representation of path up to this node
        - path.current_depth: Number of nodes in current execution path
        - path.full_path: Complete execution path including current node
    
    Requirements:
        - The decorated method must be part of a class with a 'tracer' attribute
        - The method must accept 'state: PathTrackingState' as parameter
        - The method must return PathTrackingState
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(self, state: PathTrackingState) -> PathTrackingState:
            with self.tracer.start_as_current_span(f"node_{node_name}") as span:
                try:
                    # Standard tracing setup
                    span.set_attribute("node.name", node_name)
                    span.set_attribute("node.type", node_type)
                    span.set_attribute("node.timestamp", datetime.now().isoformat())
                    
                    # Path tracking from state
                    previous_node = SpanPathExtractor.get_previous_node_from_state(state)
                    previous_path = SpanPathExtractor.get_previous_node_path_from_state(state)
                    
                    span.set_attribute("path.previous_node", previous_node or "none")
                    span.set_attribute("path.previous_path", " -> ".join(previous_path) if previous_path else "none")
                    span.set_attribute("path.current_depth", len(state["execution_path"]))
                    
                    # Update execution path in state
                    state["execution_path"].append(node_name)
                    span.set_attribute("path.full_path", " -> ".join(state["execution_path"]))
                    
                    # Standard console output for debugging
                    print(f"🚀 Node {node_name}: Starting workflow" if node_name == "A" else f"🔧 Node {node_name}: {node_type}")
                    print(f"   Current path: {' -> '.join(state['execution_path'])}")
                    if previous_node:
                        print(f"   Previous node: {previous_node}")
                    
                    # Execute the actual node function
                    result = await func(self, state)
                    
                    # Mark span as successful
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    # Record error in span and re-raise
                    span.set_status(Status(StatusCode.ERROR, description=str(e)))
                    span.record_exception(e)
                    print(f"❌ Node {node_name} failed: {e}")
                    raise
                    
        return wrapper
    return decorator


class SpanPathExtractor:
    """
    Utility class for extracting execution path information from OpenTelemetry spans and workflow state.
    
    This class provides both state-based and span-based methods for path introspection,
    enabling nodes to adapt their behavior based on execution history.
    
    State-based methods (recommended for most use cases):
    - Use workflow state for reliable, efficient path tracking
    - Faster execution and guaranteed accuracy
    - Integrated with traced_node decorator
    
    Span-based methods (advanced use cases):
    - Extract path information directly from OpenTelemetry span hierarchy
    - Useful for debugging or when state is not available
    - More complex but provides additional span metadata
    """
    
    @staticmethod
    def get_execution_path_from_state(state: PathTrackingState) -> List[str]:
        """
        Get a copy of the complete execution path from workflow state.
        
        Args:
            state: The current workflow state
            
        Returns:
            List[str]: Copy of the execution path (safe to modify)
        """
        return state["execution_path"].copy()
    
    @staticmethod
    def get_previous_node_from_state(state: PathTrackingState) -> Optional[str]:
        """
        Get the immediately previous node from workflow state.
        
        Args:
            state: The current workflow state
            
        Returns:
            Optional[str]: Name of the previous node, or None if this is the first node
        """
        path = state["execution_path"]
        # Return the second-to-last node (the one before the current execution)
        return path[-2] if len(path) >= 2 else None
    
    @staticmethod
    def get_previous_node_path_from_state(state: PathTrackingState) -> List[str]:
        """
        Get the path excluding any nodes currently being added.
        
        Note: This returns the same as execution_path since the current node
        hasn't been added to the path yet when this is called.
        
        Args:
            state: The current workflow state
            
        Returns:
            List[str]: Copy of the execution path leading to this node
        """
        return state["execution_path"].copy()
    
    @staticmethod
    def get_execution_path_from_spans() -> List[str]:
        """
        Extract the execution path from OpenTelemetry span hierarchy.
        
        This method walks up the span tree to reconstruct the execution path
        by examining span attributes and names. Useful for debugging or when
        state-based tracking is not available.
        
        Returns:
            List[str]: Reconstructed execution path from span hierarchy
        """
        current_span = trace.get_current_span()
        if not current_span:
            return []
        
        path = []
        span = current_span
        
        # Walk up the span hierarchy to reconstruct path
        while span and hasattr(span, 'parent'):
            if hasattr(span, 'attributes'):
                attrs = getattr(span, 'attributes', {})
                node_name = attrs.get('node.name')
                if node_name and node_name not in path:
                    path.insert(0, node_name)
            
            # Also check span name for node information as fallback
            span_name = getattr(span, "name", None)
            if isinstance(span_name, str) and span_name.startswith('node_'):
                node_name = span_name.replace('node_', '')
                if node_name not in path:
                    path.insert(0, node_name)
            
            span = getattr(span, 'parent', None)
        
        return path
    
    @staticmethod
    def get_previous_node_from_spans() -> Optional[str]:
        """
        Get the previous node from OpenTelemetry span context.
        
        Searches parent spans to find the most recent node span.
        
        Returns:
            Optional[str]: Name of the previous node, or None if not found
        """
        current_span = trace.get_current_span()
        # Look for parent span that represents a node
        span = getattr(current_span, 'parent', None)
        while span:
            if hasattr(span, 'attributes'):
                attrs = getattr(span, 'attributes', {})
                node_name = attrs.get('node.name')
                if node_name:
                    return node_name
            
            # Check span name as fallback
            span_name = getattr(span, "get_span_name", None)
            if callable(span_name):
                span_name = span.get_span_name()
            else:
                span_name = getattr(span, "name", None)
            if isinstance(span_name, str) and span_name.startswith('node_'):
                return span_name.replace('node_', '')
            
            span = getattr(span, 'parent', None)
        
        return None
    
    @staticmethod
    def get_span_attributes() -> Dict[str, Any]:
        """
        Get all attributes from current and parent spans.
        
        Collects attributes from the entire span hierarchy, with child spans
        taking precedence over parent spans for duplicate keys.
        
        Returns:
            Dict[str, Any]: Aggregated span attributes
        """
        current_span = trace.get_current_span()
        if not current_span:
            return {}
        
        attributes = {}
        span = current_span
        
        while span and hasattr(span, 'parent'):
            if hasattr(span, 'attributes'):
                span_attrs = getattr(span, 'attributes', {})
                for key, value in span_attrs.items():
                    if key not in attributes:  # Don't override child span attributes
                        attributes[key] = value
            
            span = getattr(span, 'parent', None)
        
        return attributes
    
    @staticmethod
    def get_routing_history() -> List[Dict[str, Any]]:
        """
        Extract routing decisions from span hierarchy.
        
        Searches through parent spans to collect all routing decisions
        made during the current workflow execution.
        
        Returns:
            List[Dict[str, Any]]: List of routing decisions with metadata
        """
        current_span = trace.get_current_span()
        if not current_span:
            return []
        
        routing_history = []
        span = current_span
        
        while span and hasattr(span, 'parent'):
            if hasattr(span, 'attributes'):
                attrs = getattr(span, 'attributes', {})
                if 'routing.decision' in attrs:
                    routing_info = {
                        'node': attrs.get('node.name', 'unknown'),
                        'decision': attrs.get('routing.decision'),
                        'criteria': attrs.get('routing.criteria'),
                        'timestamp': attrs.get('node.timestamp')
                    }
                    routing_history.append(routing_info)
            
            span = getattr(span, 'parent', None)
        
        return list(reversed(routing_history))  # Return in chronological order
