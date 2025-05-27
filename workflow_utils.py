"""
Workflow Utilities Module

This module provides common utilities and patterns for building path-aware workflows
with LangGraph and OpenTelemetry. It includes router functions, state initialization,
and execution helpers.

Key Components:
- create_router_function: Factory for creating conditional edge routers
- create_initial_state: Helper for creating properly structured initial state
- ExecutionTracker: Utility class for tracking workflow execution metrics

Usage:
    from workflow_utils import create_router_function, create_initial_state
    from tracing import PathTrackingState
    
    # Create a router function for conditional edges
    router = create_router_function("decision_key", ["option1", "option2"])
    
    # Create initial state for workflow execution
    state = create_initial_state(custom_data={"key": "value"})
"""

from typing import List, Dict, Any, Callable, Optional
from datetime import datetime
import random

from tracing import PathTrackingState


def create_router_function(decision_key: str, options: List[str], 
                         default_option: Optional[str] = None) -> Callable[[PathTrackingState], str]:
    """
    Create a router function for LangGraph conditional edges.
    
    This factory function creates router functions that examine the routing_decisions
    in the workflow state to determine which edge to take. It's designed to work
    with the routing decision patterns used by @traced_node decorated functions.
    
    Args:
        decision_key (str): Key in state["routing_decisions"] to check for routing decision
        options (List[str]): List of valid routing options (node names)
        default_option (Optional[str]): Default option if decision not found.
                                      If None, uses the last option in the list.
        
    Returns:
        Callable: Router function compatible with LangGraph conditional edges
        
    Example:
        # Create router for A -> B1/B2 routing
        a_router = create_router_function("a_decision", ["B1", "B2"])
        workflow.add_conditional_edges("A", a_router, {"B1": "B1", "B2": "B2"})
        
        # Create router with explicit default
        b_router = create_router_function("b_decision", ["C1", "C2", "D1"], default_option="C2")
    """
    def router(state: PathTrackingState) -> str:
        """
        Router function that examines state for routing decisions.
        
        Args:
            state: Current workflow state
            
        Returns:
            str: Target node name for routing
        """
        if decision_key in state["routing_decisions"]:
            decision = state["routing_decisions"][decision_key]
            target = decision.get("target")
            if target in options:
                return target
        
        # Return default option
        return default_option if default_option else options[-1]
    
    return router


def create_initial_state(custom_data: Optional[Dict[str, Any]] = None,
                        routing_decisions: Optional[Dict[str, Dict]] = None,
                        node_metadata: Optional[Dict[str, Dict]] = None) -> PathTrackingState:
    """
    Create a properly structured initial state for workflow execution.
    
    This helper ensures that the initial state has all required fields properly
    initialized, preventing errors during workflow execution.
    
    Args:
        custom_data: Optional initial data to include in state["data"]
        routing_decisions: Optional initial routing decisions
        node_metadata: Optional initial node metadata
        
    Returns:
        PathTrackingState: Properly initialized workflow state
        
    Example:
        # Basic initialization
        state = create_initial_state()
        
        # With custom data
        state = create_initial_state(custom_data={"user_id": "12345", "request_type": "analysis"})
        
        # With pre-set routing decisions
        state = create_initial_state(
            routing_decisions={"initial_route": {"target": "B2", "reason": "high_priority"}}
        )
    """
    return PathTrackingState(
        data=custom_data or {},
        execution_path=[],
        routing_decisions=routing_decisions or {},
        node_metadata=node_metadata or {}
    )


class ExecutionTracker:
    """
    Utility class for tracking workflow execution metrics and patterns.
    
    This class helps analyze workflow behavior across multiple executions,
    tracking path variety, performance metrics, and routing patterns.
    """
    
    def __init__(self):
        """Initialize the execution tracker."""
        self.executions: List[Dict[str, Any]] = []
        self.start_time = datetime.now()
    
    def record_execution(self, execution_num: int, execution_path: List[str], 
                        execution_time: float, final_result: Optional[Dict[str, Any]] = None):
        """
        Record a completed workflow execution.
        
        Args:
            execution_num: Execution sequence number
            execution_path: Complete node execution path
            execution_time: Total execution time in seconds
            final_result: Optional final result data from the workflow
        """
        execution_record = {
            "execution_num": execution_num,
            "execution_path": execution_path.copy(),
            "execution_time": execution_time,
            "path_length": len(execution_path),
            "path_string": " → ".join(execution_path),
            "timestamp": datetime.now(),
            "final_result": final_result
        }
        self.executions.append(execution_record)
    
    def get_path_variety_stats(self) -> Dict[str, Any]:
        """
        Calculate path variety statistics across recorded executions.
        
        Returns:
            Dict containing path variety metrics
        """
        if not self.executions:
            return {"error": "No executions recorded"}
        
        unique_paths = set(exec_record["path_string"] for exec_record in self.executions)
        total_executions = len(self.executions)
        
        path_counts = {}
        for execution in self.executions:
            path = execution["path_string"]
            path_counts[path] = path_counts.get(path, 0) + 1
        
        return {
            "total_executions": total_executions,
            "unique_paths": len(unique_paths),
            "path_variety_percentage": (len(unique_paths) / total_executions) * 100,
            "path_distribution": path_counts,
            "most_common_path": max(path_counts.items(), key=lambda x: x[1]),
            "average_path_length": sum(exec_record["path_length"] for exec_record in self.executions) / total_executions
        }
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Calculate performance statistics across recorded executions.
        
        Returns:
            Dict containing performance metrics
        """
        if not self.executions:
            return {"error": "No executions recorded"}
        
        execution_times = [exec_record["execution_time"] for exec_record in self.executions]
        path_lengths = [exec_record["path_length"] for exec_record in self.executions]
        
        return {
            "total_executions": len(self.executions),
            "average_execution_time": sum(execution_times) / len(execution_times),
            "min_execution_time": min(execution_times),
            "max_execution_time": max(execution_times),
            "average_path_length": sum(path_lengths) / len(path_lengths),
            "min_path_length": min(path_lengths),
            "max_path_length": max(path_lengths),
            "total_tracking_time": (datetime.now() - self.start_time).total_seconds()
        }
    
    def print_summary(self):
        """Print a comprehensive summary of recorded executions."""
        if not self.executions:
            print("📊 No executions recorded yet")
            return
        
        print("\n" + "=" * 60)
        print("📊 Execution Summary")
        print("=" * 60)
        
        variety_stats = self.get_path_variety_stats()
        perf_stats = self.get_performance_stats()
        
        print(f"✅ Completed executions: {variety_stats['total_executions']}")
        print(f"🔀 Unique paths discovered: {variety_stats['unique_paths']}")
        print(f"📈 Path variety: {variety_stats['path_variety_percentage']:.1f}%")
        print(f"⏱️  Average execution time: {perf_stats['average_execution_time']:.3f}s")
        print(f"📏 Average path length: {perf_stats['average_path_length']:.1f} nodes")
        
        print("\n🛤️  All execution paths:")
        for execution in self.executions:
            print(f"   #{execution['execution_num']}: {execution['path_string']} "
                  f"({execution['execution_time']:.3f}s)")
        
        if variety_stats['unique_paths'] > 1:
            print("\n🎯 Path variety achieved: Different routing each run!")
        
        print(f"\n📋 Most common path: {variety_stats['most_common_path'][0]} "
              f"({variety_stats['most_common_path'][1]} times)")


def generate_routing_scores(node_name: str, options: List[str], 
                          randomization_factor: float = 0.3) -> Dict[str, float]:
    """
    Generate routing scores for decision-making nodes.
    
    This utility function generates realistic routing scores that can be used
    by nodes to make routing decisions. It includes configurable randomization
    to ensure path variety.
    
    Args:
        node_name: Name of the node generating scores (for reproducible randomization)
        options: List of routing options to score
        randomization_factor: Amount of randomization (0.0 = none, 1.0 = full random)
        
    Returns:
        Dict mapping option names to scores
        
    Example:
        scores = generate_routing_scores("B1", ["C1", "C2", "D1", "D2"])
        best_option = max(scores.items(), key=lambda x: x[1])[0]
    """
    scores = {}
    
    # Generate base scores for each option
    for i, option in enumerate(options):
        # Base score with slight preference for earlier options
        base_score = 5.0 - (i * 0.5)
        
        # Add randomization
        random_component = random.uniform(0, 5.0) * randomization_factor
        deterministic_component = base_score * (1 - randomization_factor)
        
        scores[option] = deterministic_component + random_component
    
    return scores


def apply_random_override(scores: Dict[str, float], override_probability: float = 0.25) -> str:
    """
    Apply random override to routing decisions for path variety.
    
    This function implements the random override pattern described in the README,
    where a certain percentage of routing decisions ignore scores and choose randomly.
    
    Args:
        scores: Dictionary of option scores
        override_probability: Probability of random override (0.0 to 1.0)
        
    Returns:
        str: Selected option (either highest scoring or random override)
        
    Example:
        scores = {"C1": 7.2, "C2": 4.1, "D1": 5.8}
        choice = apply_random_override(scores, override_probability=0.3)
    """
    if random.random() < override_probability:
        # Random override
        return random.choice(list(scores.keys()))
    else:
        # Choose highest scoring option
        return max(scores.items(), key=lambda x: x[1])[0]


# Export commonly used components
__all__ = [
    'create_router_function',
    'create_initial_state', 
    'ExecutionTracker',
    'generate_routing_scores',
    'apply_random_override'
]
