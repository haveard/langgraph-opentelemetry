import asyncio
import random
import sys
from typing import Dict, Any, List, Optional
from datetime import datetime

from langgraph.graph import StateGraph, END
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider, ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Status, StatusCode
from typing_extensions import TypedDict

class PathTrackingState(TypedDict):
    data: Dict[str, Any]
    execution_path: List[str]
    routing_decisions: Dict[str, Dict]
    node_metadata: Dict[str, Dict]

class SpanPathExtractor:
    """Utility class to extract execution path from current span context"""
    
    @staticmethod
    def get_execution_path_from_state(state: PathTrackingState) -> List[str]:
        """Get the execution path from state"""
        return state["execution_path"].copy()
    
    @staticmethod
    def get_previous_node_from_state(state: PathTrackingState) -> Optional[str]:
        """Get the immediately previous node from state"""
        path = state["execution_path"]
        return path[-1] if path else None
    
    @staticmethod
    def get_previous_node_path_from_state(state: PathTrackingState) -> List[str]:
        """Get the path excluding the current node (i.e., the path that led to this node)"""
        return state["execution_path"].copy()
    
    @staticmethod
    def get_execution_path_from_spans() -> List[str]:
        """Extract the execution path from span hierarchy - enhanced version"""
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
            
            # Also check span name for node information
            span_name = getattr(span, "name", None)
            if isinstance(span_name, str) and span_name.startswith('node_'):
                node_name = span_name.replace('node_', '')
                if node_name not in path:
                    path.insert(0, node_name)
            
            span = getattr(span, 'parent', None)
        
        return path
    
    @staticmethod
    def get_previous_node_from_spans() -> Optional[str]:
        """Get the previous node from span context"""
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
        """Get all attributes from current and parent spans"""
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
        """Extract routing decisions from span hierarchy"""
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
        
        return list(reversed(routing_history))

class PathAwareNodes:
    """Nodes that can introspect their execution path"""
    
    def __init__(self, tracer):
        self.tracer = tracer
    
    async def node_a(self, state: PathTrackingState) -> PathTrackingState:
        """Entry point node with enhanced randomization"""
        with self.tracer.start_as_current_span("node_A") as span:
            span.set_attribute("node.name", "A")
            span.set_attribute("node.type", "entry_point")
            span.set_attribute("node.timestamp", datetime.now().isoformat())
            
            # Track previous node path (empty for entry point)
            previous_node = SpanPathExtractor.get_previous_node_from_state(state)
            previous_path = SpanPathExtractor.get_previous_node_path_from_state(state)
            
            span.set_attribute("path.previous_node", previous_node or "none")
            span.set_attribute("path.previous_path", " -> ".join(previous_path) if previous_path else "none")
            span.set_attribute("path.current_depth", len(state["execution_path"]))
            
            # Initialize execution tracking
            state["execution_path"].append("A")
            
            # Enhanced randomization with more variety
            request_types = ["data_processing", "analytics", "validation", "transformation", "aggregation"]
            complexity_levels = ["simple", "moderate", "complex", "very_complex"]
            urgency_levels = ["low", "normal", "high", "critical"]
            data_types = ["structured", "unstructured", "mixed", "streaming"]
            
            state["data"]["entry_data"] = {
                "request_id": f"req_{random.randint(1000, 9999)}",
                "data_size": random.randint(50, 2000),  # Wider range
                "priority": random.choice(["high", "medium", "low"]),
                "request_type": random.choice(request_types),
                "complexity": random.choice(complexity_levels),
                "urgency": random.choice(urgency_levels),
                "data_type": random.choice(data_types),
                "batch_size": random.randint(1, 100),
                "retry_count": 0,
                "processing_flags": {
                    "requires_validation": random.choice([True, False]),
                    "needs_preprocessing": random.choice([True, False]),
                    "enable_parallel": random.choice([True, False]),
                    "use_cache": random.choice([True, False])
                }
            }
            
            span.set_attribute("data.size", state["data"]["entry_data"]["data_size"])
            span.set_attribute("data.priority", state["data"]["entry_data"]["priority"])
            span.set_attribute("data.type", state["data"]["entry_data"]["data_type"])
            span.set_attribute("data.complexity", state["data"]["entry_data"]["complexity"])
            span.set_attribute("path.full_path", " -> ".join(state["execution_path"]))
            
            print(f"🚀 Node A: Starting workflow")
            print(f"   Current path: {' -> '.join(state['execution_path'])}")
            print(f"   Previous node: {previous_node or 'none'}")
            print(f"   Request type: {state['data']['entry_data']['request_type']}")
            print(f"   Complexity: {state['data']['entry_data']['complexity']}")
            
            return state
    
    async def node_b1(self, state: PathTrackingState) -> PathTrackingState:
        """Alternative routing node with different decision logic"""
        with self.tracer.start_as_current_span("node_B1") as span:
            span.set_attribute("node.name", "B1")
            span.set_attribute("node.type", "alternative_router")
            span.set_attribute("node.timestamp", datetime.now().isoformat())
            
            previous_node = SpanPathExtractor.get_previous_node_from_state(state)
            previous_path = SpanPathExtractor.get_previous_node_path_from_state(state)
            
            span.set_attribute("path.previous_node", previous_node or "none")
            span.set_attribute("path.previous_path", " -> ".join(previous_path) if previous_path else "none")
            span.set_attribute("path.current_depth", len(state["execution_path"]))
            
            state["execution_path"].append("B1")
            span.set_attribute("path.full_path", " -> ".join(state["execution_path"]))
            
            print(f"🔀 Node B1: Alternative routing logic")
            print(f"   Current path: {' -> '.join(state['execution_path'])}")
            print(f"   Previous node: {previous_node}")
            
            # Different routing logic based on request type and complexity
            entry_data = state["data"]["entry_data"]
            request_type = entry_data["request_type"]
            complexity = entry_data["complexity"]
            data_type = entry_data["data_type"]
            
            # More sophisticated routing with enhanced randomization for variety
            routing_score = 0
            routing_factors = []
            
            # Request type scoring (reduced weights for more balance)
            if request_type in ["analytics", "aggregation"]:
                routing_score += 2  # Reduced from 3
                routing_factors.append("analytics_preferred")
            elif request_type in ["validation", "transformation"]:
                routing_score += 1  # Favor D1 for validation
                routing_factors.append("validation_preferred")
            
            # Complexity scoring (more balanced)
            if complexity in ["complex", "very_complex"]:
                routing_score += 1.5  # Reduced from 2
                routing_factors.append("high_complexity")
            elif complexity in ["simple", "moderate"]:
                routing_score += 1    # Favor lighter processing
                routing_factors.append("moderate_complexity")
            
            # Data type influence
            if data_type == "streaming":
                routing_score += 1.5  # Reduced from 2
                routing_factors.append("streaming_data")
            elif data_type in ["unstructured", "mixed"]:
                routing_score += 1
                routing_factors.append("complex_data_type")
            
            # Processing flags
            if entry_data["processing_flags"]["needs_preprocessing"]:
                routing_score += 1  # Reduced from 1
                routing_factors.append("preprocessing_required")
            if entry_data["processing_flags"]["requires_validation"]:
                routing_score += 1.5  # Favor D1 for validation
                routing_factors.append("validation_required")
            
            # Add significant randomness for variety (up to 4 points)
            random_factor = random.uniform(0, 4)  # Increased from 2
            routing_score += random_factor
            routing_factors.append(f"random_boost_{random_factor:.1f}")
            
            # More varied decision thresholds
            if routing_score >= 6:
                routing_decision = "C1"
                routing_reason = "high_complexity_processing"
            elif routing_score >= 4:
                routing_decision = "D1"  # More likely to hit D1
                routing_reason = "intermediate_processing" 
            elif routing_score >= 2:
                routing_decision = "C2"
                routing_reason = "lightweight_processing"
            else:
                # Even low scores can go to D2 sometimes
                routing_decision = random.choice(["C2", "D2"])
                routing_reason = "fallback_processing"
            
            # Add 20% pure random override for maximum variety
            if random.random() < 0.2:
                routing_decision = random.choice(["C1", "C2", "D1", "D2"])
                routing_reason = "pure_random_override"
                routing_factors.append("random_override_applied")
            
            state["routing_decisions"]["b1_decision"] = {
                "target": routing_decision,
                "reason": routing_reason,
                "score": routing_score,
                "factors": routing_factors,
                "context": {
                    "request_type": request_type,
                    "complexity": complexity,
                    "data_type": data_type,
                    "path_length": len(state["execution_path"])
                }
            }
            
            span.set_attribute("routing.decision", routing_decision)
            span.set_attribute("routing.reason", routing_reason)
            span.set_attribute("routing.score", int(routing_score * 100))  # Convert to int
            span.set_attribute("routing.factors", ",".join(routing_factors))
            
            print(f"   Routing decision: {routing_decision} (score: {routing_score:.1f})")
            print(f"   Routing factors: {routing_factors}")
            
            return state
    
    async def node_b2(self, state: PathTrackingState) -> PathTrackingState:
        """Node that makes routing decisions based on path context with enhanced randomization"""
        with self.tracer.start_as_current_span("node_B2") as span:
            span.set_attribute("node.name", "B2")
            span.set_attribute("node.type", "primary_router")
            span.set_attribute("node.timestamp", datetime.now().isoformat())
            
            # Enhanced path tracking
            previous_node = SpanPathExtractor.get_previous_node_from_state(state)
            previous_path = SpanPathExtractor.get_previous_node_path_from_state(state)
            
            span.set_attribute("path.previous_node", previous_node or "none")
            span.set_attribute("path.previous_path", " -> ".join(previous_path) if previous_path else "none")
            span.set_attribute("path.current_depth", len(state["execution_path"]))
            
            # Get execution context
            span_attributes = SpanPathExtractor.get_span_attributes()
            routing_history = SpanPathExtractor.get_routing_history()
            
            state["execution_path"].append("B2")
            span.set_attribute("path.full_path", " -> ".join(state["execution_path"]))
            
            print(f"🔀 Node B2: Enhanced routing analysis")
            print(f"   Current path: {' -> '.join(state['execution_path'])}")
            print(f"   Previous node: {previous_node}")
            print(f"   Previous path: {' -> '.join(previous_path) if previous_path else 'none'}")
            
            # Enhanced routing logic with multiple decision points
            entry_data = state["data"]["entry_data"]
            data_size = entry_data["data_size"]
            priority = entry_data["priority"]
            urgency = entry_data["urgency"]
            complexity = entry_data["complexity"]
            
            # Multi-factor decision matrix with enhanced randomization
            decision_matrix = {
                "C1": 0,  # Complex processing
                "C2": 0,  # Lightweight processing  
                "D1": 0,  # Intermediate processing
                "D2": 0   # Parallel processing
            }
            
            # Priority scoring (reduced weight to allow more variety)
            priority_weights = {"high": 2, "medium": 1.5, "low": 1}
            urgency_weights = {"critical": 2.5, "high": 2, "normal": 1.5, "low": 1}
            complexity_weights = {"very_complex": 2.5, "complex": 2, "moderate": 1.5, "simple": 1}
            
            decision_matrix["C1"] += priority_weights.get(priority, 1) * 1.5  # Reduced multiplier
            decision_matrix["C1"] += urgency_weights.get(urgency, 1) * 0.8
            decision_matrix["C1"] += complexity_weights.get(complexity, 1) * 0.8
            
            # Size-based routing (more balanced thresholds)
            if data_size > 1200:
                decision_matrix["D2"] += 2.5  # Reduced from 4
                decision_matrix["C1"] += 1    # Reduced from 2
            elif data_size > 600:
                decision_matrix["D1"] += 2    # Reduced from 3
                decision_matrix["C1"] += 0.5  # Reduced from 1
            else:
                decision_matrix["C2"] += 2    # Reduced from 3
            
            # Processing flags influence (reduced impact)
            flags = entry_data["processing_flags"]
            if flags["enable_parallel"]:
                decision_matrix["D2"] += 1.5  # Reduced from 3
            if flags["requires_validation"]:
                decision_matrix["D1"] += 1.5  # Reduced from 2
            if flags["use_cache"]:
                decision_matrix["C2"] += 1.5  # Reduced from 2
            
            # Path history influence
            if "B1" in state["execution_path"]:
                decision_matrix["C1"] += 0.5  # Reduced from 1
                # B1 also likes D1 for intermediate processing
                decision_matrix["D1"] += 1
                
            # Add significant randomness for variety (each option gets 0-3 points randomly)
            for key in decision_matrix:
                decision_matrix[key] += random.uniform(0, 3)
            
            # Add base score to ensure all options are viable
            base_scores = {"C1": 1, "C2": 1.5, "D1": 1.2, "D2": 1.1}
            for key, base in base_scores.items():
                decision_matrix[key] += base
            
            # Select the highest scoring option
            routing_decision = max(decision_matrix.items(), key=lambda x: x[1])[0]
            max_score = decision_matrix[routing_decision]
            
            # Increase random override chance for more variety (25% chance)
            if random.random() < 0.25:
                alternatives = [k for k, v in decision_matrix.items() if k != routing_decision and v > max_score * 0.6]
                if alternatives:
                    routing_decision = random.choice(alternatives)
                    routing_reason = "random_override"
                else:
                    routing_reason = "optimal_choice"
            else:
                routing_reason = "score_based"
            
            state["routing_decisions"]["b2_decision"] = {
                "target": routing_decision,
                "reason": routing_reason,
                "decision_matrix": decision_matrix,
                "max_score": max_score,
                "context": {
                    "data_size": data_size,
                    "priority": priority,
                    "urgency": urgency,
                    "complexity": complexity,
                    "path_length": len(state["execution_path"]),
                    "has_b1_in_path": "B1" in state["execution_path"]
                }
            }
            
            span.set_attribute("routing.decision", routing_decision)
            span.set_attribute("routing.reason", routing_reason)
            span.set_attribute("routing.max_score", int(max_score * 100))  # Convert to int for OpenTelemetry
            span.set_attribute("routing.alternatives", len([v for v in decision_matrix.values() if v > max_score * 0.8]))
            
            print(f"   Decision matrix: {decision_matrix}")
            print(f"   Routing decision: {routing_decision} (score: {max_score:.1f}, reason: {routing_reason})")
            
            return state
    
    async def node_d1(self, state: PathTrackingState) -> PathTrackingState:
        """Intermediate processing node with validation capabilities"""
        with self.tracer.start_as_current_span("node_D1") as span:
            span.set_attribute("node.name", "D1")
            span.set_attribute("node.type", "intermediate_processor")
            span.set_attribute("node.timestamp", datetime.now().isoformat())
            
            previous_node = SpanPathExtractor.get_previous_node_from_state(state)
            previous_path = SpanPathExtractor.get_previous_node_path_from_state(state)
            
            span.set_attribute("path.previous_node", previous_node or "none")
            span.set_attribute("path.previous_path", " -> ".join(previous_path) if previous_path else "none")
            span.set_attribute("path.current_depth", len(state["execution_path"]))
            
            state["execution_path"].append("D1")
            span.set_attribute("path.full_path", " -> ".join(state["execution_path"]))
            
            print(f"🔧 Node D1: Intermediate processing with validation")
            print(f"   Path: {' -> '.join(state['execution_path'])}")
            print(f"   Previous node: {previous_node}")
            
            # Processing based on how we got here
            entry_data = state["data"]["entry_data"]
            processing_strategy = "standard_intermediate"
            
            if previous_node == "B1":
                processing_strategy = "b1_optimized"
                processing_time = random.uniform(0.08, 0.15)
            elif previous_node == "B2":
                processing_strategy = "b2_optimized"  
                processing_time = random.uniform(0.06, 0.12)
            else:
                processing_strategy = "fallback"
                processing_time = random.uniform(0.1, 0.18)
            
            # Add validation if required
            if entry_data["processing_flags"]["requires_validation"]:
                processing_strategy += "_with_validation"
                processing_time += random.uniform(0.02, 0.05)
            
            span.set_attribute("processing.strategy", processing_strategy)
            span.set_attribute("processing.time_ms", int(processing_time * 1000))
            
            await asyncio.sleep(processing_time)
            
            # Enhanced routing recommendation with more variety
            path_length = len(state["execution_path"])
            complexity_factor = entry_data["complexity"]
            
            # Multiple factors influence the recommendation
            c1_likelihood = 0
            
            # Base factors
            if complexity_factor in ["complex", "very_complex"]:
                c1_likelihood += 0.3
            if entry_data["data_size"] > 1000:
                c1_likelihood += 0.2
            if entry_data["processing_flags"]["enable_parallel"]:
                c1_likelihood += 0.15
            if previous_node == "B1":
                c1_likelihood += 0.1  # B1 might want additional processing
            
            # Path length consideration
            if path_length <= 3:
                c1_likelihood += 0.1  # Short paths can afford more processing
            else:
                c1_likelihood -= 0.1  # Longer paths might want to wrap up
            
            # Add randomness for variety
            c1_likelihood += random.uniform(-0.2, 0.3)
            
            # Make the decision
            should_route_to_c1 = c1_likelihood > 0.35  # Adjusted threshold
            
            # Additional pure random chance for variety
            if random.random() < 0.2:  # 20% pure random override
                should_route_to_c1 = random.choice([True, False])
                recommendation_reason = "random_override"
            else:
                recommendation_reason = f"likelihood_{c1_likelihood:.2f}"
            
            state["data"]["d1_result"] = {
                "processing_time": processing_time,
                "strategy": processing_strategy,
                "previous_node": previous_node,
                "validation_performed": entry_data["processing_flags"]["requires_validation"],
                "next_recommendation": "C1" if should_route_to_c1 else "E",
                "recommendation_reason": recommendation_reason,
                "c1_likelihood": c1_likelihood,
                "path_context": {
                    "execution_path": state["execution_path"].copy(),
                    "came_from": previous_node
                }
            }
            
            span.set_attribute("processing.next_recommendation", "C1" if should_route_to_c1 else "E")
            
            print(f"   Strategy: {processing_strategy}")
            print(f"   Recommending next: {'C1' if should_route_to_c1 else 'E'}")
            
            return state
    
    async def node_d2(self, state: PathTrackingState) -> PathTrackingState:
        """Parallel processing node for large data"""
        with self.tracer.start_as_current_span("node_D2") as span:
            span.set_attribute("node.name", "D2")
            span.set_attribute("node.type", "parallel_processor")
            span.set_attribute("node.timestamp", datetime.now().isoformat())
            
            previous_node = SpanPathExtractor.get_previous_node_from_state(state)
            previous_path = SpanPathExtractor.get_previous_node_path_from_state(state)
            
            span.set_attribute("path.previous_node", previous_node or "none")
            span.set_attribute("path.previous_path", " -> ".join(previous_path) if previous_path else "none")
            span.set_attribute("path.current_depth", len(state["execution_path"]))
            
            state["execution_path"].append("D2")
            span.set_attribute("path.full_path", " -> ".join(state["execution_path"]))
            
            print(f"⚡ Node D2: Parallel processing for large datasets")
            print(f"   Path: {' -> '.join(state['execution_path'])}")
            print(f"   Previous node: {previous_node}")
            
            entry_data = state["data"]["entry_data"]
            data_size = entry_data["data_size"]
            
            # Simulate parallel processing
            parallel_workers = min(4, max(1, data_size // 400))
            processing_time = random.uniform(0.05, 0.1) + (data_size / 10000)
            
            # Path-aware optimization
            if "B1" in state["execution_path"] and "B2" in state["execution_path"]:
                optimization = "dual_router_optimization"
                processing_time *= 0.9  # 10% faster
            elif previous_node in ["B1", "B2"]:
                optimization = f"{previous_node.lower()}_direct_optimization"
                processing_time *= 0.95  # 5% faster
            else:
                optimization = "standard_processing"
            
            span.set_attribute("processing.parallel_workers", parallel_workers)
            span.set_attribute("processing.optimization", optimization)
            span.set_attribute("processing.time_ms", int(processing_time * 1000))
            
            await asyncio.sleep(processing_time)
            
            # Enhanced decision logic for additional processing
            complexity_score = len(state["execution_path"]) + (data_size / 500)
            
            # Multiple factors for deciding additional processing
            additional_processing_likelihood = 0
            
            # Base complexity assessment
            if complexity_score > 5:
                additional_processing_likelihood += 0.25
            if data_size > 1500:
                additional_processing_likelihood += 0.2
            if entry_data["complexity"] in ["very_complex", "complex"]:
                additional_processing_likelihood += 0.2
            if entry_data["processing_flags"]["enable_parallel"]:
                additional_processing_likelihood += 0.15
            
            # Path context influence
            if "B1" in state["execution_path"]:
                additional_processing_likelihood += 0.1  # B1 paths may need more processing
            if previous_node == "B2":
                additional_processing_likelihood += 0.05  # Direct B2 routing
            
            # Random factor for variety
            additional_processing_likelihood += random.uniform(-0.15, 0.25)
            
            # Make the decision with enhanced randomness
            if random.random() < 0.15:  # 15% pure random override
                needs_additional_processing = random.choice([True, False])
                processing_reason = "random_override"
            else:
                needs_additional_processing = additional_processing_likelihood > 0.4
                processing_reason = f"likelihood_{additional_processing_likelihood:.2f}"
            
            state["data"]["d2_result"] = {
                "processing_time": processing_time,
                "parallel_workers": parallel_workers,
                "optimization": optimization,
                "data_size_processed": data_size,
                "previous_node": previous_node,
                "needs_additional_processing": needs_additional_processing,
                "processing_reason": processing_reason,
                "additional_processing_likelihood": additional_processing_likelihood,
                "complexity_score": complexity_score,
                "path_context": {
                    "execution_path": state["execution_path"].copy(),
                    "routing_history": SpanPathExtractor.get_routing_history()
                }
            }
            
            span.set_attribute("processing.needs_additional", needs_additional_processing)
            span.set_attribute("processing.complexity_score", int(complexity_score * 100))
            
            print(f"   Parallel workers: {parallel_workers}")
            print(f"   Optimization: {optimization}")
            print(f"   Additional processing needed: {needs_additional_processing}")
            
            return state
    
    async def node_c1(self, state: PathTrackingState) -> PathTrackingState:
        """Complex processing node that adapts based on how it was reached"""
        with self.tracer.start_as_current_span("node_C1") as span:
            span.set_attribute("node.name", "C1")
            span.set_attribute("node.type", "adaptive_processor")
            span.set_attribute("node.timestamp", datetime.now().isoformat())
            
            # Enhanced path introspection
            previous_node = SpanPathExtractor.get_previous_node_from_state(state)
            previous_path = SpanPathExtractor.get_previous_node_path_from_state(state)
            span_attributes = SpanPathExtractor.get_span_attributes()
            routing_history = SpanPathExtractor.get_routing_history()
            
            span.set_attribute("path.previous_node", previous_node or "none")
            span.set_attribute("path.previous_path", " -> ".join(previous_path) if previous_path else "none")
            span.set_attribute("path.current_depth", len(state["execution_path"]))
            
            state["execution_path"].append("C1")
            span.set_attribute("path.full_path", " -> ".join(state["execution_path"]))
            
            print(f"🧠 Node C1: Adaptive processing based on execution path")
            print(f"   Full execution path: {' -> '.join(state['execution_path'])}")
            print(f"   Previous node: {previous_node}")
            print(f"   Previous path: {' -> '.join(previous_path) if previous_path else 'none'}")
            
            # Analyze how we got here with enhanced context
            came_from_b1 = "B1" in state["execution_path"]
            came_from_b2 = "B2" in state["execution_path"]
            routing_from_b2 = any(r.get('node') == 'B2' for r in routing_history)
            direct_from_previous = previous_node in ["B1", "B2"]
            
            span.set_attribute("path.came_from_b1", came_from_b1)
            span.set_attribute("path.came_from_b2", came_from_b2)
            span.set_attribute("path.routed_from_b2", routing_from_b2)
            span.set_attribute("path.direct_from_previous", direct_from_previous)
            span.set_attribute("path.immediate_predecessor", previous_node or "none")
            
            # Enhanced adaptation logic that considers immediate predecessor
            if came_from_b1 and came_from_b2:
                strategy = "multi_branch_convergence"
                complexity = "high"
                processing_time = random.uniform(0.15, 0.3)
            elif routing_from_b2 and previous_node == "B2":
                b2_decision = state["routing_decisions"].get("b2_decision", {})
                reason = b2_decision.get("reason", "unknown")
                if "high_priority" in reason:
                    strategy = "priority_processing"
                    complexity = "high"
                    processing_time = random.uniform(0.1, 0.2)
                else:
                    strategy = "standard_processing"
                    complexity = "medium"
                    processing_time = random.uniform(0.08, 0.15)
            elif previous_node == "B2":
                strategy = "direct_b2_processing"
                complexity = "medium"
                processing_time = random.uniform(0.06, 0.12)
            else:
                strategy = "fallback_processing"
                complexity = "low"
                processing_time = random.uniform(0.05, 0.1)
            
            span.set_attribute("processing.strategy", strategy)
            span.set_attribute("processing.complexity", complexity)
            span.set_attribute("processing.time_ms", processing_time * 1000)
            span.set_attribute("processing.adaptation_reason", f"prev={previous_node},b1={came_from_b1},b2={came_from_b2}")
            
            print(f"   Processing strategy: {strategy} (complexity: {complexity})")
            print(f"   Reasoning: came_from_b1={came_from_b1}, came_from_b2={came_from_b2}, prev_node={previous_node}")
            
            # Store enhanced path-aware results
            state["data"]["c1_result"] = {
                "strategy": strategy,
                "complexity": complexity,
                "processing_time": processing_time,
                "path_context": {
                    "execution_path": state["execution_path"].copy(),
                    "previous_node": previous_node,
                    "previous_path": previous_path.copy(),
                    "routing_history": routing_history,
                    "adaptation_factors": {
                        "came_from_b1": came_from_b1,
                        "came_from_b2": came_from_b2,
                        "direct_from_previous": direct_from_previous,
                        "immediate_predecessor": previous_node
                    },
                    "span_attributes": {k: v for k, v in span_attributes.items() if not k.startswith('internal.')}
                }
            }
            
            # Simulate processing
            await asyncio.sleep(processing_time)
            
            return state
    
    async def node_c2(self, state: PathTrackingState) -> PathTrackingState:
        """Lightweight processing node"""
        with self.tracer.start_as_current_span("node_C2") as span:
            span.set_attribute("node.name", "C2")
            span.set_attribute("node.type", "lightweight_processor")
            span.set_attribute("node.timestamp", datetime.now().isoformat())
            
            # Enhanced path tracking for C2
            previous_node = SpanPathExtractor.get_previous_node_from_state(state)
            previous_path = SpanPathExtractor.get_previous_node_path_from_state(state)
            
            span.set_attribute("path.previous_node", previous_node or "none")
            span.set_attribute("path.previous_path", " -> ".join(previous_path) if previous_path else "none")
            span.set_attribute("path.current_depth", len(state["execution_path"]))
            
            state["execution_path"].append("C2")
            span.set_attribute("path.full_path", " -> ".join(state["execution_path"]))
            
            print(f"⚡ Node C2: Lightweight processing")
            print(f"   Path to here: {' -> '.join(state['execution_path'])}")
            print(f"   Previous node: {previous_node}")
            print(f"   Previous path: {' -> '.join(previous_path) if previous_path else 'none'}")
            
            # Check routing context with enhanced path awareness
            routing_history = SpanPathExtractor.get_routing_history()
            b2_routing = next((r for r in routing_history if r.get('node') == 'B2'), None)
            
            if b2_routing:
                print(f"   Routed from B2: {b2_routing.get('decision')} ({b2_routing.get('criteria')})")
                span.set_attribute("routing.source", "B2")
                span.set_attribute("routing.criteria", b2_routing.get('criteria', ''))
                span.set_attribute("routing.came_directly_from_b2", previous_node == "B2")
            
            # Adapt processing based on how we arrived
            if previous_node == "B2":
                processing_approach = "direct_b2_routing"
                processing_time = random.uniform(0.03, 0.06)
            elif "B2" in state["execution_path"]:
                processing_approach = "indirect_b2_routing"
                processing_time = random.uniform(0.04, 0.08)
            else:
                processing_approach = "alternative_routing"
                processing_time = random.uniform(0.05, 0.08)
            
            span.set_attribute("processing.approach", processing_approach)
            span.set_attribute("processing.time_ms", processing_time * 1000)
            span.set_attribute("processing.optimized_for_previous", previous_node or "none")
            
            await asyncio.sleep(processing_time)
            
            state["data"]["c2_result"] = {
                "processing_time": processing_time,
                "processing_approach": processing_approach,
                "routed_from": b2_routing.get('node') if b2_routing else None,
                "previous_node": previous_node,
                "previous_path": previous_path.copy(),
                "path_length": len(state["execution_path"]),
                "routing_context": {
                    "came_directly_from_b2": previous_node == "B2",
                    "b2_in_path": "B2" in state["execution_path"]
                }
            }
            
            print(f"   Processing approach: {processing_approach} (optimized for: {previous_node})")
            
            return state
    
    async def node_e(self, state: PathTrackingState) -> PathTrackingState:
        """Final convergence node that analyzes the complete execution"""
        with self.tracer.start_as_current_span("node_E") as span:
            span.set_attribute("node.name", "E")
            span.set_attribute("node.type", "convergence_analyzer")
            span.set_attribute("node.timestamp", datetime.now().isoformat())
            
            # Enhanced path analysis
            previous_node = SpanPathExtractor.get_previous_node_from_state(state)
            previous_path = SpanPathExtractor.get_previous_node_path_from_state(state)
            all_attributes = SpanPathExtractor.get_span_attributes()
            routing_history = SpanPathExtractor.get_routing_history()
            
            span.set_attribute("path.previous_node", previous_node or "none")
            span.set_attribute("path.previous_path", " -> ".join(previous_path) if previous_path else "none")
            span.set_attribute("path.current_depth", len(state["execution_path"]))
            
            state["execution_path"].append("E")
            span.set_attribute("path.full_path", " -> ".join(state["execution_path"]))
            
            print(f"🎯 Node E: Final convergence and path analysis")
            print(f"   Complete execution path: {' -> '.join(state['execution_path'])}")
            print(f"   Immediate previous node: {previous_node}")
            print(f"   Previous path before E: {' -> '.join(previous_path) if previous_path else 'none'}")
            print(f"   Total routing decisions: {len(routing_history)}")
            
            # Analyze the complete workflow execution with enhanced path tracking
            paths_taken = []
            if "c1_result" in state["data"]:
                paths_taken.append("C1")
            if "c2_result" in state["data"]:
                paths_taken.append("C2")
            
            # Enhanced performance analysis with path context
            total_processing_time = 0
            node_performances = {}
            path_analysis = {}
            
            for key, result in state["data"].items():
                if isinstance(result, dict) and "processing_time" in result:
                    total_processing_time += result["processing_time"]
                    node_name = key.replace("_result", "")
                    node_performances[node_name] = result["processing_time"]
                    
                    # Extract path-specific analysis
                    if "path_context" in result:
                        path_context = result["path_context"]
                        path_analysis[node_name] = {
                            "previous_node": path_context.get("previous_node"),
                            "previous_path": path_context.get("previous_path", []),
                            "adaptation_factors": path_context.get("adaptation_factors", {}),
                            "processing_approach": result.get("processing_approach"),
                            "strategy": result.get("strategy")
                        }
            
            span.set_attribute("convergence.paths_taken", ",".join(paths_taken))
            span.set_attribute("convergence.total_nodes", len(state["execution_path"]))
            span.set_attribute("convergence.immediate_predecessor", previous_node or "none")
            span.set_attribute("performance.total_time_ms", total_processing_time * 1000)
            
            # Path-based optimization suggestions with enhanced analysis
            optimization_suggestions = []
            if len(routing_history) > 2:
                optimization_suggestions.append("Consider simplifying routing logic")
            if total_processing_time > 0.5:
                optimization_suggestions.append("Review processing efficiency")
            if previous_node in ["C1", "C2"]:
                optimization_suggestions.append(f"Direct convergence from {previous_node} - efficient path")
            
            # Analyze path efficiency
            path_efficiency = {
                "total_hops": len(state["execution_path"]),
                "routing_decisions": len(routing_history),
                "convergence_point": previous_node,
                "path_complexity": "high" if len(state["execution_path"]) > 4 else "medium" if len(state["execution_path"]) > 3 else "low"
            }
            
            state["data"]["final_analysis"] = {
                "execution_path": state["execution_path"].copy(),
                "routing_decisions": routing_history,
                "paths_converged": paths_taken,
                "immediate_predecessor": previous_node,
                "previous_path": previous_path.copy(),
                "total_processing_time": total_processing_time,
                "node_performances": node_performances,
                "path_analysis": path_analysis,
                "path_efficiency": path_efficiency,
                "optimization_suggestions": optimization_suggestions,
                "trace_attributes": {k: v for k, v in all_attributes.items() if 'internal' not in k}
            }
            
            print(f"   Paths converged: {paths_taken}")
            print(f"   Convergence from: {previous_node}")
            print(f"   Path efficiency: {path_efficiency['path_complexity']} complexity ({path_efficiency['total_hops']} hops)")
            print(f"   Total processing time: {total_processing_time:.3f}s")
            print(f"   Optimization suggestions: {optimization_suggestions}")
            
            return state

def setup_telemetry(use_jaeger=True, jaeger_endpoint="http://localhost:14268/api/traces"):
    """Setup OpenTelemetry with Jaeger or console output for debugging"""
    # Only setup if not already configured
    current_provider = trace.get_tracer_provider()
    if hasattr(current_provider, '_resource'):
        # Already setup, just return tracer
        return trace.get_tracer(__name__)
    
    resource = Resource.create({
        "service.name": "path-aware-workflow",
        "service.version": "1.0.0",
        "service.namespace": "langgraph-demo"
    })
    
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)
    
    if use_jaeger:
        # Setup Jaeger exporter
        jaeger_exporter = JaegerExporter(
            agent_host_name="localhost",
            agent_port=6831,
            collector_endpoint=jaeger_endpoint,
        )
        span_processor = SimpleSpanProcessor(jaeger_exporter)
        provider.add_span_processor(span_processor)
        print(f"📡 Telemetry: Sending traces to Jaeger at {jaeger_endpoint}")
    else:
        # Use console exporter to see spans in terminal
        console_exporter = ConsoleSpanExporter()
        span_processor = SimpleSpanProcessor(console_exporter)
        provider.add_span_processor(span_processor)
        print("📝 Telemetry: Using console output")
    
    return trace.get_tracer(__name__)

async def run_path_aware_workflow(use_jaeger=True, jaeger_endpoint="http://localhost:14268/api/traces"):
    """Run workflow where nodes can see their execution path"""
    
    tracer = setup_telemetry(use_jaeger=use_jaeger, jaeger_endpoint=jaeger_endpoint)
    nodes = PathAwareNodes(tracer)
    
    # Create enhanced workflow with multiple routing points
    workflow = StateGraph(PathTrackingState)
    
    # Add all nodes
    workflow.add_node("A", nodes.node_a)
    workflow.add_node("B1", nodes.node_b1) 
    workflow.add_node("B2", nodes.node_b2)
    workflow.add_node("C1", nodes.node_c1)
    workflow.add_node("C2", nodes.node_c2)
    workflow.add_node("D1", nodes.node_d1)
    workflow.add_node("D2", nodes.node_d2)
    workflow.add_node("E", nodes.node_e)
    
    workflow.set_entry_point("A")
    
    # Enhanced routing logic with much more randomization
    def route_from_a(state: PathTrackingState) -> str:
        """Route from A to either B1 or B2 with high randomization for variety"""
        entry_data = state["data"]["entry_data"]
        
        # Calculate routing scores for both options
        b1_score = 0
        b2_score = 0
        
        # Preference factors for B1
        if entry_data["request_type"] in ["validation", "transformation"]:
            b1_score += 2
        if entry_data["data_type"] in ["unstructured", "mixed"]:
            b1_score += 1.5
        if entry_data["processing_flags"]["needs_preprocessing"]:
            b1_score += 1
        if entry_data["complexity"] in ["simple", "moderate"]:
            b1_score += 1
        
        # Preference factors for B2  
        if entry_data["request_type"] in ["analytics", "aggregation"]:
            b2_score += 2
        if entry_data["data_size"] > 1000:
            b2_score += 1.5
        if entry_data["priority"] == "high":
            b2_score += 1
        if entry_data["urgency"] in ["high", "critical"]:
            b2_score += 1
        
        # Add significant randomness to ensure variety (40% of decision)
        b1_score += random.uniform(0, 3)
        b2_score += random.uniform(0, 3)
        
        # Add base randomness to ensure both paths are viable
        base_random = random.uniform(0, 2)
        b1_score += base_random
        b2_score += (2 - base_random)  # Inverse correlation
        
        # Make the final decision - but add a 30% pure random override
        if random.random() < 0.3:
            choice = random.choice(["B1", "B2"])
            print(f"   A routing: {choice} (RANDOM OVERRIDE - ignoring scores)")
            return choice
        else:
            choice = "B1" if b1_score > b2_score else "B2"
            print(f"   A routing: {choice} (B1 score: {b1_score:.1f}, B2 score: {b2_score:.1f})")
            return choice
    
    def route_from_b1(state: PathTrackingState) -> str:
        """Route from B1 based on its decision"""
        decision = state["routing_decisions"].get("b1_decision", {})
        return decision.get("target", "C1")
    
    def route_from_b2(state: PathTrackingState) -> str:
        """Route from B2 based on its enhanced decision matrix"""
        decision = state["routing_decisions"].get("b2_decision", {})
        return decision.get("target", "C1")
    
    def route_from_d1(state: PathTrackingState) -> str:
        """Route from D1 - enhanced randomization for variety"""
        d1_result = state["data"].get("d1_result", {})
        entry_data = state["data"]["entry_data"]
        
        # Multiple factors influence the decision
        score_for_c1 = 0
        score_for_e = 0
        
        # Base recommendation from D1 processing
        recommendation = d1_result.get("next_recommendation", "E")
        if recommendation == "C1":
            score_for_c1 += 2
        else:
            score_for_e += 2
        
        # Consider complexity and data characteristics
        if entry_data["complexity"] in ["complex", "very_complex"]:
            score_for_c1 += 1.5
        if entry_data["data_size"] > 800:
            score_for_c1 += 1
        if entry_data["processing_flags"]["requires_validation"] and d1_result.get("validation_performed"):
            score_for_e += 1.5  # Already validated, can go straight to E
        
        # Add randomness for variety
        score_for_c1 += random.uniform(0, 2)
        score_for_e += random.uniform(0, 2)
        
        # 20% chance of pure random decision
        if random.random() < 0.2:
            decision = random.choice(["C1", "E"])
            print(f"   D1 routing: {decision} (RANDOM OVERRIDE)")
            return decision
        
        decision = "C1" if score_for_c1 > score_for_e else "E"
        print(f"   D1 routing: {decision} (C1 score: {score_for_c1:.1f}, E score: {score_for_e:.1f})")
        return decision
    
    def route_from_d2(state: PathTrackingState) -> str:
        """Route from D2 - enhanced variety in routing decisions"""
        d2_result = state["data"].get("d2_result", {})
        entry_data = state["data"]["entry_data"]
        
        # Calculate routing scores
        score_for_c1 = 0
        score_for_e = 0
        
        # Base processing recommendation
        if d2_result.get("needs_additional_processing", False):
            score_for_c1 += 2
        else:
            score_for_e += 2
        
        # Consider complexity and optimization
        complexity_score = d2_result.get("complexity_score", 0)
        if complexity_score > 6:
            score_for_c1 += 1.5
        elif complexity_score < 3:
            score_for_e += 1
        
        # Path length consideration (longer paths might want to wrap up)
        path_length = len(state["execution_path"])
        if path_length >= 4:
            score_for_e += 1  # Encourage ending for longer paths
        else:
            score_for_c1 += 0.5
        
        # Processing optimization factor
        optimization = d2_result.get("optimization", "")
        if "dual_router" in optimization:
            score_for_c1 += 1  # Complex routing suggests more processing
        
        # Add significant randomness
        score_for_c1 += random.uniform(0, 2.5)
        score_for_e += random.uniform(0, 2.5)
        
        # 25% chance of pure random override
        if random.random() < 0.25:
            decision = random.choice(["C1", "E"])
            print(f"   D2 routing: {decision} (RANDOM OVERRIDE)")
            return decision
        
        decision = "C1" if score_for_c1 > score_for_e else "E"
        print(f"   D2 routing: {decision} (C1 score: {score_for_c1:.1f}, E score: {score_for_e:.1f})")
        return decision
    
    # Add conditional routing
    workflow.add_conditional_edges("A", route_from_a, {"B1": "B1", "B2": "B2"})
    workflow.add_conditional_edges("B1", route_from_b1, {"C1": "C1", "C2": "C2", "D1": "D1", "D2": "D2"})
    workflow.add_conditional_edges("B2", route_from_b2, {"C1": "C1", "C2": "C2", "D1": "D1", "D2": "D2"})
    workflow.add_conditional_edges("D1", route_from_d1, {"C1": "C1", "E": "E"})
    workflow.add_conditional_edges("D2", route_from_d2, {"C1": "C1", "E": "E"})
    
    # Simple edges for final convergence
    workflow.add_edge("C1", "E")
    workflow.add_edge("C2", "E")
    workflow.add_edge("E", END)
    
    compiled_workflow = workflow.compile()
    
    # Execute with root tracing
    with tracer.start_as_current_span("path_aware_execution") as root_span:
        root_span.set_attribute("workflow.type", "enhanced_path_introspection_demo")
        
        initial_state = PathTrackingState(
            data={},
            execution_path=[],
            routing_decisions={},
            node_metadata={}
        )
        final_state = await compiled_workflow.ainvoke(initial_state)
        
        print(f"\n📊 Final Analysis:")
        analysis = final_state["data"].get("final_analysis", {})
        print(f"   Execution Path: {' -> '.join(analysis.get('execution_path', []))}")
        print(f"   Routing Decisions: {len(analysis.get('routing_decisions', []))}")
        print(f"   Performance: {analysis.get('total_processing_time', 0):.3f}s")
        print(f"   Path Complexity: {analysis.get('path_efficiency', {}).get('path_complexity', 'unknown')}")
        
        return final_state

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Path-Aware Node Execution Demo')
    parser.add_argument('--jaeger', action='store_true', 
                       help='Send traces to Jaeger (default: console output)')
    parser.add_argument('--jaeger-endpoint', default='http://localhost:14268/api/traces',
                       help='Jaeger collector endpoint (default: http://localhost:14268/api/traces)')
    parser.add_argument('--executions', type=int, default=5,
                       help='Number of workflow executions to run (default: 5)')
    
    args = parser.parse_args()
    
    print("🔍 Enhanced Path-Aware Node Execution Demo")
    print("=" * 60)
    print("Nodes can introspect their execution path and adapt accordingly")
    print("Enhanced with multiple routing points and randomized decision making")
    
    if args.jaeger:
        print(f"📡 Telemetry: Configured for Jaeger at {args.jaeger_endpoint}")
        print("   Make sure Jaeger is running: docker run -p 16686:16686 -p 14268:14268 jaegertracing/all-in-one:latest")
    else:
        print("📝 Telemetry: Using console output")
    
    print(f"🔄 Running {args.executions} workflow executions to demonstrate path variety\n")
    
    # Track unique paths to show variety
    unique_paths = set()
    
    # Run multiple examples to show different paths
    for i in range(args.executions):
        print(f"🔄 Execution #{i+1}")
        print("-" * 35)
        final_state = asyncio.run(run_path_aware_workflow(
            use_jaeger=args.jaeger, 
            jaeger_endpoint=args.jaeger_endpoint
        ))
        
        # Track unique execution paths
        analysis = final_state["data"].get("final_analysis", {})
        path = " -> ".join(analysis.get("execution_path", []))
        unique_paths.add(path)
        
        print("\n" + "="*60 + "\n")
    
    print(f"📈 Execution Summary:")
    print(f"   Total executions: {args.executions}")
    print(f"   Unique paths discovered: {len(unique_paths)}")
    print(f"   Path variety: {len(unique_paths) / args.executions * 100:.1f}%")
    print(f"\n🛤️  Discovered execution paths:")
    for i, path in enumerate(sorted(unique_paths), 1):
        print(f"   {i}. {path}")
        
    if len(unique_paths) < args.executions:
        print(f"\n💡 Some paths were repeated, showing the probabilistic nature of the routing!")
    else:
        print(f"\n🎯 All executions took different paths - excellent randomization!")