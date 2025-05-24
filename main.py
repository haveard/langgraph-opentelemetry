import asyncio
import random
import sys
import functools
from typing import Callable, Dict, Any, List, Optional
from datetime import datetime

from langgraph.graph import StateGraph, END
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider, ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Status, StatusCode
from typing_extensions import TypedDict

def traced_node(node_name: str, node_type: str = "processor"):
    """Decorator to automatically handle tracing for workflow nodes"""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(self, state: PathTrackingState) -> PathTrackingState:
            with self.tracer.start_as_current_span(f"node_{node_name}") as span:
                try:
                    # Standard tracing setup
                    span.set_attribute("node.name", node_name)
                    span.set_attribute("node.type", node_type)
                    span.set_attribute("node.timestamp", datetime.now().isoformat())
                    
                    # Path tracking
                    previous_node = SpanPathExtractor.get_previous_node_from_state(state)
                    previous_path = SpanPathExtractor.get_previous_node_path_from_state(state)
                    
                    span.set_attribute("path.previous_node", previous_node or "none")
                    span.set_attribute("path.previous_path", " -> ".join(previous_path) if previous_path else "none")
                    span.set_attribute("path.current_depth", len(state["execution_path"]))
                    
                    # Update execution path
                    state["execution_path"].append(node_name)
                    span.set_attribute("path.full_path", " -> ".join(state["execution_path"]))
                    
                    # Print standard info
                    print(f"🚀 Node {node_name}: Starting workflow" if node_name == "A" else f"🔧 Node {node_name}: {node_type}")
                    print(f"   Current path: {' -> '.join(state['execution_path'])}")
                    if previous_node:
                        print(f"   Previous node: {previous_node}")
                    
                    # Call the actual function
                    result = await func(self, state)
                    
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, description=str(e)))
                    span.record_exception(e)
                    raise
        return wrapper
    return decorator

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
    
    @traced_node("A", "entry_point")
    async def node_a(self, state: PathTrackingState) -> PathTrackingState:
        """Entry point node with enhanced randomization"""
        # Just the business logic - tracing is handled by decorator
        request_types = ["data_processing", "analytics", "validation", "transformation", "aggregation"]
        complexity_levels = ["simple", "moderate", "complex", "very_complex"]
        urgency_levels = ["low", "normal", "high", "critical"]
        data_types = ["structured", "unstructured", "mixed", "streaming"]
        
        state["data"]["entry_data"] = {
            "request_id": f"req_{random.randint(1000, 9999)}",
            "data_size": random.randint(50, 2000),
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
        
        # Add custom attributes for this specific node
        current_span = trace.get_current_span()
        current_span.set_attribute("data.size", state["data"]["entry_data"]["data_size"])
        current_span.set_attribute("data.priority", state["data"]["entry_data"]["priority"])
        current_span.set_attribute("data.type", state["data"]["entry_data"]["data_type"])
        current_span.set_attribute("data.complexity", state["data"]["entry_data"]["complexity"])
        
        print(f"   Request type: {state['data']['entry_data']['request_type']}")
        print(f"   Complexity: {state['data']['entry_data']['complexity']}")
        print(f"   Data size: {state['data']['entry_data']['data_size']}")
        
        return state
    
    @traced_node("B1", "alternative_router")
    async def node_b1(self, state: PathTrackingState) -> PathTrackingState:
        """Alternative routing node with different decision logic"""
        # Business logic only
        entry_data = state["data"]["entry_data"]
        routing_score = self._calculate_b1_routing_score(entry_data)
        routing_decision, routing_reason, routing_factors = self._make_b1_routing_decision(routing_score, entry_data)
        
        # Set routing attributes
        current_span = trace.get_current_span()
        current_span.set_attribute("routing.decision", routing_decision)
        current_span.set_attribute("routing.reason", routing_reason)
        current_span.set_attribute("routing.score", int(routing_score * 100))
        current_span.set_attribute("routing.factors", ",".join(routing_factors))
        
        state["routing_decisions"]["b1_decision"] = {
            "target": routing_decision,
            "reason": routing_reason,
            "score": routing_score,
            "factors": routing_factors,
            "context": {
                "request_type": entry_data["request_type"],
                "complexity": entry_data["complexity"],
                "data_type": entry_data["data_type"],
                "path_length": len(state["execution_path"])
            }
        }
        
        print(f"   Routing decision: {routing_decision} (score: {routing_score:.1f})")
        print(f"   Reason: {routing_reason}")
        if routing_factors:
            print(f"   Factors: {', '.join(routing_factors)}")
        
        return state
    
    @traced_node("B2", "alternative_router")
    async def node_b2(self, state: PathTrackingState) -> PathTrackingState:
        """Second alternative routing node"""
        entry_data = state["data"]["entry_data"]
        
        # Different routing logic than B1
        if entry_data["data_type"] == "streaming":
            routing_decision = "D2"
            routing_reason = "streaming_specialized"
        elif entry_data["urgency"] == "critical":
            routing_decision = "C1"
            routing_reason = "critical_priority"
        elif entry_data["complexity"] in ["simple", "moderate"]:
            routing_decision = "C2"
            routing_reason = "simple_processing"
        else:
            routing_decision = "D1"
            routing_reason = "default_complex"
        
        current_span = trace.get_current_span()
        current_span.set_attribute("routing.decision", routing_decision)
        current_span.set_attribute("routing.reason", routing_reason)
        
        state["routing_decisions"]["b2_decision"] = {
            "target": routing_decision,
            "reason": routing_reason
        }
        
        print(f"   Routing decision: {routing_decision}")
        print(f"   Reason: {routing_reason}")
        return state
    
    @traced_node("C1", "complex_processor")
    async def node_c1(self, state: PathTrackingState) -> PathTrackingState:
        """Complex processing node"""
        processing_time = random.uniform(0.5, 2.0)
        await asyncio.sleep(processing_time)
        
        strategy = "advanced_analytics" if state["data"]["entry_data"]["complexity"] == "very_complex" else "standard_complex"
        
        state["data"]["c1_result"] = {
            "processing_strategy": strategy,
            "processing_time": processing_time,
            "complexity_score": random.randint(80, 100)
        }
        
        current_span = trace.get_current_span()
        current_span.set_attribute("processing.strategy", strategy)
        current_span.set_attribute("processing.time", processing_time)
        
        print(f"   Processing strategy: {strategy}")
        print(f"   Processing time: {processing_time:.3f}s")
        return state
    
    @traced_node("C2", "simple_processor")
    async def node_c2(self, state: PathTrackingState) -> PathTrackingState:
        """Simple processing node"""
        processing_time = random.uniform(0.1, 0.5)
        await asyncio.sleep(processing_time)
        
        strategy = "lightweight_processing"
        
        state["data"]["c2_result"] = {
            "processing_strategy": strategy,
            "processing_time": processing_time,
            "efficiency_score": random.randint(90, 100)
        }
        
        current_span = trace.get_current_span()
        current_span.set_attribute("processing.strategy", strategy)
        current_span.set_attribute("processing.time", processing_time)
        
        print(f"   Processing strategy: {strategy}")
        print(f"   Processing time: {processing_time:.3f}s")
        return state
    
    @traced_node("D1", "data_processor")
    async def node_d1(self, state: PathTrackingState) -> PathTrackingState:
        """Data processing node"""
        processing_time = random.uniform(0.3, 1.0)
        await asyncio.sleep(processing_time)
        
        strategy = "data_transformation"
        
        state["data"]["d1_result"] = {
            "processing_strategy": strategy,
            "processing_time": processing_time,
            "data_quality_score": random.randint(75, 95)
        }
        
        current_span = trace.get_current_span()
        current_span.set_attribute("processing.strategy", strategy)
        current_span.set_attribute("processing.time", processing_time)
        
        print(f"   Processing strategy: {strategy}")
        print(f"   Processing time: {processing_time:.3f}s")
        return state
    
    @traced_node("D2", "stream_processor")
    async def node_d2(self, state: PathTrackingState) -> PathTrackingState:
        """Stream processing node"""
        processing_time = random.uniform(0.2, 0.8)
        await asyncio.sleep(processing_time)
        
        strategy = "stream_processing"
        
        state["data"]["d2_result"] = {
            "processing_strategy": strategy,
            "processing_time": processing_time,
            "throughput_score": random.randint(85, 100)
        }
        
        current_span = trace.get_current_span()
        current_span.set_attribute("processing.strategy", strategy)
        current_span.set_attribute("processing.time", processing_time)
        
        print(f"   Processing strategy: {strategy}")
        print(f"   Processing time: {processing_time:.3f}s")
        return state
    
    @traced_node("E", "aggregator")
    async def node_e(self, state: PathTrackingState) -> PathTrackingState:
        """Final aggregation node"""
        processing_time = random.uniform(0.1, 0.3)
        await asyncio.sleep(processing_time)
        
        # Collect all processing results
        results = {}
        for key in state["data"]:
            if key.endswith("_result"):
                results[key] = state["data"][key]
        
        state["data"]["final_result"] = {
            "aggregation_strategy": "comprehensive_summary",
            "processing_time": processing_time,
            "total_nodes_processed": len(state["execution_path"]),
            "processing_results": results,
            "final_score": random.randint(80, 100)
        }
        
        current_span = trace.get_current_span()
        current_span.set_attribute("aggregation.nodes_count", len(state["execution_path"]))
        current_span.set_attribute("aggregation.time", processing_time)
        
        print(f"   Processed {len(state['execution_path'])} nodes in {processing_time:.3f}s")
        print(f"   Final score: {state['data']['final_result']['final_score']}")
        return state
    
    def _calculate_b1_routing_score(self, entry_data: Dict[str, Any]) -> float:
        """Extract routing score calculation logic"""
        routing_score = 0
        
        # Request type scoring
        if entry_data["request_type"] in ["analytics", "aggregation"]:
            routing_score += 2
        elif entry_data["request_type"] in ["validation", "transformation"]:
            routing_score += 1
        
        # Complexity scoring
        if entry_data["complexity"] in ["complex", "very_complex"]:
            routing_score += 1.5
        elif entry_data["complexity"] in ["simple", "moderate"]:
            routing_score += 1
        
        # Add randomness
        routing_score += random.uniform(0, 4)
        
        return routing_score
    
    def _make_b1_routing_decision(self, routing_score: float, entry_data: Dict[str, Any]) -> tuple:
        """Extract routing decision logic"""
        routing_factors = []
        
        if routing_score >= 6:
            routing_decision = "C1"
            routing_reason = "high_complexity_processing"
        elif routing_score >= 4:
            routing_decision = "D1"
            routing_reason = "intermediate_processing"
        elif routing_score >= 2:
            routing_decision = "C2"
            routing_reason = "lightweight_processing"
        else:
            routing_decision = random.choice(["C2", "D2"])
            routing_reason = "fallback_processing"
        
        # Random override
        if random.random() < 0.2:
            routing_decision = random.choice(["C1", "C2", "D1", "D2"])
            routing_reason = "pure_random_override"
            routing_factors.append("random_override_applied")
        
        return routing_decision, routing_reason, routing_factors


def test_otlp_connection(endpoint: str = "http://localhost:4317", timeout: int = 2) -> bool:
    """Test if an OTLP collector is available at the given endpoint"""
    try:
        import socket
        import urllib.parse
        
        parsed = urllib.parse.urlparse(endpoint)
        host = parsed.hostname or "localhost"
        port = parsed.port or 4317
        
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


def setup_tracing(enable_console=False, enable_otlp=False):
    """Setup OpenTelemetry tracing with robust error handling"""
    # Configure the tracer provider
    resource = Resource.create({"service.name": "langgraph-workflow"})
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)
    
    # Track if any exporter was successfully configured
    exporters_configured = 0
    
    # Setup OTLP exporter only if explicitly requested
    if enable_otlp:
        try:
            # First check if the OTLP package is available
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            
            # Test if collector is available before setting up exporter
            if test_otlp_connection():
                # Create the exporter with a reasonable timeout
                otlp_exporter = OTLPSpanExporter(
                    endpoint="http://localhost:4317",  # Default OTLP gRPC endpoint
                    insecure=True,
                    timeout=3  # 3-second timeout for export attempts
                )
                provider.add_span_processor(SimpleSpanProcessor(otlp_exporter))
                print("📡 OTLP tracing enabled - sending to localhost:4317")
                exporters_configured += 1
            else:
                print("⚠️  OTLP collector not available at localhost:4317")
                print("💡 Start an OTLP collector (like Jaeger) or try:")
                print("   docker run -d -p 4317:4317 -p 16686:16686 jaegertracing/all-in-one")
                print("📝 Falling back to console-only tracing")
                enable_console = True
                
        except ImportError:
            print("⚠️  OTLP exporter not installed. Install with:")
            print("   pip install opentelemetry-exporter-otlp-proto-grpc")
            print("📝 Falling back to console-only tracing")
            enable_console = True
        except Exception as e:
            print(f"⚠️  OTLP exporter setup failed: {e}")
            print("📝 Falling back to console-only tracing")
            enable_console = True
    
    # Add console exporter for debugging or as fallback
    if enable_console:
        console_exporter = ConsoleSpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(console_exporter))
        print("📝 Console tracing enabled")
        exporters_configured += 1
    
    # If no exporters requested or configured, use a minimal setup
    if exporters_configured == 0:
        print("🔇 Tracing configured but no exporters enabled")
        print("💡 Use --console for console output or --otlp for OTLP export")
        print("📊 Spans will be recorded internally but not exported")
    
    return trace.get_tracer(__name__)


def create_router_function(from_node: str, options: List[str]):
    """Create a router function for the workflow"""
    def router(state: PathTrackingState) -> str:
        if from_node in state["routing_decisions"]:
            return state["routing_decisions"][from_node]["target"]
        return options[0]  # Default fallback
    return router


async def run_single_execution(tracer, app, execution_num: int):
    """Run a single workflow execution"""
    print(f"\n🔄 Execution #{execution_num}")
    
    # Create fresh state for each execution
    initial_state: PathTrackingState = {
        "data": {},
        "execution_path": [],
        "routing_decisions": {},
        "node_metadata": {}
    }
    
    # Execute the workflow
    with tracer.start_as_current_span(f"workflow_execution_{execution_num}") as main_span:
        main_span.set_attribute("workflow.name", "langgraph_demo")
        main_span.set_attribute("workflow.execution_number", execution_num)
        main_span.set_attribute("workflow.start_time", datetime.now().isoformat())
        
        try:
            # Add routing decision for A -> B1/B2
            initial_state["routing_decisions"]["a_decision"] = {
                "target": random.choice(["B1", "B2"])
            }
            
            result = await app.ainvoke(initial_state)
            
            main_span.set_attribute("workflow.status", "completed")
            main_span.set_attribute("workflow.final_path", " -> ".join(result["execution_path"]))
            main_span.set_attribute("workflow.nodes_executed", len(result["execution_path"]))
            
            # Summary output
            final_path = " → ".join(result["execution_path"])
            print(f"🎯 Complete execution path: {final_path}")
            
            if "final_result" in result["data"]:
                final_score = result["data"]["final_result"]["final_score"]
                print(f"   Final score: {final_score}")
            
            return result["execution_path"]
            
        except Exception as e:
            main_span.set_status(Status(StatusCode.ERROR, description=str(e)))
            main_span.record_exception(e)
            print(f"❌ Execution #{execution_num} failed: {e}")
            raise


async def main():
    """Main execution function with multiple runs"""
    print("🎯 LangGraph Path-Aware Workflow Demo")
    print("=" * 60)
    
    # Setup tracing (console output disabled for cleaner demo)
    enable_console = "--verbose" in sys.argv or "--console" in sys.argv
    enable_otlp = "--otlp" in sys.argv
    tracer = setup_tracing(enable_console=enable_console, enable_otlp=enable_otlp)
    nodes = PathAwareNodes(tracer)
    
    # Build the workflow graph
    workflow = StateGraph(PathTrackingState)
    
    # Add nodes
    workflow.add_node("A", nodes.node_a)
    workflow.add_node("B1", nodes.node_b1)
    workflow.add_node("B2", nodes.node_b2)
    workflow.add_node("C1", nodes.node_c1)
    workflow.add_node("C2", nodes.node_c2)
    workflow.add_node("D1", nodes.node_d1)
    workflow.add_node("D2", nodes.node_d2)
    workflow.add_node("E", nodes.node_e)
    
    # Set entry point
    workflow.set_entry_point("A")
    
    # Add edges
    workflow.add_conditional_edges(
        "A",
        create_router_function("a_decision", ["B1", "B2"]),
        {"B1": "B1", "B2": "B2"}
    )
    
    workflow.add_conditional_edges(
        "B1",
        create_router_function("b1_decision", ["C1", "C2", "D1", "D2"]),
        {"C1": "C1", "C2": "C2", "D1": "D1", "D2": "D2"}
    )
    
    workflow.add_conditional_edges(
        "B2",
        create_router_function("b2_decision", ["C1", "C2", "D1", "D2"]),
        {"C1": "C1", "C2": "C2", "D1": "D1", "D2": "D2"}
    )
    
    # All processing nodes go to E
    workflow.add_edge("C1", "E")
    workflow.add_edge("C2", "E")
    workflow.add_edge("D1", "E")
    workflow.add_edge("D2", "E")
    
    # E goes to END
    workflow.add_edge("E", END)
    
    # Compile the workflow
    app = workflow.compile()
    
    print("🏗️  Workflow graph compiled successfully")
    
    # Run multiple executions
    execution_paths = []
    
    # Check for command line argument for number of runs
    num_runs = 3
    if "--runs" in sys.argv:
        try:
            runs_idx = sys.argv.index("--runs")
            if runs_idx + 1 < len(sys.argv):
                num_runs = int(sys.argv[runs_idx + 1])
        except (ValueError, IndexError):
            print("Invalid --runs argument, using default of 3")
    
    print(f"🚀 Running {num_runs} executions to demonstrate path variety...\n")
    
    for i in range(1, num_runs + 1):
        try:
            path = await run_single_execution(tracer, app, i)
            execution_paths.append(path)
            
            # Add a small delay between executions for readability
            await asyncio.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Execution {i} failed: {e}")
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Execution Summary")
    print("=" * 60)
    
    unique_paths = set(" → ".join(path) for path in execution_paths)
    
    print(f"✅ Completed executions: {len(execution_paths)}")
    print(f"🔀 Unique paths discovered: {len(unique_paths)}")
    print(f"📈 Path variety: {len(unique_paths)/len(execution_paths)*100:.0f}%")
    
    print("\n🛤️  All execution paths:")
    for i, path in enumerate(execution_paths, 1):
        path_str = " → ".join(path)
        print(f"   #{i}: {path_str}")
    
    if len(unique_paths) > 1:
        print("\n🎯 Path variety achieved: Different routing each run!")


if __name__ == "__main__":
    asyncio.run(main())
