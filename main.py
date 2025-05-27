import asyncio
import random
import sys
from typing import Dict, Any, List
from datetime import datetime

from langgraph.graph import StateGraph, END
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

# Import from our modular components
from tracing import traced_node, PathTrackingState, SpanPathExtractor
from telemetry import setup_tracing
from workflow_utils import create_router_function


class PathAwareNodes:
    """Nodes that can introspect their execution path"""
    
    def __init__(self, tracer):
        self.tracer = tracer
    
    @traced_node("A", "entry_point")
    async def node_a(self, state: PathTrackingState) -> PathTrackingState:
        """
        Entry Point Node (A) - Workflow Initialization & Request Processing
        
        PURPOSE:
        - Acts as the primary entry point for all workflow executions
        - Generates randomized request data to simulate diverse workload scenarios
        - Establishes foundational tracing attributes for downstream path analysis
        - Initializes execution path tracking with automatic OpenTelemetry span creation
        
        TRACING BEHAVIOR:
        - @traced_node decorator automatically creates "node_A" span with type "entry_point"
        - Records essential request metadata as span attributes (data.size, data.priority, etc.)
        - Initializes execution_path state tracking for path-aware node behavior
        - Captures request characteristics for routing decision analysis in subsequent nodes
        
        PATH-AWARE FEATURES:
        - First node in execution path - establishes baseline for path-aware adaptations
        - Sets up routing context that influences B1/B2 decision algorithms
        - Creates diverse request profiles that drive different execution paths
        - Enables downstream nodes to adapt behavior based on request characteristics
        
        DATA GENERATION:
        - Creates comprehensive request metadata with randomized attributes
        - Generates processing flags that influence routing and processing strategies
        - Establishes data size, priority, and complexity metrics for adaptive processing
        - Sets up retry mechanisms and processing preferences for fault tolerance
        
        OPENTELEMETRY INTEGRATION:
        - Automatic span creation with structured attributes for request profiling
        - Path tracking initialization for distributed tracing correlation
        - Custom attributes for data characteristics enable performance analysis
        - Provides foundation for end-to-end workflow observability
        """
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
        
        print(f"🚀 Node A: Starting workflow (request: {state['data']['entry_data']['request_type']}, complexity: {state['data']['entry_data']['complexity']})")
        print(f"   Current path: {' -> '.join(state['execution_path'])}")
        # Display A routing decision (access from pre-set routing decision)
        if "a_decision" in state["routing_decisions"]:
            a_decision = state["routing_decisions"]["a_decision"]
            print(f"   A routing: {a_decision['target']} (B1 score: {a_decision['b1_score']:.1f}, B2 score: {a_decision['b2_score']:.1f})")
        
        return state
    
    @traced_node("B1", "alternative_router")
    async def node_b1(self, state: PathTrackingState) -> PathTrackingState:
        """
        Alternative Router Node (B1) - Scoring-Based Route Selection
        
        PURPOSE:
        - Implements alternative routing algorithm using scoring-based decision making
        - Provides path diversity by offering different routing logic than B2
        - Demonstrates path-aware routing where decisions consider request characteristics
        - Acts as one of two possible routing nodes (B1 or B2) from entry point A
        
        TRACING BEHAVIOR:
        - @traced_node decorator creates "node_B1" span with type "alternative_router"
        - Records routing decision, reasoning, and scoring factors as span attributes
        - Captures routing.decision, routing.reason, routing.score, and routing.factors
        - Enables analysis of routing patterns and decision quality across executions
        
        PATH-AWARE FEATURES:
        - Accesses previous routing decisions to display A's routing choice
        - Adapts routing strategy based on request type, complexity, and data characteristics
        - Considers execution path context for routing factor weighting
        - Influences downstream C1/C2 routing based on scoring algorithm results
        
        ROUTING ALGORITHM:
        - Calculates routing scores using _calculate_b1_routing_score method
        - Weighs multiple factors: request_type, complexity, data_type, urgency
        - Applies business logic rules for C1 vs C2 vs D1/D2 routing decisions
        - Generates human-readable routing reasons for observability
        
        OPENTELEMETRY INTEGRATION:
        - Detailed routing decision attributes for workflow analysis
        - Score-based metrics for routing algorithm performance evaluation
        - Factor tracking enables routing optimization and debugging
        - Supports A/B testing of routing strategies through traceable decisions
        """
        # Get A's routing decision to show which was chosen
        a_target = state["routing_decisions"]["a_decision"]["target"]
        print(f"🔀 Node B1: Alternative routing analysis")
        
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
    
    @traced_node("B2", "enhanced_router")
    async def node_b2(self, state: PathTrackingState) -> PathTrackingState:
        """
        Enhanced Router Node (B2) - Matrix-Based Decision Engine
        
        PURPOSE:
        - Implements sophisticated routing using multi-dimensional decision matrix
        - Provides alternative routing path from B1 with different decision criteria
        - Demonstrates advanced path-aware routing with comprehensive scoring
        - Offers more complex routing logic for high-priority/complex requests
        
        TRACING BEHAVIOR:
        - @traced_node decorator creates "node_B2" span with type "enhanced_router"
        - Records complete decision matrix as span attribute for full transparency
        - Captures routing.decision_matrix, routing.decision, and routing.reason
        - Enables detailed analysis of multi-factor routing decisions
        
        PATH-AWARE FEATURES:
        - Analyzes request complexity to determine appropriate downstream processing
        - Considers data size, priority, urgency, and complexity in routing matrix
        - Adapts decision criteria based on execution context and previous path
        - Routes to C1/C2/D1/D2 based on sophisticated scoring algorithms
        
        DECISION MATRIX ALGORITHM:
        - Calculates scores for all possible downstream nodes (C1, C2, D1, D2)
        - Weighs multiple dimensions: data_size, priority, complexity, urgency
        - Applies business rules for optimal resource allocation
        - Selects highest-scoring option with detailed reasoning
        
        OPENTELEMETRY INTEGRATION:
        - Complete decision matrix visibility for routing analysis
        - Multi-dimensional scoring enables optimization of routing algorithms
        - Detailed reasoning supports debugging and performance tuning
        - Facilitates comparison with B1 routing effectiveness
        """
        entry_data = state["data"]["entry_data"]
        
        # Calculate decision matrix scores for all options
        decision_matrix = self._calculate_b2_decision_matrix(entry_data)
        routing_decision, routing_reason = self._make_b2_routing_decision(decision_matrix, entry_data)
        
        # Set routing attributes
        current_span = trace.get_current_span()
        current_span.set_attribute("routing.decision", routing_decision)
        current_span.set_attribute("routing.reason", routing_reason)
        current_span.set_attribute("routing.decision_matrix", str(decision_matrix))
        
        state["routing_decisions"]["b2_decision"] = {
            "target": routing_decision,
            "reason": routing_reason,
            "decision_matrix": decision_matrix,
            "context": {
                "data_size": entry_data["data_size"],
                "priority": entry_data["priority"],
                "complexity": entry_data["complexity"],
                "urgency": entry_data["urgency"]
            }
        }
        
        # Format decision matrix for display
        matrix_display = {k: f"{v:.1f}" for k, v in decision_matrix.items()}
        print(f"   Decision matrix: {matrix_display}")
        print(f"   Routing decision: {routing_decision} (score: {decision_matrix[routing_decision]:.1f}, reason: {routing_reason})")
        
        return state
    
    @traced_node("C1", "adaptive_processor")
    async def node_c1(self, state: PathTrackingState) -> PathTrackingState:
        """
        Adaptive Processor Node (C1) - Path-Aware Strategy Selection
        
        PURPOSE:
        - Demonstrates advanced path-aware processing with dynamic strategy adaptation
        - Changes processing behavior based on complete execution path history
        - Shows how nodes can leverage OpenTelemetry context for intelligent adaptations
        - Implements complex business logic that varies based on routing patterns
        
        TRACING BEHAVIOR:
        - @traced_node decorator creates "node_C1" span with type "adaptive_processor"
        - Records processing.strategy, processing.complexity, and processing.time
        - Captures adaptation_reason showing why specific strategy was chosen
        - Enables analysis of processing efficiency across different execution paths
        
        PATH-AWARE FEATURES:
        - Analyzes complete execution path using SpanPathExtractor utilities
        - Adapts processing strategy based on previous nodes (B1, B2, D1, D2)
        - Implements different complexity levels based on routing path taken
        - Demonstrates how downstream nodes can optimize based on upstream decisions
        
        ADAPTIVE STRATEGIES:
        - multi_branch_convergence: When both B1 and B2 were visited (complex scenario)
        - priority_processing: High-priority requests from B2 enhanced routing
        - standard_processing: Normal processing for moderate complexity requests
        - post_processing_analysis: Special handling when coming from D1/D2
        - direct_b1_processing: Optimized path for B1 alternative routing
        - fallback_processing: Default strategy for unexpected routing patterns
        
        OPENTELEMETRY INTEGRATION:
        - Strategy selection attributes enable processing pattern analysis
        - Processing time metrics support performance optimization
        - Adaptation reasoning provides insights into path-aware decision making
        - Complexity scoring enables workload characterization and resource planning
        """
        # Determine strategy based on how we got here
        previous_path = SpanPathExtractor.get_previous_node_path_from_state(state)
        previous_node = SpanPathExtractor.get_previous_node_from_state(state)
        
        # Adaptive strategy selection
        if "B1" in previous_path and "B2" in previous_path:
            strategy = "multi_branch_convergence"
            complexity = "high"
        elif previous_node == "B2":
            if "routing_decisions" in state and "b2_decision" in state["routing_decisions"]:
                b2_reason = state["routing_decisions"]["b2_decision"]["reason"]
                if "high_priority" in b2_reason or "complex" in b2_reason:
                    strategy = "priority_processing"
                    complexity = "high"
                else:
                    strategy = "standard_processing" 
                    complexity = "medium"
            else:
                strategy = "standard_processing"
                complexity = "medium"
        elif previous_node in ["D1", "D2"]:
            strategy = "post_processing_analysis"
            complexity = "medium"
        elif previous_node == "B1":
            strategy = "direct_b1_processing"
            complexity = "medium"
        else:
            strategy = "fallback_processing"
            complexity = "low"
        
        # Processing time based on complexity
        if complexity == "high":
            processing_time = random.uniform(0.8, 2.0)
        elif complexity == "medium":
            processing_time = random.uniform(0.5, 1.2)
        else:
            processing_time = random.uniform(0.3, 0.8)
        
        await asyncio.sleep(processing_time)
        
        state["data"]["c1_result"] = {
            "processing_strategy": strategy,
            "complexity": complexity,
            "processing_time": processing_time,
            "adaptation_reason": f"prev={previous_node},path_depth={len(previous_path)}",
            "complexity_score": random.randint(80, 100)
        }
        
        current_span = trace.get_current_span()
        current_span.set_attribute("processing.strategy", strategy)
        current_span.set_attribute("processing.complexity", complexity)
        current_span.set_attribute("processing.time", processing_time)
        current_span.set_attribute("processing.adaptation_reason", state["data"]["c1_result"]["adaptation_reason"])
        
        print(f"   Adaptive processing (strategy: {strategy})")
        print(f"   Complexity: {complexity}, Processing time: {processing_time:.3f}s")
        return state
    
    @traced_node("C2", "simple_processor")
    async def node_c2(self, state: PathTrackingState) -> PathTrackingState:
        """
        Simple Processor Node (C2) - Lightweight Processing Strategy
        
        PURPOSE:
        - Provides efficient, lightweight processing for simple requests
        - Demonstrates contrasting approach to C1's adaptive complexity
        - Acts as optimized path for requests not requiring advanced processing
        - Shows how different nodes can implement varying processing philosophies
        
        TRACING BEHAVIOR:
        - @traced_node decorator creates "node_C2" span with type "simple_processor"
        - Records processing.strategy and processing.time for performance analysis
        - Captures efficiency metrics for comparison with C1 adaptive processing
        - Enables identification of optimal routing decisions for simple workloads
        
        PATH-AWARE FEATURES:
        - Inherits path context through traced_node decorator automatically
        - Benefits from execution path tracking without complex path-specific logic
        - Represents optimized processing path for straightforward requests
        - Demonstrates that not all nodes need complex path-aware adaptations
        
        PROCESSING STRATEGY:
        - lightweight_processing: Fast, efficient processing for simple requests
        - Short processing times (0.1-0.5s) for rapid throughput
        - High efficiency scores (90-100) indicating optimal resource utilization
        - Minimal overhead approach for maximum processing speed
        
        OPENTELEMETRY INTEGRATION:
        - Processing time metrics for performance benchmarking
        - Efficiency scoring enables comparison with adaptive processing approaches
        - Strategy attributes support workload characterization
        - Provides baseline for measuring processing optimization benefits
        """
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
    
    @traced_node("D1", "intermediate_processor")
    async def node_d1(self, state: PathTrackingState) -> PathTrackingState:
        """
        Intermediate Processor Node (D1) - Validation & Transformation Pipeline
        
        PURPOSE:
        - Implements intermediate processing for complex workflows requiring validation
        - Demonstrates conditional routing based on processing requirements
        - Acts as preprocessing stage before final convergence or additional processing
        - Shows dynamic workflow branching based on data quality assessment
        
        TRACING BEHAVIOR:
        - @traced_node decorator creates "node_D1" span with type "intermediate_processor"
        - Records processing.strategy and processing.time for pipeline analysis
        - Captures validation results and data quality metrics
        - Enables tracking of intermediate processing effectiveness
        
        PATH-AWARE FEATURES:
        - Inherits complete execution path context through traced_node infrastructure
        - Makes routing decisions (C1 vs E) based on processing results
        - Demonstrates how intermediate nodes can influence final workflow convergence
        - Shows conditional path extension based on data quality assessment
        
        PROCESSING STRATEGIES:
        - intermediate_validation: When additional processing validation is required
        - direct_transformation: When data can be processed without extra validation
        - Dynamic routing to C1 for additional processing or E for final convergence
        - Data quality scoring (75-95) influences downstream routing decisions
        
        CONDITIONAL ROUTING:
        - Calculates C1 vs E scores to determine optimal next step
        - Routes to C1 when additional_processing_required
        - Routes to E for direct convergence when validation passes
        - Sets routing_decisions state for downstream path-aware adaptations
        
        OPENTELEMETRY INTEGRATION:
        - Processing strategy metrics for pipeline optimization
        - Data quality scores enable quality assurance tracking
        - Routing decision attributes support workflow pattern analysis
        - Processing time metrics facilitate performance tuning
        """
        processing_time = random.uniform(0.3, 1.0)
        await asyncio.sleep(processing_time)
        
        # Determine if additional processing is needed
        additional_processing = random.choice([True, False])
        strategy = "intermediate_validation" if additional_processing else "direct_transformation"
        
        # Calculate routing to next node (C1 or E)
        c1_score = random.uniform(2.0, 6.0)
        e_score = random.uniform(1.0, 4.0)
        next_target = "C1" if c1_score > e_score else "E"
        
        state["data"]["d1_result"] = {
            "processing_strategy": strategy,
            "processing_time": processing_time,
            "additional_processing": additional_processing,
            "data_quality_score": random.randint(75, 95)
        }
        
        # Set routing decision if going to C1
        if next_target == "C1":
            state["routing_decisions"]["d1_decision"] = {
                "target": "C1",
                "reason": "additional_processing_required"
            }
        
        current_span = trace.get_current_span()
        current_span.set_attribute("processing.strategy", strategy)
        current_span.set_attribute("processing.time", processing_time)
        
        print(f"   Strategy: {strategy}, Additional processing: {additional_processing}")
        if next_target == "C1":
            print(f"   D1 routing: C1 (C1 score: {c1_score:.1f}, E score: {e_score:.1f})")
        
        return state
    
    @traced_node("D2", "parallel_processor")
    async def node_d2(self, state: PathTrackingState) -> PathTrackingState:
        """
        Parallel Processor Node (D2) - High-Throughput Processing Engine
        
        PURPOSE:
        - Implements parallel processing for large datasets and high-throughput scenarios
        - Demonstrates scalable processing strategies with configurable worker allocation
        - Shows how processing can be optimized for different data characteristics
        - Acts as alternative intermediate processing path to D1 with focus on throughput
        
        TRACING BEHAVIOR:
        - @traced_node decorator creates "node_D2" span with type "parallel_processor"
        - Records processing.strategy, processing.time, and processing.parallel_workers
        - Captures throughput metrics and parallel configuration details
        - Enables analysis of parallel processing efficiency and resource utilization
        
        PATH-AWARE FEATURES:
        - Inherits execution path context for adaptive parallel configuration
        - Makes intelligent routing decisions (C1 vs E) based on processing results
        - Demonstrates how parallel processing can influence workflow convergence patterns
        - Shows conditional path extension for complex parallel-to-serial processing scenarios
        
        PARALLEL PROCESSING STRATEGIES:
        - parallel_streaming: High-throughput streaming for 3+ workers
        - standard_parallel: Standard parallel processing for 2 workers
        - Dynamic worker allocation (2-4 workers) based on workload characteristics
        - Throughput scoring (85-100) indicating processing efficiency
        
        CONDITIONAL ROUTING:
        - Calculates C1 vs E scores for optimal downstream routing
        - Routes to C1 for parallel_to_complex_processing when additional work needed
        - Routes to E for direct convergence when parallel processing completes workflow
        - Balances parallel processing benefits with convergence efficiency
        
        OPENTELEMETRY INTEGRATION:
        - Parallel worker metrics for resource allocation analysis
        - Throughput scoring enables parallel processing optimization
        - Processing strategy attributes support workload characterization
        - Routing decision tracking facilitates parallel workflow pattern analysis
        """
        processing_time = random.uniform(0.2, 0.8)
        await asyncio.sleep(processing_time)
        
        # Determine parallel processing configuration
        parallel_workers = random.randint(2, 4)
        additional_processing = random.choice([True, False])
        strategy = "parallel_streaming" if parallel_workers > 2 else "standard_parallel"
        
        # Calculate routing to next node (C1 or E)
        c1_score = random.uniform(2.0, 6.0)
        e_score = random.uniform(1.0, 4.0)
        next_target = "C1" if c1_score > e_score else "E"
        
        state["data"]["d2_result"] = {
            "processing_strategy": strategy,
            "processing_time": processing_time,
            "parallel_workers": parallel_workers,
            "additional_processing": additional_processing,
            "throughput_score": random.randint(85, 100)
        }
        
        # Set routing decision if going to C1
        if next_target == "C1":
            state["routing_decisions"]["d2_decision"] = {
                "target": "C1",
                "reason": "parallel_to_complex_processing"
            }
        
        current_span = trace.get_current_span()
        current_span.set_attribute("processing.strategy", strategy)
        current_span.set_attribute("processing.time", processing_time)
        current_span.set_attribute("processing.parallel_workers", parallel_workers)
        
        print(f"   Parallel workers: {parallel_workers}, Additional processing needed: {additional_processing}")
        if next_target == "C1":
            print(f"   D2 routing: C1 (C1 score: {c1_score:.1f}, E score: {e_score:.1f})")
        
        return state
    
    @traced_node("E", "convergence_analyzer")
    async def node_e(self, state: PathTrackingState) -> PathTrackingState:
        """
        Convergence Analyzer Node (E) - Final Path Analysis & Result Aggregation
        
        PURPOSE:
        - Acts as final convergence point for all workflow execution paths
        - Demonstrates comprehensive path analysis using complete execution history
        - Aggregates results from all processing nodes for final workflow assessment
        - Shows how OpenTelemetry context enables end-to-end workflow analysis
        
        TRACING BEHAVIOR:
        - @traced_node decorator creates "node_E" span with type "convergence_analyzer"
        - Records aggregation.nodes_count, aggregation.time, and path.efficiency
        - Captures complete execution path analysis for workflow optimization
        - Provides final workflow metrics for performance and pattern analysis
        
        PATH-AWARE FEATURES:
        - Analyzes complete execution path using state["execution_path"]
        - Evaluates path efficiency (optimal ≤4 nodes, suboptimal >4 nodes)
        - Identifies convergence patterns from different processing branches
        - Demonstrates how final nodes can provide comprehensive workflow insights
        
        RESULT AGGREGATION:
        - Collects all *_result data from previous processing nodes
        - Aggregates processing times, strategies, and scores across execution path
        - Calculates final workflow score based on complete processing pipeline
        - Provides comprehensive workflow summary for analysis and optimization
        
        PATH ANALYSIS FEATURES:
        - execution_path: Complete node sequence for pattern analysis
        - path_efficiency: Workflow efficiency assessment based on node count
        - convergence_point: Identifies immediate predecessor for routing analysis
        - total_nodes_processed: Quantitative measure of workflow complexity
        
        OPENTELEMETRY INTEGRATION:
        - Final workflow metrics for end-to-end performance analysis
        - Path efficiency scoring enables workflow optimization
        - Complete execution path tracking supports pattern recognition
        - Aggregated results provide comprehensive workflow observability
        """
        processing_time = random.uniform(0.1, 0.3)
        await asyncio.sleep(processing_time)
        
        # Analyze the complete execution path
        complete_path = state["execution_path"]
        
        # Collect all processing results
        results = {}
        for key in state["data"]:
            if key.endswith("_result"):
                results[key] = state["data"][key]
        
        state["data"]["final_result"] = {
            "aggregation_strategy": "comprehensive_summary",
            "processing_time": processing_time,
            "total_nodes_processed": len(complete_path),
            "processing_results": results,
            "final_score": random.randint(80, 100),
            "path_analysis": {
                "execution_path": complete_path,
                "path_efficiency": "optimal" if len(complete_path) <= 4 else "suboptimal",
                "convergence_point": complete_path[-2] if len(complete_path) > 1 else "direct"
            }
        }
        
        current_span = trace.get_current_span()
        current_span.set_attribute("aggregation.nodes_count", len(complete_path))
        current_span.set_attribute("aggregation.time", processing_time)
        current_span.set_attribute("path.efficiency", state["data"]["final_result"]["path_analysis"]["path_efficiency"])
        
        print(f"   Final convergence")
        complete_path_str = " → ".join(complete_path)
        print(f"   Complete execution path: {complete_path_str}")
        if len(complete_path) > 3:
            print(f"   Path variety achieved: Different routing each run!")
        
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
    
    def _calculate_b2_decision_matrix(self, entry_data: Dict[str, Any]) -> Dict[str, float]:
        """Calculate sophisticated decision matrix for B2 routing"""
        scores = {"C1": 0.0, "C2": 0.0, "D1": 0.0, "D2": 0.0}
        
        # Base scoring by data characteristics
        data_size = entry_data["data_size"]
        priority = entry_data["priority"]
        complexity = entry_data["complexity"]
        urgency = entry_data["urgency"]
        data_type = entry_data["data_type"]
        
        # C1 (Complex processor) scoring
        if complexity in ["complex", "very_complex"]:
            scores["C1"] += 3.0
        if priority == "high":
            scores["C1"] += 2.5
        if data_size > 1000:
            scores["C1"] += 2.0
        if urgency == "critical":
            scores["C1"] += 1.5
        
        # C2 (Simple processor) scoring  
        if complexity in ["simple", "moderate"]:
            scores["C2"] += 3.0
        if data_size < 500:
            scores["C2"] += 2.0
        if priority == "low":
            scores["C2"] += 1.5
        if urgency in ["low", "normal"]:
            scores["C2"] += 1.0
        
        # D1 (Data processor) scoring
        if data_type in ["structured", "mixed"]:
            scores["D1"] += 2.5
        if complexity == "moderate":
            scores["D1"] += 2.0
        if 500 <= data_size <= 1000:
            scores["D1"] += 1.5
        if entry_data["processing_flags"]["requires_validation"]:
            scores["D1"] += 1.0
        
        # D2 (Stream processor) scoring
        if data_type == "streaming":
            scores["D2"] += 4.0
        if entry_data["processing_flags"]["enable_parallel"]:
            scores["D2"] += 2.0
        if data_size > 800:
            scores["D2"] += 1.5
        if urgency in ["high", "critical"]:
            scores["D2"] += 1.0
        
        # Add randomness to each score
        for key in scores:
            scores[key] += random.uniform(0, 3.0)
        
        return scores
    
    def _make_b2_routing_decision(self, decision_matrix: Dict[str, float], entry_data: Dict[str, Any]) -> tuple:
        """Make routing decision based on decision matrix with override possibilities"""
        
        # 30% chance of random override (as mentioned in README)
        if random.random() < 0.3:
            override_target = random.choice(list(decision_matrix.keys()))
            return override_target, "random_override"
        
        # Find highest scoring option
        best_target = max(decision_matrix.items(), key=lambda x: x[1])
        target_node = best_target[0]
        
        # Determine reason based on characteristics
        if target_node == "C1":
            if entry_data["priority"] == "high":
                reason = "high_priority_complex"
            elif entry_data["complexity"] in ["complex", "very_complex"]:
                reason = "complexity_routing"
            else:
                reason = "best_score_c1"
        elif target_node == "C2":
            if entry_data["complexity"] in ["simple", "moderate"]:
                reason = "simple_processing"
            elif entry_data["data_size"] < 500:
                reason = "small_data_optimization"
            else:
                reason = "best_score_c2"
        elif target_node == "D1":
            if entry_data["data_type"] in ["structured", "mixed"]:
                reason = "structured_data_processing"
            else:
                reason = "best_score_d1"
        else:  # D2
            if entry_data["data_type"] == "streaming":
                reason = "streaming_specialized"
            elif entry_data["processing_flags"]["enable_parallel"]:
                reason = "parallel_processing"
            else:
                reason = "best_score_d2"
        
        return target_node, reason


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
            # Calculate B1 and B2 scores for A routing decision
            b1_score = random.uniform(3.0, 8.0)
            b2_score = random.uniform(3.0, 8.0)
            
            # Choose based on scores (with some randomness)
            a_target = "B2" if b2_score > b1_score else "B1"
            
            # 25% chance to override for demonstration
            if random.random() < 0.25:
                a_target = random.choice(["B1", "B2"])
            
            initial_state["routing_decisions"]["a_decision"] = {
                "target": a_target,
                "b1_score": b1_score,
                "b2_score": b2_score
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
    
    workflow.add_conditional_edges(
        "D1",
        create_router_function("d1_decision", ["C1", "E"]),
        {"C1": "C1", "E": "E"}
    )
    
    workflow.add_conditional_edges(
        "D2", 
        create_router_function("d2_decision", ["C1", "E"]),
        {"C1": "C1", "E": "E"}
    )
    
    # C1 and C2 go to E
    workflow.add_edge("C1", "E")
    workflow.add_edge("C2", "E")
    
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
