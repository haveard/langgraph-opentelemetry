# LangGraph Path-Aware Workflow Demo

A demonstration of intelligent workflow nodes that can see their execution path and adapt their behavior accordingly. Built with LangGraph and OpenTelemetry.

## What This Does

This project shows how to create workflow nodes that are "path-aware" - they can:
- **See where they came from** in the workflow
- **Adapt their behavior** based on their execution path
- **Make smarter routing decisions** using context
- **Generate rich telemetry** for monitoring

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run the demo
python main.py
```

You'll see multiple workflow executions with varied paths like:
- `A → B1 → C1 → E`
- `A → B2 → C2 → E` 
- `A → B2 → D2 → C1 → E`

## How It Works

The workflow creates **randomized execution paths** through these nodes:

- **Node A**: Entry point that generates random data and parameters
- **Node B1/B2**: Alternative routing nodes with sophisticated decision logic  
- **Node C1**: Adaptive processor that changes strategy based on how it was reached
- **Node C2**: Lightweight processor for simple cases
- **Node D1/D2**: Intermediate processing nodes for complex workflows
- **Node E**: Final analysis node that examines the complete execution path

## Key Features

### Enhanced Randomization
- **30% random override** at initial routing decisions
- **Multiple decision factors**: complexity, data size, priority, processing flags
- **Probabilistic routing** with scoring systems at each decision point
- **80% path variety** achieved in testing runs

### Path Introspection
Each node can examine:
- Complete execution path taken to reach it
- Previous node and routing context
- Decision history and reasoning
- Performance metrics from earlier nodes

### OpenTelemetry Integration
- Distributed tracing with detailed span attributes
- Console output for debugging
- Optional Jaeger integration for visual trace analysis
- Rich telemetry data for monitoring and optimization

## Example Output

```
🔄 Execution #1
🚀 Node A: Starting workflow (request: analytics, complexity: complex)
   A routing: B2 (B1 score: 4.9, B2 score: 7.5)
🔀 Node B2: Enhanced routing analysis
   Decision matrix: {'C1': 9.2, 'C2': 3.8, 'D1': 3.9, 'D2': 7.9}
   Routing decision: D2 (score: 9.2, reason: random_override)
⚡ Node D2: Parallel processing for large datasets
   Parallel workers: 3, Additional processing needed: True
   D2 routing: C1 (C1 score: 4.3, E score: 1.5)
🧠 Node C1: Adaptive processing (strategy: fallback_processing)
🎯 Node E: Final convergence
   Complete execution path: A → B2 → D2 → C1 → E
   Path variety achieved: Different routing each run!
```

## Why This Matters

Traditional workflows treat each step in isolation. This demo shows how **context-aware nodes** can:

1. **Optimize performance** - Choose different algorithms based on how data arrived
2. **Enable smart routing** - Make decisions using full workflow context  
3. **Improve debugging** - Trace exactly how data flowed through the system
4. **Support A/B testing** - Route different paths and measure outcomes
5. **Build resilient systems** - Implement fallbacks based on execution history

## Optional: Visual Tracing with Jaeger

For advanced users who want visual trace analysis:

```bash
# Start Jaeger (requires Docker)
docker run -d --name jaeger -p 16686:16686 -p 14268:14268 jaegertracing/all-in-one:latest

# Run with Jaeger tracing
python main.py --jaeger

# View traces at http://localhost:16686
```

## Requirements

- Python 3.8+
- Dependencies in `requirements.txt` (LangGraph, OpenTelemetry, etc.)
- Optional: Docker for Jaeger tracing

## Learn More

Check out the other files for deeper dives:
- `PATH_TRACKING_GUIDE.md` - Detailed telemetry documentation
- `JAEGER_SETUP.md` - Complete Jaeger setup guide  
- `QUICK_REFERENCE.md` - Commands and troubleshooting
