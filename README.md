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

# Run the demo (default: 3 executions, no trace export)
python main.py

# Run with more executions to see path variety
python main.py --runs 5

# Enable console output for detailed telemetry
python main.py --console --runs 3

# Enable OTLP export (requires collector like Jaeger)
python main.py --otlp --runs 3
```

You'll see multiple workflow executions with varied paths like:
- `A → B1 → C1 → E`
- `A → B2 → C2 → E` 
- `A → B2 → D2 → E`
- `A → B1 → D1 → E`

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

## Command Line Options

```bash
# Basic usage
python main.py                    # 3 runs, no trace export
python main.py --runs 5           # 5 runs, no trace export

# Tracing options
python main.py --console          # Enable console trace output
python main.py --otlp             # Enable OTLP export (requires collector)

# Combined examples
python main.py --console --runs 2 # Console tracing with 2 runs
python main.py --otlp --runs 10   # OTLP export with 10 runs
```

### Tracing Modes

- **Default (no flags)**: Spans recorded internally, no export, clean output
- **`--console`**: Detailed JSON span output in terminal for debugging
- **`--otlp`**: Export to OTLP collector (automatically falls back to console if no collector)

## Optional: Visual Tracing with Jaeger

For advanced users who want visual trace analysis:

```bash
# Start Jaeger (requires Docker)
docker run -d --name jaeger \
  -p 4317:4317 \
  -p 16686:16686 \
  -p 14268:14268 \
  -p 6831:6831/udp \
  jaegertracing/all-in-one:latest

# Run with OTLP tracing
python main.py --otlp

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
