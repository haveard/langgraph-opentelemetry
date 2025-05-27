"""
OpenTelemetry Configuration and Utilities Module

This module provides OpenTelemetry setup, configuration, and utility functions
for path-aware workflow tracing. It handles tracer provider configuration,
exporter setup, and connection testing.

Key Components:
- setup_tracing: Main function to configure OpenTelemetry with various exporters
- test_otlp_connection: Utility to test OTLP collector availability
- Robust error handling and fallback mechanisms
- Support for console and OTLP exporters

Usage:
    from telemetry import setup_tracing
    
    # Basic setup with console output
    tracer = setup_tracing(enable_console=True)
    
    # Setup with OTLP export to Jaeger
    tracer = setup_tracing(enable_otlp=True)
    
    # Setup with both exporters
    tracer = setup_tracing(enable_console=True, enable_otlp=True)
"""

import socket
import urllib.parse
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, ConsoleSpanExporter
from opentelemetry.sdk.resources import Resource


def test_otlp_connection(endpoint: str = "http://localhost:4317", timeout: int = 2) -> bool:
    """
    Test if an OTLP collector is available at the given endpoint.
    
    This function attempts to establish a TCP connection to the OTLP collector
    endpoint to verify availability before setting up the exporter.
    
    Args:
        endpoint (str): OTLP collector endpoint URL. Default: "http://localhost:4317"
        timeout (int): Connection timeout in seconds. Default: 2
        
    Returns:
        bool: True if collector is reachable, False otherwise
        
    Example:
        if test_otlp_connection():
            print("Jaeger is running")
        else:
            print("No OTLP collector found")
    """
    try:
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


def setup_tracing(enable_console: bool = False, enable_otlp: bool = False, 
                 service_name: str = "langgraph-workflow", 
                 otlp_endpoint: str = "http://localhost:4317") -> trace.Tracer:
    """
    Setup OpenTelemetry tracing with configurable exporters and robust error handling.
    
    This function configures the OpenTelemetry tracer provider with the requested
    exporters. It provides intelligent fallbacks and clear user feedback about
    the tracing configuration.
    
    Args:
        enable_console (bool): Enable console span output for debugging. Default: False
        enable_otlp (bool): Enable OTLP exporter for external collectors. Default: False
        service_name (str): Service name for resource identification. Default: "langgraph-workflow"
        otlp_endpoint (str): OTLP collector endpoint URL. Default: "http://localhost:4317"
        
    Returns:
        trace.Tracer: Configured OpenTelemetry tracer instance
        
    Behavior:
        - If no exporters are enabled, spans are recorded internally but not exported
        - OTLP exporter automatically tests collector availability and falls back gracefully
        - Console exporter provides immediate span visibility for debugging
        - Multiple exporters can be enabled simultaneously
        - Provides clear user feedback about configuration status
        
    Example:
        # Minimal setup for production (internal recording only)
        tracer = setup_tracing()
        
        # Development setup with console output
        tracer = setup_tracing(enable_console=True)
        
        # Production setup with Jaeger
        tracer = setup_tracing(enable_otlp=True)
        
        # Full debugging setup
        tracer = setup_tracing(enable_console=True, enable_otlp=True)
    """
    # Configure the tracer provider with service identification
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)
    
    # Track successful exporter configurations
    exporters_configured = 0
    
    # Setup OTLP exporter with availability testing and fallback
    if enable_otlp:
        try:
            # Test collector availability before configuring exporter
            if test_otlp_connection(otlp_endpoint):
                # Dynamic import to handle optional dependency
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
                
                # Create exporter with reasonable timeout
                otlp_exporter = OTLPSpanExporter(
                    endpoint=otlp_endpoint,
                    insecure=True,
                    timeout=3  # 3-second timeout for export attempts
                )
                provider.add_span_processor(SimpleSpanProcessor(otlp_exporter))
                print(f"📡 OTLP tracing enabled - sending to {otlp_endpoint}")
                exporters_configured += 1
            else:
                print(f"⚠️  OTLP collector not available at {otlp_endpoint}")
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
    
    # Setup console exporter for debugging or as fallback
    if enable_console:
        console_exporter = ConsoleSpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(console_exporter))
        print("📝 Console tracing enabled")
        exporters_configured += 1
    
    # Provide feedback about final configuration
    if exporters_configured == 0:
        print("🔇 Tracing configured but no exporters enabled")
        print("💡 Use enable_console=True for console output or enable_otlp=True for OTLP export")
        print("📊 Spans will be recorded internally but not exported")
    
    return trace.get_tracer(service_name)


def get_tracer(service_name: str = "langgraph-workflow") -> trace.Tracer:
    """
    Get a tracer instance from the current tracer provider.
    
    This is a convenience function for getting a tracer after setup_tracing()
    has been called, or for getting additional tracers with different names.
    
    Args:
        service_name (str): Name for the tracer instance. Default: "langgraph-workflow"
        
    Returns:
        trace.Tracer: Tracer instance
        
    Example:
        # After setup_tracing() has been called
        tracer = get_tracer("my-workflow-component")
    """
    return trace.get_tracer(service_name)


def is_tracing_enabled() -> bool:
    """
    Check if OpenTelemetry tracing is currently enabled.
    
    Returns:
        bool: True if a tracer provider is configured, False otherwise
    """
    try:
        provider = trace.get_tracer_provider()
        return provider is not None and hasattr(provider, 'get_tracer')
    except Exception:
        return False


def create_manual_span(tracer: trace.Tracer, span_name: str, attributes: Optional[dict] = None):
    """
    Create a manual span for custom tracing outside of the @traced_node decorator.
    
    This is useful for tracing utility functions, setup code, or other operations
    that don't fit the node pattern.
    
    Args:
        tracer: OpenTelemetry tracer instance
        span_name: Name for the span
        attributes: Optional dictionary of span attributes
        
    Returns:
        Span context manager
        
    Example:
        with create_manual_span(tracer, "database_connection", {"db.type": "postgres"}):
            # Database connection code here
            pass
    """
    span = tracer.start_span(span_name)
    if attributes:
        for key, value in attributes.items():
            span.set_attribute(key, value)
    return span


# Export commonly used components for convenience
__all__ = [
    'setup_tracing',
    'get_tracer', 
    'test_otlp_connection',
    'is_tracing_enabled',
    'create_manual_span'
]
