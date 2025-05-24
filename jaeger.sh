#!/bin/bash

# Jaeger Management Script for Path-Aware Workflow Demo

show_help() {
    echo "Jaeger Management for Path-Aware Workflow Demo"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  start     Start Jaeger container"
    echo "  stop      Stop Jaeger container"
    echo "  restart   Restart Jaeger container"
    echo "  status    Show Jaeger container status"
    echo "  clean     Stop and remove Jaeger container"
    echo "  logs      Show Jaeger container logs"
    echo "  ui        Open Jaeger UI in browser"
    echo "  test      Test Jaeger connectivity"
    echo "  help      Show this help message"
    echo ""
    echo "Ports:"
    echo "  16686     Jaeger UI"
    echo "  14268     Jaeger HTTP collector"
    echo "  6831      Jaeger UDP agent"
}

start_jaeger() {
    echo "🚀 Starting Jaeger..."
    docker run -d --name jaeger-demo \
        -p 16686:16686 \
        -p 14268:14268 \
        -p 6831:6831/udp \
        jaegertracing/all-in-one:latest
    
    if [ $? -eq 0 ]; then
        echo "✅ Jaeger started successfully"
        echo "📱 UI available at: http://localhost:16686"
    else
        echo "❌ Failed to start Jaeger"
    fi
}

stop_jaeger() {
    echo "🛑 Stopping Jaeger..."
    docker stop jaeger-demo
    if [ $? -eq 0 ]; then
        echo "✅ Jaeger stopped successfully"
    else
        echo "❌ Failed to stop Jaeger (may not be running)"
    fi
}

restart_jaeger() {
    echo "🔄 Restarting Jaeger..."
    stop_jaeger
    sleep 2
    clean_jaeger
    start_jaeger
}

status_jaeger() {
    echo "📊 Jaeger Status:"
    docker ps --filter name=jaeger-demo --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    
    echo ""
    echo "🌐 Testing connectivity..."
    if curl -s http://localhost:16686 > /dev/null; then
        echo "✅ Jaeger UI is accessible at http://localhost:16686"
    else
        echo "❌ Jaeger UI is not accessible"
    fi
    
    if curl -s http://localhost:14268 > /dev/null; then
        echo "✅ Jaeger collector is accessible"
    else
        echo "❌ Jaeger collector is not accessible"
    fi
}

clean_jaeger() {
    echo "🧹 Cleaning up Jaeger container..."
    docker stop jaeger-demo 2>/dev/null
    docker rm jaeger-demo 2>/dev/null
    echo "✅ Cleanup complete"
}

show_logs() {
    echo "📋 Jaeger logs:"
    docker logs jaeger-demo
}

open_ui() {
    echo "🌐 Opening Jaeger UI..."
    if command -v open > /dev/null; then
        open http://localhost:16686
    elif command -v xdg-open > /dev/null; then
        xdg-open http://localhost:16686
    else
        echo "📱 Please open http://localhost:16686 in your browser"
    fi
}

test_connectivity() {
    echo "🔍 Testing Jaeger connectivity..."
    
    echo "Testing UI (port 16686)..."
    if curl -s -o /dev/null -w "%{http_code}" http://localhost:16686 | grep -q "200"; then
        echo "✅ UI is responding"
    else
        echo "❌ UI is not responding"
    fi
    
    echo "Testing collector (port 14268)..."
    if curl -s -o /dev/null http://localhost:14268; then
        echo "✅ Collector is responding"
    else
        echo "❌ Collector is not responding"
    fi
    
    echo "Checking container status..."
    if docker ps --filter name=jaeger-demo | grep -q jaeger-demo; then
        echo "✅ Container is running"
    else
        echo "❌ Container is not running"
    fi
}

# Main script logic
case "$1" in
    start)
        start_jaeger
        ;;
    stop)
        stop_jaeger
        ;;
    restart)
        restart_jaeger
        ;;
    status)
        status_jaeger
        ;;
    clean)
        clean_jaeger
        ;;
    logs)
        show_logs
        ;;
    ui)
        open_ui
        ;;
    test)
        test_connectivity
        ;;
    help|--help|-h)
        show_help
        ;;
    "")
        echo "❓ No command specified. Use '$0 help' for usage information."
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo "Use '$0 help' for usage information."
        exit 1
        ;;
esac
