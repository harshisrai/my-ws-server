#!/usr/bin/env python3
"""
CSV Replay Streaming Server

Replays CSV market data over HTTP/WebSocket for testing purposes.
Simulates live market data stream from historical CSV file.

Features:
- HTTP REST API endpoints for current data
- WebSocket streaming for real-time updates
- Configurable replay speed
- Multiple symbols support
- Kafka publishing (optional)

Usage:
  python csv_replay_server.py                          # Default settings
  python csv_replay_server.py --csv d1.csv             # Custom CSV file
  python csv_replay_server.py --port 8080              # Custom port
  python csv_replay_server.py --speed 2.0              # 2x replay speed
  python csv_replay_server.py --kafka                  # Also publish to Kafka
"""

import asyncio
import json
import csv
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set
import signal
import sys

from aiohttp import web
import aiohttp

# Optional: Kafka support
KAFKA_AVAILABLE = False
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    pass

# =============================================================================
# Configuration
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CSV Replay Manager
# =============================================================================
class CSVReplayManager:
    """Manages CSV data replay and streaming to clients."""
    
    def __init__(self, csv_file: str, replay_speed: float = 100.0, 
                 publish_kafka: bool = False, kafka_servers: str = "localhost:9094"):
        self.csv_file = csv_file
        self.replay_speed = replay_speed
        self.publish_kafka = publish_kafka
        self.kafka_servers = kafka_servers
        
        # Data storage
        self.data: List[Dict] = []
        self.current_index = 0
        
        # WebSocket clients
        self.ws_clients: Set[web.WebSocketResponse] = set()
        
        # Stats
        self.messages_sent = 0
        self.start_time = None
        self.is_running = False
        
        # Kafka producer (optional)
        self.kafka_producer = None
        if self.publish_kafka and KAFKA_AVAILABLE:
            self._init_kafka()
        
        # Load CSV data
        self._load_csv()
    
    def _init_kafka(self):
        """Initialize Kafka producer."""
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                acks=0,
                linger_ms=1,
                batch_size=5,
            )
            logger.info(f"✅ Kafka producer initialized: {self.kafka_servers}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Kafka: {e}")
            self.kafka_producer = None
    
    def _load_csv(self):
        """Load CSV data into memory."""
        try:
            logger.info(f"📂 Loading CSV file: {self.csv_file}")
            
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.data = list(reader)
            
            logger.info(f"✅ Loaded {len(self.data)} records")
            
            # Log sample record
            if self.data:
                logger.info(f"📊 Sample record: {json.dumps(self.data[0], indent=2)}")
                logger.info(f"📋 Columns: {list(self.data[0].keys())}")
        
        except Exception as e:
            logger.error(f"❌ Failed to load CSV: {e}")
            self.data = []
    
    def _parse_record(self, record: Dict) -> Dict:
        """Parse and clean CSV record."""
        # Convert numeric strings to proper types
        parsed = {}
        
        for key, value in record.items():
            # Skip empty values
            if value == '' or value is None:
                continue
            
            # Try to convert to appropriate type
            try:
                # Try integer first
                if '.' not in str(value):
                    parsed[key] = int(value)
                else:
                    parsed[key] = float(value)
            except (ValueError, TypeError):
                # Try boolean
                if str(value).lower() in ('true', 'false'):
                    parsed[key] = str(value).lower() == 'true'
                else:
                    # Keep as string
                    parsed[key] = str(value)
        
        # Ensure timestamp field exists
        if 'time' not in parsed and 'timestamp' not in parsed:
            parsed['time'] = datetime.now(timezone.utc).isoformat()
        
        return parsed
    
    async def broadcast_message(self, message: Dict):
        """Send message to all connected WebSocket clients."""
        if not self.ws_clients:
            return
        
        # Prepare JSON message
        json_message = json.dumps(message, default=str)
        
        # Send to all clients
        disconnected = set()
        for ws in self.ws_clients:
            try:
                await ws.send_str(json_message)
            except Exception as e:
                logger.warning(f"⚠️ Failed to send to client: {e}")
                disconnected.add(ws)
        
        # Remove disconnected clients
        self.ws_clients -= disconnected
    
    def publish_to_kafka(self, message: Dict):
        """Publish message to Kafka topic."""
        if not self.kafka_producer:
            return
        
        try:
            # Publish to 'market_data' topic
            self.kafka_producer.send('market_data', message)
            
            # Also publish to symbol-specific topic if available
            symbol = message.get('symbol', '').lower()
            if symbol:
                self.kafka_producer.send(symbol, message)
        
        except Exception as e:
            logger.error(f"❌ Kafka publish error: {e}")
    
    async def replay_loop(self):
        """Main replay loop - streams data to clients."""
        if not self.data:
            logger.error("❌ No data to replay!")
            return
        
        self.is_running = True
        self.start_time = datetime.now()
        self.current_index = 0
        
        logger.info("=" * 60)
        logger.info("🚀 Starting CSV replay...")
        logger.info(f"   Records: {len(self.data)}")
        logger.info(f"   Speed: {self.replay_speed}x")
        logger.info(f"   Kafka: {'Enabled' if self.kafka_producer else 'Disabled'}")
        logger.info(f"   WebSocket clients: {len(self.ws_clients)}")
        logger.info("=" * 60)
        
        # Calculate delay between messages
        base_delay = 0.005  # 5ms base delay (200 msgs/sec at 1x speed)
        delay = base_delay / self.replay_speed
        
        while self.is_running:
            # Only replay if there are connected clients or Kafka is enabled
            if not self.ws_clients and not self.kafka_producer:
                logger.info("⏸️ No active clients. Pausing replay...")
                await asyncio.sleep(1)  # Wait for clients to connect
                continue
            
            # Replay loop
            while self.is_running and self.current_index < len(self.data):
                # Check if we still have clients
                if not self.ws_clients and not self.kafka_producer:
                    break
                
                # Get current record
                record = self.data[self.current_index]
                message = self._parse_record(record)
                
                # Broadcast to WebSocket clients
                await self.broadcast_message(message)
                
                # Publish to Kafka (optional)
                if self.kafka_producer:
                    self.publish_to_kafka(message)
                
                # Update stats
                self.messages_sent += 1
                self.current_index += 1
                
                # Log progress every 100 messages
                if self.messages_sent % 100 == 0:
                    elapsed = (datetime.now() - self.start_time).total_seconds()
                    rate = self.messages_sent / elapsed if elapsed > 0 else 0
                    progress = (self.current_index / len(self.data)) * 100
                    logger.info(f"📊 Progress: {progress:.1f}% | "
                              f"Messages: {self.messages_sent} | "
                              f"Rate: {rate:.1f} msg/s | "
                              f"Clients: {len(self.ws_clients)}")
                
                # Sleep before next message
                await asyncio.sleep(delay)
            
            # Replay finished - loop back to start
            if self.is_running and self.current_index >= len(self.data):
                logger.info("🔄 Replay finished. Looping back to start...")
                self.current_index = 0
    
    def stop(self):
        """Stop replay loop."""
        self.is_running = False
        logger.info("🛑 Stopping replay...")
    
    def get_stats(self) -> Dict:
        """Get current statistics."""
        elapsed = 0
        rate = 0
        
        if self.start_time:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            rate = self.messages_sent / elapsed if elapsed > 0 else 0
        
        return {
            "is_running": self.is_running,
            "total_records": len(self.data),
            "current_index": self.current_index,
            "messages_sent": self.messages_sent,
            "ws_clients_connected": len(self.ws_clients),
            "replay_speed": self.replay_speed,
            "elapsed_seconds": round(elapsed, 2),
            "messages_per_second": round(rate, 2),
            "kafka_enabled": self.kafka_producer is not None,
        }


# =============================================================================
# HTTP Server & WebSocket Handlers
# =============================================================================
class StreamingServer:
    """HTTP server with WebSocket support for streaming market data."""
    
    def __init__(self, replay_manager: CSVReplayManager, port: int = 8080):
        self.replay_manager = replay_manager
        self.port = port
        self.app = web.Application()
        self.runner = None
        
        # Setup routes
        self._setup_routes()
    
    def _setup_routes(self):
        """Configure HTTP routes."""
        self.app.router.add_get('/', self.handle_index)
        self.app.router.add_get('/ws', self.handle_websocket)
        self.app.router.add_get('/api/stats', self.handle_stats)
        self.app.router.add_get('/api/current', self.handle_current)
        self.app.router.add_post('/api/control/{action}', self.handle_control)
    
    async def handle_index(self, request):
        """Serve index page with information."""
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>CSV Replay Server</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
                .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                h1 {{ color: #333; }}
                .endpoint {{ background: #f8f9fa; padding: 15px; margin: 10px 0; border-radius: 4px; border-left: 4px solid #007bff; }}
                .endpoint code {{ background: #e9ecef; padding: 2px 6px; border-radius: 3px; }}
                .stats {{ background: #e7f5ff; padding: 15px; border-radius: 4px; margin: 20px 0; }}
                .stat-item {{ margin: 8px 0; }}
                .status {{ display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 8px; }}
                .status.running {{ background: #28a745; }}
                .status.stopped {{ background: #dc3545; }}
                button {{ background: #007bff; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; margin: 5px; }}
                button:hover {{ background: #0056b3; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📊 CSV Replay Streaming Server</h1>
                
                <div class="stats" id="stats">
                    <h3>Server Status</h3>
                    <div id="status-content">Loading...</div>
                </div>
                
                <h2>API Endpoints</h2>
                
                <div class="endpoint">
                    <h3>WebSocket Stream</h3>
                    <code>ws://localhost:{self.port}/ws</code>
                    <p>Connect to receive real-time market data stream</p>
                </div>
                
                <div class="endpoint">
                    <h3>Current Data</h3>
                    <code>GET /api/current</code>
                    <p>Get current record being replayed</p>
                </div>
                
                <div class="endpoint">
                    <h3>Statistics</h3>
                    <code>GET /api/stats</code>
                    <p>Get replay statistics and server status</p>
                </div>
                
                <div class="endpoint">
                    <h3>Control Actions</h3>
                    <code>POST /api/control/start</code> - Start replay<br>
                    <code>POST /api/control/stop</code> - Stop replay<br>
                    <code>POST /api/control/restart</code> - Restart from beginning
                </div>
                
                <h2>Control Panel</h2>
                <button onclick="control('start')">▶️ Start</button>
                <button onclick="control('stop')">⏸️ Stop</button>
                <button onclick="control('restart')">🔄 Restart</button>
                
                <h2>Example Client Code</h2>
                <div class="endpoint">
                    <h3>Python WebSocket Client</h3>
                    <pre>
import asyncio
import websockets
import json

async def consume():
    uri = "ws://localhost:{self.port}/ws"
    async with websockets.connect(uri) as ws:
        while True:
            message = await ws.recv()
            data = json.loads(message)
            print(f"Received: {{data}}")

asyncio.run(consume())
                    </pre>
                </div>
            </div>
            
            <script>
                async function updateStats() {{
                    try {{
                        const response = await fetch('/api/stats');
                        const stats = await response.json();
                        
                        const statusClass = stats.is_running ? 'running' : 'stopped';
                        const statusText = stats.is_running ? 'Running' : 'Stopped';
                        
                        document.getElementById('status-content').innerHTML = `
                            <div class="stat-item">
                                <span class="status ${{statusClass}}"></span>
                                <strong>Status:</strong> ${{statusText}}
                            </div>
                            <div class="stat-item"><strong>Total Records:</strong> ${{stats.total_records}}</div>
                            <div class="stat-item"><strong>Current Position:</strong> ${{stats.current_index}} (${{((stats.current_index/stats.total_records)*100).toFixed(1)}}%)</div>
                            <div class="stat-item"><strong>Messages Sent:</strong> ${{stats.messages_sent}}</div>
                            <div class="stat-item"><strong>Connected Clients:</strong> ${{stats.ws_clients_connected}}</div>
                            <div class="stat-item"><strong>Replay Speed:</strong> ${{stats.replay_speed}}x</div>
                            <div class="stat-item"><strong>Message Rate:</strong> ${{stats.messages_per_second}} msg/s</div>
                            <div class="stat-item"><strong>Kafka:</strong> ${{stats.kafka_enabled ? '✅ Enabled' : '❌ Disabled'}}</div>
                        `;
                    }} catch (e) {{
                        console.error('Failed to fetch stats:', e);
                    }}
                }}
                
                async function control(action) {{
                    try {{
                        const response = await fetch(`/api/control/${{action}}`, {{ method: 'POST' }});
                        const result = await response.json();
                        alert(result.message);
                        updateStats();
                    }} catch (e) {{
                        alert('Control action failed: ' + e);
                    }}
                }}
                
                // Update stats every 2 seconds
                setInterval(updateStats, 2000);
                updateStats();
            </script>
        </body>
        </html>
        """
        return web.Response(text=html, content_type='text/html')
    
    async def handle_websocket(self, request):
        """Handle WebSocket connections."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        # Add to active clients
        self.replay_manager.ws_clients.add(ws)
        client_id = id(ws)
        logger.info(f"✅ New WebSocket client connected: {client_id} | Total: {len(self.replay_manager.ws_clients)}")
        
        # Auto-start replay if this is the first client and replay isn't running
        if len(self.replay_manager.ws_clients) == 1 and not self.replay_manager.is_running:
            logger.info("🚀 First client connected. Starting replay loop...")
            asyncio.create_task(self.replay_manager.replay_loop())
        
        try:
            # Send welcome message
            welcome = {
                "type": "connection",
                "message": "Connected to CSV Replay Server",
                "stats": self.replay_manager.get_stats(),
            }
            await ws.send_json(welcome)
            
            # Keep connection alive
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if msg.data == 'ping':
                        await ws.send_str('pong')
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f'❌ WebSocket error: {ws.exception()}')
        
        finally:
            # Remove from active clients
            self.replay_manager.ws_clients.discard(ws)
            logger.info(f"❌ WebSocket client disconnected: {client_id} | Remaining: {len(self.replay_manager.ws_clients)}")
        
        return ws
    
    async def handle_stats(self, request):
        """Get replay statistics."""
        stats = self.replay_manager.get_stats()
        return web.json_response(stats)
    
    async def handle_current(self, request):
        """Get current record being replayed."""
        if 0 <= self.replay_manager.current_index < len(self.replay_manager.data):
            record = self.replay_manager.data[self.replay_manager.current_index]
            parsed = self.replay_manager._parse_record(record)
            return web.json_response(parsed)
        else:
            return web.json_response({"error": "No data available"}, status=404)
    
    async def handle_control(self, request):
        """Handle control actions (start/stop/restart)."""
        action = request.match_info['action']
        
        if action == 'start':
            if not self.replay_manager.is_running:
                asyncio.create_task(self.replay_manager.replay_loop())
                return web.json_response({"status": "success", "message": "Replay started"})
            else:
                return web.json_response({"status": "info", "message": "Already running"})
        
        elif action == 'stop':
            self.replay_manager.stop()
            return web.json_response({"status": "success", "message": "Replay stopped"})
        
        elif action == 'restart':
            self.replay_manager.stop()
            await asyncio.sleep(0.5)
            self.replay_manager.current_index = 0
            self.replay_manager.messages_sent = 0
            asyncio.create_task(self.replay_manager.replay_loop())
            return web.json_response({"status": "success", "message": "Replay restarted"})
        
        else:
            return web.json_response({"status": "error", "message": f"Unknown action: {action}"}, status=400)
    
    async def start(self):
        """Start HTTP server."""
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, '0.0.0.0', self.port)
        await site.start()
        
        logger.info("=" * 60)
        logger.info(f"🌐 Server started on http://0.0.0.0:{self.port}")
        logger.info(f"📡 WebSocket endpoint: ws://localhost:{self.port}/ws")
        logger.info(f"📊 Web UI: http://localhost:{self.port}")
        logger.info("=" * 60)
    
    async def stop(self):
        """Stop HTTP server."""
        if self.runner:
            await self.runner.cleanup()


# =============================================================================
# Main
# =============================================================================
async def main():
    parser = argparse.ArgumentParser(description='CSV Replay Streaming Server')
    parser.add_argument('--csv', type=str, default='d1.csv',
                       help='Path to CSV file (default: d1.csv)')
    parser.add_argument('--port', type=int, default=8080,
                       help='Server port (default: 8080)')
    parser.add_argument('--speed', type=float, default=1.0,
                       help='Replay speed multiplier (default: 1.0)')
    parser.add_argument('--kafka', action='store_true',
                       help='Also publish to Kafka (requires kafka-python)')
    parser.add_argument('--kafka-servers', type=str, default='localhost:9094',
                       help='Kafka bootstrap servers (default: localhost:9094)')
    
    args = parser.parse_args()
    
    # Check if CSV file exists
    csv_path = Path(args.csv)
    if not csv_path.exists():
        logger.error(f"❌ CSV file not found: {args.csv}")
        sys.exit(1)
    
    # Initialize replay manager
    replay_manager = CSVReplayManager(
        csv_file=str(csv_path),
        replay_speed=args.speed,
        publish_kafka=args.kafka,
        kafka_servers=args.kafka_servers,
    )
    
    # Initialize server
    server = StreamingServer(replay_manager, port=args.port)
    
    # Start server
    await server.start()
    
    # Don't auto-start replay - it will start when first client connects
    logger.info("⏸️ Server ready. Replay will start when clients connect.")
    
    # Graceful shutdown handler
    def signal_handler(signum, frame):
        logger.info(f"\n🛑 Received signal {signum}. Shutting down...")
        replay_manager.stop()
        asyncio.create_task(server.stop())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Keep running
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("\n🛑 Keyboard interrupt. Shutting down...")
        replay_manager.stop()
        await server.stop()


if __name__ == '__main__':
    asyncio.run(main())
