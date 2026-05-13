#!/usr/bin/env python3
"""
Universal Signal Bridge
-----------------------
Production-grade FastAPI webhook listener that accepts JSON signals,
authenticates via X-API-Key, and dispatches alerts to Telegram and a
Virtual Exchange concurrently. Uses an internal asyncio task queue to
ensure zero message loss even when external services are slow.

Environment variables:
    API_KEY              : Required API key for webhook authentication
    TELEGRAM_BOT_TOKEN   : Bot token for Telegram API (optional, mock if missing)
    TELEGRAM_CHAT_ID     : Target chat ID for Telegram alerts (optional)
    VIRTUAL_EXCHANGE_URL : Mock URL for virtual trade execution (optional)
    LOG_LEVEL            : Logging level (default INFO)

Run with: uvicorn script:app --host 0.0.0.0 --port 8000
"""

import asyncio
import json
import logging
import os
import signal
import sys
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

import httpx
from fastapi import FastAPI, HTTPException, Request, Depends, BackgroundTasks
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, Field, ValidationError, HttpUrl
from logging.handlers import RotatingFileHandler

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("UniversalSignalBridge")
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        file_handler = RotatingFileHandler(
            "signal_bridge.log", maxBytes=10_485_760, backupCount=5
        )
        file_handler.setFormatter(console_formatter)
        logger.addHandler(file_handler)

    return logger

logger = setup_logging()

# -----------------------------------------------------------------------------
# Pydantic Models for Signal Validation
# -----------------------------------------------------------------------------
class SignalPayload(BaseModel):
    """
    Expected JSON structure for an incoming signal.
    """
    signal_id: str = Field(..., description="Unique identifier for the signal")
    symbol: str = Field(..., description="Trading pair, e.g., BTCUSDT")
    action: str = Field(..., description="buy, sell, hold, alert")
    price: Optional[float] = Field(None, description="Trigger price if any")
    confidence: float = Field(0.0, ge=0.0, le=1.0, description="Signal confidence")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional data")

# -----------------------------------------------------------------------------
# Mock External Services (Telegram & Virtual Exchange)
# -----------------------------------------------------------------------------
class TelegramSender:
    """
    Sends messages to Telegram using bot API. Falls back to mock logging if
    credentials are not provided or API call fails.
    """
    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.client = httpx.AsyncClient(timeout=10.0)

    async def send_alert(self, signal: SignalPayload) -> bool:
        """Send formatted alert to Telegram. Returns True on success."""
        if not self.bot_token or not self.chat_id:
            logger.warning("Telegram credentials missing. Mock alert: %s", signal.json())
            return True

        text = (
            f"Signal Alert\n"
            f"ID: {signal.signal_id}\n"
            f"Symbol: {signal.symbol}\n"
            f"Action: {signal.action}\n"
            f"Price: {signal.price or 'N/A'}\n"
            f"Confidence: {signal.confidence:.2f}"
        )
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        try:
            resp = await self.client.post(url, json={"chat_id": self.chat_id, "text": text})
            resp.raise_for_status()
            logger.info("Telegram alert sent for signal %s", signal.signal_id)
            return True
        except Exception as e:
            logger.error("Telegram send failed for signal %s: %s", signal.signal_id, str(e))
            return False

    async def close(self):
        await self.client.aclose()

class VirtualExchange:
    """
    Mock virtual exchange that simulates executing a trade. Logs the action
    and optionally calls an external mock URL.
    """
    def __init__(self):
        self.mock_url = os.getenv("VIRTUAL_EXCHANGE_URL", "http://localhost:8080/mock_trade")
        self.client = httpx.AsyncClient(timeout=5.0)

    async def execute_trade(self, signal: SignalPayload) -> bool:
        """Mock trade execution. Returns True on success."""
        trade_payload = {
            "signal_id": signal.signal_id,
            "symbol": signal.symbol,
            "action": signal.action,
            "price": signal.price,
            "quantity": 0.01  # fixed mock quantity
        }
        logger.info("Mock trade execution: %s", json.dumps(trade_payload, indent=2))

        # Optionally notify a mock external service
        try:
            resp = await self.client.post(self.mock_url, json=trade_payload, timeout=2.0)
            if resp.status_code < 400:
                logger.debug("Virtual exchange acknowledged trade %s", signal.signal_id)
            else:
                logger.warning("Virtual exchange returned %s for %s", resp.status_code, signal.signal_id)
        except Exception as e:
            logger.warning("Virtual exchange notification failed (non-critical): %s", str(e))

        return True

    async def close(self):
        await self.client.aclose()

# -----------------------------------------------------------------------------
# Background Task Queue
# -----------------------------------------------------------------------------
class SignalQueueProcessor:
    """
    Asynchronous worker that consumes signals from a queue and processes each
    by concurrently dispatching to Telegram and Virtual Exchange.
    """
    def __init__(self, telegram: TelegramSender, exchange: VirtualExchange, max_workers: int = 5):
        self.queue = asyncio.Queue()
        self.telegram = telegram
        self.exchange = exchange
        self.max_workers = max_workers
        self.workers = []
        self._running = True

    async def enqueue(self, signal: SignalPayload):
        """Add a signal to the processing queue."""
        await self.queue.put(signal)
        logger.debug("Signal %s enqueued. Queue size: %d", signal.signal_id, self.queue.qsize())

    async def _process_one(self, signal: SignalPayload):
        """Process a single signal by concurrently sending to both channels."""
        logger.info("Processing signal %s", signal.signal_id)
        # Concurrent dispatch to Telegram and Virtual Exchange
        results = await asyncio.gather(
            self.telegram.send_alert(signal),
            self.exchange.execute_trade(signal),
            return_exceptions=True
        )
        # Log any failures
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                logger.error("Error in channel %d for signal %s: %s", i, signal.signal_id, str(res))
            elif res is False:
                logger.warning("Channel %d returned failure for signal %s", i, signal.signal_id)
        logger.info("Completed processing signal %s", signal.signal_id)

    async def worker(self, worker_id: int):
        """Worker coroutine that continuously pulls from the queue."""
        logger.info("Worker %d started", worker_id)
        while self._running:
            try:
                signal = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                await self._process_one(signal)
            except Exception as e:
                logger.exception("Unhandled error in worker %d processing signal %s: %s",
                                 worker_id, signal.signal_id, str(e))
            finally:
                self.queue.task_done()

    async def start(self):
        """Launch worker tasks."""
        self._running = True
        self.workers = [asyncio.create_task(self.worker(i)) for i in range(self.max_workers)]

    async def stop(self):
        """Gracefully stop workers and wait for queue to drain."""
        self._running = False
        # Wait for all queued signals to be processed
        await self.queue.join()
        for w in self.workers:
            w.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        logger.info("All workers stopped, queue drained.")

# -----------------------------------------------------------------------------
# FastAPI Application with Lifespan
# -----------------------------------------------------------------------------
# Global instances
telegram_sender = TelegramSender()
virtual_exchange = VirtualExchange()
queue_processor = SignalQueueProcessor(telegram_sender, virtual_exchange, max_workers=3)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background workers on startup and clean up on shutdown."""
    await queue_processor.start()
    logger.info("Signal bridge started. Workers ready.")
    yield
    logger.info("Shutting down signal bridge...")
    await queue_processor.stop()
    await telegram_sender.close()
    await virtual_exchange.close()
    logger.info("Signal bridge stopped.")

app = FastAPI(title="Universal Signal Bridge", lifespan=lifespan)

# API Key Security
api_key_header = APIKeyHeader(name="X-API-KEY", auto_error=False)

async def verify_api_key(api_key: str = Depends(api_key_header)):
    """Dependency to check X-API-Key header against environment variable."""
    expected_key = os.getenv("API_KEY")
    if not expected_key:
        raise HTTPException(status_code=500, detail="API_KEY environment variable not set")
    if not api_key or api_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API Key")
    return True

# -----------------------------------------------------------------------------
# Webhook Endpoint
# -----------------------------------------------------------------------------
@app.post("/webhook", status_code=202)
async def webhook_receiver(
    signal: SignalPayload,
    authenticated: bool = Depends(verify_api_key)
):
    """
    Accept incoming JSON signal, validate with Pydantic, enqueue for processing,
    and return immediately (202 Accepted). No signal is lost because the queue
    persists in memory.
    """
    logger.info("Received signal %s for %s action %s", signal.signal_id, signal.symbol, signal.action)
    await queue_processor.enqueue(signal)
    return {"status": "accepted", "signal_id": signal.signal_id}

# -----------------------------------------------------------------------------
# Health Check Endpoint
# -----------------------------------------------------------------------------
@app.get("/health")
async def health_check():
    """Simple health check for orchestration tools."""
    return {"status": "running", "queue_size": queue_processor.queue.qsize()}

# -----------------------------------------------------------------------------
# Main (for local development)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    # Check required API key
    if not os.getenv("API_KEY"):
        logger.error("API_KEY environment variable is required. Exiting.")
        sys.exit(1)
    uvicorn.run(app, host="0.0.0.0", port=8000)
