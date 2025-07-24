import os 
import asyncio
import aiohttp
import logging
from typing import Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
load_dotenv()

OLLAMA_MODEL_NAME = os.getenv("OLLAMA_MODEL_NAME")
OLLAMA_ENDPOINT = os.getenv("OLLAMA_ENDPOINT")

session = None
session_lock = asyncio.Lock()

async def initialize_session():
    global session
    async with session_lock:
        if session is None:
            session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60),
                connector=aiohttp.TCPConnector(limit=100)
            )

async def close_session():
    global session
    if session:
        logger.info("Closing HTTP Session")
        await session.close()
        session = None

async def call_qwen(prompt: str, retries: int = 2, model: Optional[str] = None) -> str:
    global session
    if session is None:
        await initialize_session()

    model_name = model or OLLAMA_MODEL_NAME

    for attempt in range(retries + 1):
        try:
            async with session.post(
                OLLAMA_ENDPOINT,
                json={
                    "model": model_name,
                    "prompt": prompt,
                    "stream": False
                }
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data["response"]
                else:
                    error_text = await response.text()
                    logger.warning(f"Qwen API returned status {response.status}: {error_text}")
                    raise Exception(f"Status {response.status}: {error_text}")
        except Exception as e:
            if attempt == retries:
                logger.error(f"Qwen failed after {retries + 1} tries: {e}")
                raise Exception(f"Qwen failed after {retries + 1} tries: {e}")
            logger.warning(f"Qwen attempt {attempt + 1} failed: {e}, retrying...")
            await asyncio.sleep(0.5)