"""
Standalone daemon entry point: an explicit, user-invoked long-lived process
(tied to whatever TTY, process manager, or supervisor the user chooses to run
it under - nothing auto-spawns it). Runs the socket listener loop.
"""
import asyncio
import logging
import signal

from .client import ClaudeClient

logger = logging.getLogger("claude_chatter")


async def run_standalone(client: ClaudeClient):
    client.register_session_descriptor(session_name=client.session_name, kind="bg")

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        logger.info("Termination signal received. Initiating graceful shutdown...")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, RuntimeError):
            pass

    listener_task = asyncio.create_task(client.start_bridge_listener())
    stop_task = asyncio.create_task(stop_event.wait())

    done, pending = await asyncio.wait(
        [listener_task, stop_task],
        return_when=asyncio.FIRST_COMPLETED
    )

    for task in pending:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    client.cleanup_session_descriptor()
    client.cleanup_socket()
    logger.info("Standalone daemon shut down gracefully.")
