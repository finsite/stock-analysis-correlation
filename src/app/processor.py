"""Processor module for computing correlation signals from input messages.

This module validates incoming messages and computes a derived correlation signal
based on the input data. All operations are logged for observability.
"""

from typing import Any

from app.utils.setup_logger import setup_logger
from app.utils.types import ValidatedMessage
from app.utils.validate_data import validate_message_schema

logger = setup_logger(__name__)


def validate_input_message(message: dict[str, Any]) -> ValidatedMessage:
    """
    Validate the incoming raw message against the expected schema.

    Parameters:
        message (dict[str, Any]): The raw message payload.

    Returns:
        ValidatedMessage: A validated message object.
    """
    logger.debug("🔍 Validating message schema...")
    if not validate_message_schema(message):
        logger.error("❌ Message schema invalid: %s", message)
        raise ValueError("Invalid message format")
    return message  # type: ignore[return-value]


def compute_correlation_signal(message: ValidatedMessage) -> dict[str, Any]:
    """
    Compute a correlation signal from the validated input message.

    This function is a placeholder for actual correlation computations
    between assets or data series.

    Parameters:
        message (ValidatedMessage): The validated message input.

    Returns:
        dict[str, Any]: Dictionary with correlation-related data.
    """
    logger.debug("📊 Computing correlation signal for %s", message["symbol"])
    correlation_score = 0.85  # Placeholder logic

    return {
        "symbol": message["symbol"],
        "timestamp": message["timestamp"],
        "correlation_score": correlation_score,
    }


def process_message(raw_message: dict[str, Any]) -> ValidatedMessage:
    """
    Main entry point for processing a single message.

    Parameters:
        raw_message (dict[str, Any]): Raw input from the message queue.

    Returns:
        ValidatedMessage: Enriched and validated message ready for output.
    """
    logger.info("🚦 Processing new message...")
    validated = validate_input_message(raw_message)
    correlation_data = compute_correlation_signal(validated)

    enriched: ValidatedMessage = {
        "symbol": validated["symbol"],
        "timestamp": validated["timestamp"],
        "data": {**validated["data"], **correlation_data},
    }
    logger.debug("✅ Final enriched message: %s", enriched)
    return enriched
