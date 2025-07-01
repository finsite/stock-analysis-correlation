"""
Processor module for stock-backtest-correlation signal generation.

Validates incoming messages and computes a correlation score
between the target symbol and a reference (benchmark or peer).
"""

from typing import Any

from app.utils.setup_logger import setup_logger
from app.utils.types import ValidatedMessage
from app.utils.validate_data import validate_message_schema

logger = setup_logger(__name__)


def validate_input_message(message: dict[str, Any]) -> ValidatedMessage:
    """
    Validate the incoming raw message against the expected schema.

    Args:
        message (dict[str, Any]): The raw message payload.

    Returns:
        ValidatedMessage: A validated message object.

    Raises:
        ValueError: If the message format is invalid.
    """
    logger.debug("🔍 Validating message schema...")
    if not validate_message_schema(message):
        logger.error("❌ Invalid message schema: %s", message)
        raise ValueError("Invalid message format")
    return message  # type: ignore[return-value]


def compute_correlation_signal(message: ValidatedMessage) -> dict[str, Any]:
    """
    Compute a correlation score and basic signal using placeholder logic.

    Normally, this would involve historical price comparisons against
    a benchmark like SPY or peer stocks using Pearson correlation.

    Args:
        message (ValidatedMessage): The validated input data.

    Returns:
        dict[str, Any]: The enriched message with correlation score and signal.
    """
    symbol = message.get("symbol", "UNKNOWN")
    logger.info("🔗 Computing correlation signal for %s", symbol)

    # Placeholder logic — real implementation would use a rolling window
    correlation_score = 0.76  # Simulated result from price vector comparison
    signal = "HIGH_CORRELATION" if correlation_score > 0.7 else "LOW_CORRELATION"

    result = {
        "correlation_score": correlation_score,
        "correlation_signal": signal,
    }

    logger.debug("📈 Correlation result for %s: %s", symbol, result)
    return {**message, **result}
