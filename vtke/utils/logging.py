"""Logging utilities for VTKE."""

import json
from datetime import datetime
from typing import Any, Dict, Optional


class LogEvent:
    """Structured log event."""

    def __init__(
        self,
        level: str,
        message: str,
        stage: Optional[str] = None,
        **kwargs: Any
    ):
        self.timestamp = datetime.now().isoformat()
        self.level = level
        self.message = message
        self.stage = stage
        self.extra = kwargs

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            "timestamp": self.timestamp,
            "level": self.level,
            "message": self.message,
        }
        if self.stage:
            result["stage"] = self.stage
        if self.extra:
            result.update(self.extra)
        return result

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())

    def __str__(self) -> str:
        """Human-readable format."""
        prefix = f"[{self.level}]"
        if self.stage:
            prefix += f" [{self.stage}]"
        return f"{prefix} {self.message}"


class Logger:
    """Simple logger with structured events."""

    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.events: list[LogEvent] = []

    def log(self, event: LogEvent) -> None:
        """Log an event."""
        self.events.append(event)
        if self.verbose:
            print(str(event))

    def info(self, message: str, stage: Optional[str] = None, **kwargs: Any) -> None:
        """Log info message."""
        self.log(LogEvent("INFO", message, stage, **kwargs))

    def warn(self, message: str, stage: Optional[str] = None, **kwargs: Any) -> None:
        """Log warning message."""
        self.log(LogEvent("WARN", message, stage, **kwargs))

    def error(self, message: str, stage: Optional[str] = None, **kwargs: Any) -> None:
        """Log error message."""
        self.log(LogEvent("ERROR", message, stage, **kwargs))

    def debug(self, message: str, stage: Optional[str] = None, **kwargs: Any) -> None:
        """Log debug message."""
        self.log(LogEvent("DEBUG", message, stage, **kwargs))

    def get_events_json(self) -> str:
        """Get all events as JSONL."""
        return "\n".join(event.to_json() for event in self.events)
