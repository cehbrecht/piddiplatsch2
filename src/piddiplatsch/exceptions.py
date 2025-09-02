"""Custom exceptions for piddiplatsch."""


class PiddiplatschError(Exception):
    """Base class for all piddiplatsch exceptions."""


class LookupError(PiddiplatschError):
    """Raised when a dataset version lookup via STAC or other lookup fails."""


class ValidationError(PiddiplatschError):
    """Raised when input data or a record fails validation."""
