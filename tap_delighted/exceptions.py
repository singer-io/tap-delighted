class DelightedError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class DelightedBackoffError(DelightedError):
    """class representing backoff error handling."""
    pass


class DelightedBadRequestError(DelightedError):
    """class representing 400 status code."""
    pass


class DelightedUnauthorizedError(DelightedError):
    """class representing 401 status code."""
    pass


class DelightedForbiddenError(DelightedError):
    """class representing 403 status code."""
    pass


class DelightedNotFoundError(DelightedError):
    """class representing 404 status code."""
    pass


class DelightedConflictError(DelightedError):
    """class representing 409 status code."""
    pass


class DelightedUnprocessableEntityError(DelightedBackoffError):
    """class representing 422 status code."""
    pass


class DelightedRateLimitError(DelightedBackoffError):
    """class representing 429 status code."""
    pass


class DelightedInternalServerError(DelightedBackoffError):
    """class representing 500 status code."""
    pass


class DelightedNotImplementedError(DelightedBackoffError):
    """class representing 501 status code."""
    pass


class DelightedBadGatewayError(DelightedBackoffError):
    """class representing 502 status code."""
    pass


class DelightedServiceUnavailableError(DelightedBackoffError):
    """class representing 503 status code."""
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": DelightedBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": DelightedUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": DelightedForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": DelightedNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": DelightedConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": DelightedUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": DelightedRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": DelightedInternalServerError,
        "message": "The server encountered an unexpected condition which prevented it from fulfilling the request."
    },
    501: {
        "raise_exception": DelightedNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": DelightedBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": DelightedServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}
