import time
import random
from abc import ABC, abstractmethod
from typing import List, Optional, Type, Callable
from enum import Enum

class RetryPolicy(ABC):
    """Base class for retry policies"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0,
                 exclude_exceptions: List[Exception]=None):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.exclude_exceptions = [ValueError]
        if exclude_exceptions:
            self.exclude_exceptions.append(exclude_exceptions)
    
    @abstractmethod
    def get_delay(self, attempt: int) -> float:
        """Calculate delay before next retry"""
        pass
    
    def should_retry(self, attempt: int, exception: Exception) -> bool:
        """Determine if we should retry based on attempt count and exception type"""
        if self.exclude_exceptions and type(exception) in self.exclude_exceptions:
            return False
        return attempt <= self.max_retries 

class LinearRetryPolicy(RetryPolicy):
    """Linear backoff: delay increases linearly"""
    
    def get_delay(self, attempt: int) -> float:
        return self.base_delay * attempt

class ExponentialRetryPolicy(RetryPolicy):
    """Exponential backoff: delay doubles each time"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0,
                 exclude_exceptions: List[Exception]=None):
        super().__init__(max_retries, base_delay, exclude_exceptions)
        self.max_delay = max_delay
    
    def get_delay(self, attempt: int) -> float:
        delay = self.base_delay * (2 ** (attempt - 1))
        return min(delay, self.max_delay)

class ExponentialJitterRetryPolicy(ExponentialRetryPolicy):
    """Exponential backoff with jitter to avoid thundering herd"""
    
    def get_delay(self, attempt: int) -> float:
        base_delay = super().get_delay(attempt)
        # Add random jitter (±25%)
        jitter = base_delay * 0.25 * (2 * random.random() - 1)
        return max(0.1, base_delay + jitter)

class ConditionalRetryPolicy(RetryPolicy):
    """Retry only for specific exception types"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, 
                 retryable_exceptions: tuple = None):
        super().__init__(max_retries, base_delay)
        self.retryable_exceptions = retryable_exceptions or (Exception,)
    
    def should_retry(self, attempt: int, exception: Exception) -> bool:
        if attempt > self.max_retries:
            return False
        return isinstance(exception, self.retryable_exceptions)
    
    def get_delay(self, attempt: int) -> float:
        return self.base_delay
