"""
Workflow System Package

This package provides decorators and utilities for workflow tracking,
retry policies, and lifecycle management.
"""

from .workflow import workflow_defn, workstep_defn, WorkflowContext, get_current_context, _execute_workstep_wrapper
from .retry_policies import (
    RetryPolicy, 
    LinearRetryPolicy, 
    ExponentialRetryPolicy,
    ExponentialJitterRetryPolicy,
    ConditionalRetryPolicy
)
from .models import WorkflowStatus, WorkStepStatus, WorkflowInstance, WorkStepInstance, WorkstepLifecycle, WorkflowLifecycle, Session
from .utils import (
    get_workflow_instance_summary,
    get_step_instance_details,
    # analyze_retry_effectiveness
)

# Version info
__version__ = "1.0.1"
__author__ = "Romen Law"

# Main exports
__all__ = [
    # Decorators
    'workflow_defn',
    'workstep_defn',
    'WorkflowContext',
    'get_current_context',
    '_execute_workstep_wrapper',
    
    # Retry Policies
    'RetryPolicy',
    'LinearRetryPolicy', 
    'ExponentialRetryPolicy',
    'ExponentialJitterRetryPolicy',
    'ConditionalRetryPolicy',
    
    # Data(base) Models
    'WorkflowStatus', 'WorkStepStatus',
    'WorkflowInstance', 'WorkStepInstance',
    'WorkstepLifecycle', 'WorkflowLifecycle',
    'Session',
    
    # Utilities
    'get_workflow_instance_summary',
    'get_step_instance_details',
    # 'analyze_retry_effectiveness',
]

# Convenience functions
def create_workflow_defn(**kwargs):
    """Factory function to create workflow definition with common defaults"""
    return workflow_defn(**kwargs)

def create_workstep_defn(**kwargs):
    """Factory function to create step defintion with common defaults"""
    return workstep_defn(**kwargs)
