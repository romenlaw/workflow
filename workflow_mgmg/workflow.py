from enum import Enum
from typing import Optional, Dict, Any, List, Union
import logging
import functools
import uuid
import inspect
import traceback
import threading
import json
from datetime import datetime
import time
import random
import asyncio

from .retry_policies import RetryPolicy, LinearRetryPolicy
from .models import WorkflowStatus, WorkStepStatus, WorkflowInstance, WorkStepInstance, Session

class WorkflowContext:
    def __init__(self, workflow_instance: WorkflowInstance = None, retry_policy: RetryPolicy = None):
        self.workflow_instance: WorkflowInstance = workflow_instance
        self.current_step_instance_id: Optional[int] = None
        self.session: Optional[Any] = None
        self.status: WorkflowStatus = None
        self.workflow_retry_policy = retry_policy  # Store workflow-level retry policy
        # self.workflow_payload: Dict[str, Any] = {}
    
# Thread-local storage for workflow context
_context = threading.local()

def get_current_context() -> Optional[WorkflowContext]:
    return getattr(_context, 'workflow_context', None)

def set_current_context(context: WorkflowContext):
    _context.workflow_context = context

##############################
# Workflow Definition
##############################
def workflow_defn(retry_policy: RetryPolicy = None):
    """Decorator for Workflow classes to track lifecycle and set retry policy"""
    
    def decorator(workflow_class):
        original_init = workflow_class.__init__
        
        @functools.wraps(original_init)
        def new_init(self, *args, **kwargs):

            # Find and store payload data
            sig = inspect.signature(original_init)
            bound_args = sig.bind(self, *args, **kwargs)
            bound_args.apply_defaults()
            # print("Parameter names and values:")
            # for param_name, value in bound_args.arguments.items():
            #     print(f"  {param_name} = {value}")
        
            self.workflow_payload = bound_args.arguments.get('data')

            self.status = WorkflowStatus.INSTANTIATED
            # Set workflow-level retry policy
            self.retry_policy = retry_policy or LinearRetryPolicy()
            
            # Call original init
            original_init(self, *args, **kwargs)

            # Create workflow execution record
            workflow_instance = WorkflowInstance(
                workflow_id=self.__class__.__name__,
                workflow_name=self.__class__.__name__,
                status=WorkflowStatus.INSTANTIATED,
                start_time=datetime.now(),
                payload_data=self.workflow_payload
            )
            session = Session()
            session.add(workflow_instance)
            session.commit()
            self.wf_instance_id = workflow_instance.id # allocated when DB record is created
            session.close()

        workflow_class.__init__ = new_init
        
        # Wrap the execute method
        if hasattr(workflow_class, 'execute'):
            workflow_class.execute = _wrap_workflow_execute(workflow_class.execute)
        
        return workflow_class
    
    return decorator

def _wrap_workflow_execute(execute_method):
    """Wrap workflow execute method with tracking and context setup"""
    
    # Check if the execute method is async and create appropriate wrapper
    if asyncio.iscoroutinefunction(execute_method):
        @functools.wraps(execute_method)
        async def async_wrapper(self, *args, **kwargs):
            session = Session()
            
            # Create context with workflow's retry policy
            context = WorkflowContext(
                retry_policy=self.retry_policy
            )
            context.session = session
            
            workflow_instance = session.get(WorkflowInstance, self.wf_instance_id)
            context.workflow_instance = workflow_instance
            set_current_context(context)
            
            try:
                # Update status to RUNNING
                workflow_instance.set_status(WorkflowStatus.RUNNING)
                session.commit()
                
                print(f"#### 🚀 Starting async workflow {self.__class__.__name__} with retry policy: {type(self.retry_policy).__name__}")
                
                # Execute the async workflow
                result = await execute_method(self, *args, **kwargs)
                
                # Mark as completed
                workflow_instance.set_status(WorkflowStatus.COMPLETED)
                workflow_instance.end_time = datetime.now()
                session.commit()
                
                print(f"###v ✅ Async workflow {self.__class__.__name__} completed successfully")
                return result
                
            except Exception as e:
                # Mark as failed
                workflow_instance.set_status(WorkflowStatus.FAILED)
                workflow_instance.end_time = datetime.now()
                workflow_instance.error_message = str(e)
                session.commit()
                print(f"###! ❌ Async workflow {self.__class__.__name__} failed: {str(e)}")
                raise
            finally:
                session.close()
                # Clear context
                set_current_context(None)
        
        return async_wrapper
    else:
        @functools.wraps(execute_method)
        def sync_wrapper(self, *args, **kwargs):
            session = Session()
            
            # Create context with workflow's retry policy
            context = WorkflowContext(
                retry_policy=self.retry_policy
            )
            context.session = session
            
            workflow_instance = session.get(WorkflowInstance, self.wf_instance_id)
            context.workflow_instance = workflow_instance
            set_current_context(context)
            
            try:
                # Update status to RUNNING
                workflow_instance.set_status(WorkflowStatus.RUNNING)
                session.commit()
                
                print(f"#### 🚀 Starting sync workflow {self.__class__.__name__} with retry policy: {type(self.retry_policy).__name__}")
                
                # Execute the sync workflow
                result = execute_method(self, *args, **kwargs)
                
                # Mark as completed
                workflow_instance.set_status(WorkflowStatus.COMPLETED)
                workflow_instance.end_time = datetime.now()
                session.commit()
                
                print(f"###v ✅ Sync workflow {self.__class__.__name__} completed successfully")
                return result
                
            except Exception as e:
                # Mark as failed
                workflow_instance.set_status(WorkflowStatus.FAILED)
                workflow_instance.end_time = datetime.now()
                workflow_instance.error_message = str(e)
                session.commit()
                print(f"###! ❌ Sync workflow {self.__class__.__name__} failed: {str(e)}")
                raise
            finally:
                session.close()
                # Clear context
                set_current_context(None)
        
        return sync_wrapper

############################
# Workstep Definition
############################
def workstep_defn(retry_policy: RetryPolicy = None, 
                    step_id: str = None,
                    bian_sd: str = "UNKNOWN?",
                    payload: Dict[str, Any] = None):
    """
    Decorator for WorkStep methods to track lifecycle and handle retries
    
    Args:
        retry_policy: Optional step-specific retry policy (overrides workflow policy)
        step_id: Optional custom step ID
        payload: Optional step payload data
    """
    
    def decorator(func):
        # Check if the function is async
        is_async = inspect.iscoroutinefunction(func)
        
        if is_async:
            @functools.wraps(func)
            async def async_wrapper(self, *args, **kwargs):
                # Get dynamic step_id from wrapper's attribute if set, otherwise use decorator parameter
                dynamic_step_id = getattr(async_wrapper, 'step_id', step_id)
                return await _execute_workstep_wrapper(self, func, args, kwargs, retry_policy, dynamic_step_id, bian_sd, payload, is_async)
            
            # Store the original step_id and allow it to be modified
            async_wrapper.step_id = step_id
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(self, *args, **kwargs):
                # Get dynamic step_id from wrapper's attribute if set, otherwise use decorator parameter
                dynamic_step_id = getattr(sync_wrapper, 'step_id', step_id)
                return _execute_workstep_wrapper(self, func, args, kwargs, retry_policy, dynamic_step_id, bian_sd, payload, is_async)
            
            # Store the original step_id and allow it to be modified
            sync_wrapper.step_id = step_id
            return sync_wrapper
    
    return decorator

def _execute_workstep_wrapper(self, func, args, kwargs, retry_policy, step_id, bian_sd, payload, is_async):
    """Common wrapper logic for both sync and async worksteps"""
    # Find and store payload data
    sig = inspect.signature(func)
    bound_args = sig.bind(self, *args, **kwargs)
    bound_args.apply_defaults()
    
    payload = bound_args.arguments.get('payload')
    context = get_current_context()
    
    # Determine which retry policy to use
    effective_retry_policy = _determine_retry_policy(
        step_retry_policy=retry_policy,
        instance_retry_policy=getattr(self, 'retry_policy', None),
        workflow_context=context
    )
    
    if not context or not context.session:
        # If no workflow context, create a standalone execution
        if is_async:
            return asyncio.run(_execute_standalone_workstep_async(
                self, func, args, kwargs, effective_retry_policy, step_id, bian_sd, payload
            ))
        else:
            return _execute_standalone_workstep(
                self, func, args, kwargs, effective_retry_policy, step_id, bian_sd, payload
            )
    
    if is_async:
        return asyncio.run(_execute_workstep_in_workflow_async(
            self, func, args, kwargs, effective_retry_policy, step_id, bian_sd, payload, context
        ))
    else:
        return _execute_workstep_in_workflow(
            self, func, args, kwargs, effective_retry_policy, step_id, bian_sd, payload, context
        )

def _determine_retry_policy(step_retry_policy: Optional[RetryPolicy],
                          instance_retry_policy: Optional[RetryPolicy],
                          workflow_context: Optional[WorkflowContext]) -> RetryPolicy:
    """
    Determine which retry policy to use based on precedence:
    1. Explicit step-level retry policy (highest priority)
    2. Instance-level retry policy
    3. Workflow-level retry policy 
    4. Default retry policy (lowest priority)
    """
    
    # 1. Explicit step-level retry policy
    if step_retry_policy is not None:
        print(f"\t...Using explicit step retry policy: {type(step_retry_policy).__name__}")
        return step_retry_policy
    
    # 2. Instance-level retry policy
    if instance_retry_policy is not None:
        print(f"\t...Using instance retry policy: {type(instance_retry_policy).__name__}")
        return instance_retry_policy
    
    # 3. Workflow-level retry policy (if inheriting)
    if workflow_context and workflow_context.workflow_retry_policy:
        print(f"\t...Inheriting workflow retry policy: {type(workflow_context.workflow_retry_policy).__name__}")
        return workflow_context.workflow_retry_policy
    
    # 4. Default retry policy
    default_policy = LinearRetryPolicy()
    print(f"\t...Using default retry policy: {type(default_policy).__name__}")
    return default_policy

def _execute_standalone_workstep(instance, func, args, kwargs, retry_policy, step_id, bian_sd, payload):
    """Execute work step without workflow context (standalone)"""
    session = Session()
    actual_step_id = step_id or f"{instance.__class__.__name__}.{func.__name__}"
    
    print(f"## 🚀 Executing standalone work step: {actual_step_id}")
    
    try:
        return _execute_with_retry(
            instance, func, args, kwargs, retry_policy,
            actual_step_id, session, None, bian_sd, payload
        )
    finally:
        session.close()

async def _execute_standalone_workstep_async(instance, func, args, kwargs, retry_policy, step_id, bian_sd, payload):
    """Execute async work step without workflow context (standalone)"""
    session = Session()
    actual_step_id = step_id or f"{instance.__class__.__name__}.{func.__name__}"
    
    print(f"## 🚀 Executing standalone async work step: {actual_step_id}")
    
    try:
        return await _execute_with_retry_async(
            instance, func, args, kwargs, retry_policy,
            actual_step_id, session, None, bian_sd, payload
        )
    finally:
        session.close()

def _execute_workstep_in_workflow(instance, func, args, kwargs, retry_policy,
                                 step_id, bian_sd, payload, context):
    """Execute work step within workflow context"""
    session = context.session
    actual_step_id = step_id or f"{instance.__class__.__name__}.{func.__name__}"
    
    print(f"  ## 🚀 Executing work step in workflow: {actual_step_id}")
    
    return _execute_with_retry(
        instance, func, args, kwargs, retry_policy,
        actual_step_id, session, context.workflow_instance, bian_sd, payload
    )

async def _execute_workstep_in_workflow_async(instance, func, args, kwargs, retry_policy,
                                             step_id, bian_sd, payload, context):
    """Execute async work step within workflow context"""
    session = context.session
    actual_step_id = step_id or f"{instance.__class__.__name__}.{func.__name__}"
    
    print(f"  ## 🚀 Executing async work step in workflow: {actual_step_id}")
    
    return await _execute_with_retry_async(
        instance, func, args, kwargs, retry_policy,
        actual_step_id, session, context.workflow_instance, bian_sd, payload
    )

def _execute_with_retry(instance, func, args, kwargs, retry_policy: RetryPolicy,
                       step_id: str, session, workflow_instance: Optional[int],
                       bian_sd:str, payload: Dict[str, Any]):
    """Execute function with retry logic and database tracking"""
    
    # Create work step execution record
    step_instance = WorkStepInstance(
        workflow_instance_id=workflow_instance.id,
        step_id=step_id,
        step_name=func.__name__,
        bian_sd=bian_sd,
        status=WorkStepStatus.INSTANTIATED,
        max_retries=retry_policy.max_retries,
        retry_delay=int(retry_policy.base_delay),
        payload_data=json.dumps(payload or {})
    )
    
    session.add(step_instance)
    session.commit()
    
    attempt = 1
    last_exception = None
    
    print(f"  ## 👣 Step {step_id}: Starting execution with {type(retry_policy).__name__} (max_retries={retry_policy.max_retries})")
    
    while attempt <= retry_policy.max_retries + 1:  # +1 for initial attempt
        try:
            # Update attempt info
            step_instance.attempt_number = attempt
            # step_instance.status = WorkStepStatus.RUNNING # if attempt == 1 else 'RETRYING'
            step_instance.set_status(WorkStepStatus.RUNNING)
            step_instance.start_time = datetime.now()
            session.commit()
            
            print(f"\tStep {step_id}: Attempt {attempt}")
            
            # Execute the function (sync)
            result = func(instance, *args, **kwargs)
            
            # Mark as completed
            # step_instance.status = WorkStepStatus.COMPLETED
            step_instance.set_status(WorkStepStatus.COMPLETED)
            step_instance.end_time = datetime.now()
            step_instance.result_data = json.dumps(_serialize_result(result))
            session.commit()
            
            print(f"  #v ✅ Step {step_id}: Completed successfully on attempt {attempt}")
            return result
            
        except Exception as e:
            last_exception = e
            step_instance.error_message = str(e)
            step_instance.end_time = datetime.now()
            
            # Check if we should retry
            if retry_policy.should_retry(attempt, e) and attempt <= retry_policy.max_retries:
                delay = retry_policy.get_delay(attempt)
                step_instance.set_status(WorkStepStatus.RUNNING)
                session.commit()
                
                print(f"  #! 💔 Step {step_id}: Failed on attempt {attempt}, retrying in {delay:.2f}s: {str(e)}")
                time.sleep(delay)
                attempt += 1
            else:
                # No more retries
                step_instance.set_status(WorkStepStatus.FAILED)
                session.commit()
                print(f"  #! ❌ Step {step_id}: Failed after {attempt} attempts: {str(e)}")
                raise e
    
    # This shouldn't be reached, but just in case
    step_instance.set_status(WorkStepStatus.FAILED)
    session.commit()
    raise last_exception

async def _execute_with_retry_async(instance, func, args, kwargs, retry_policy: RetryPolicy,
                                   step_id: str, session, workflow_instance: Optional[int],
                                   bian_sd:str, payload: Dict[str, Any]):
    """Execute async function with retry logic and database tracking"""
    
    # Create work step execution record
    step_instance = WorkStepInstance(
        workflow_instance_id=workflow_instance.id,
        step_id=step_id,
        step_name=func.__name__,
        bian_sd=bian_sd,
        status=WorkStepStatus.INSTANTIATED,
        max_retries=retry_policy.max_retries,
        retry_delay=int(retry_policy.base_delay),
        payload_data=json.dumps(payload or {})
    )
    
    session.add(step_instance)
    session.commit()
    
    attempt = 1
    last_exception = None
    
    print(f"  ## 👣 Step {step_id}: Starting async execution with {type(retry_policy).__name__} (max_retries={retry_policy.max_retries})")
    
    while attempt <= retry_policy.max_retries + 1:  # +1 for initial attempt
        try:
            # Update attempt info
            step_instance.attempt_number = attempt
            step_instance.set_status(WorkStepStatus.RUNNING)
            step_instance.start_time = datetime.now()
            session.commit()
            
            print(f"\tStep {step_id}: Async attempt {attempt}")
            
            # Execute the async function
            result = await func(instance, *args, **kwargs)
            
            # Mark as completed
            step_instance.set_status(WorkStepStatus.COMPLETED)
            step_instance.end_time = datetime.now()
            step_instance.result_data = json.dumps(_serialize_result(result))
            session.commit()
            
            print(f"  #v ✅ Step {step_id}: Completed successfully on async attempt {attempt}")
            return result
            
        except Exception as e:
            last_exception = e
            step_instance.error_message = str(e)
            step_instance.end_time = datetime.now()
            
            # Check if we should retry
            if retry_policy.should_retry(attempt, e) and attempt <= retry_policy.max_retries:
                delay = retry_policy.get_delay(attempt)
                step_instance.set_status(WorkStepStatus.RUNNING)
                session.commit()
                
                print(f"  #! 💔 Step {step_id}: Failed on async attempt {attempt}, retrying in {delay:.2f}s: {str(e)}")
                await asyncio.sleep(delay)  # Use async sleep for async functions
                attempt += 1
            else:
                # No more retries
                step_instance.set_status(WorkStepStatus.FAILED)
                session.commit()
                print(f"  #! ❌ Step {step_id}: Failed after {attempt} async attempts: {str(e)}")
                raise e
    
    # This shouldn't be reached, but just in case
    step_instance.set_status(WorkStepStatus.FAILED)
    session.commit()
    raise last_exception

def _serialize_result(result: Any) -> Any:
    """Serialize result for JSON storage"""
    try:
        json.dumps(result, ensure_ascii=False)  # support emojies
        return result
    except (TypeError, ValueError):
        return str(result)

