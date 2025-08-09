from datetime import datetime, timedelta
from .models import Session, WorkflowInstance, WorkStepInstance

def get_workflow_instance_summary(workflow_id: str = None):
    """Get summary of workflow executions"""
    session = Session()
    try:
        query = session.query(WorkflowInstance)
        if workflow_id:
            query = query.filter(WorkflowInstance.workflow_id == workflow_id)
        
        executions = query.order_by(WorkflowInstance.start_time.desc()).all()
        
        for execution in executions:
            print(f"\nWorkflow: {execution.workflow_name} ({execution.workflow_id})")
            print(f"Status: {execution.status}")
            print(f"Started: {execution.start_time}")
            if execution.end_time:
                duration = execution.end_time - execution.start_time
                print(f"Duration: {duration.total_seconds():.2f}s")
            
            # Show work steps
            for step in execution.work_steps:
                print(f"  Step: {step.step_name} - {step.status}")
                if step.attempt_number > 1:
                    print(f"    Attempts: {step.attempt_number}/{step.max_retries + 1}")
                if step.error_message:
                    print(f"    Error: {step.error_message}")
    
    finally:
        session.close()

def get_step_instance_details(step_id: str):
    """Get detailed execution history for a specific step"""
    session = Session()
    try:
        steps = session.query(WorkStepInstance)\
                      .filter(WorkStepInstance.step_id == step_id)\
                      .order_by(WorkStepInstance.start_time.desc())\
                      .all()
        
        print(f"\nExecution history for step: {step_id}")
        for step in steps:
            print(f"  {step.start_time} - {step.status} (Attempt {step.attempt_number})")
            if step.error_message:
                print(f"    Error: {step.error_message}")
    
    finally:
        session.close()

def cleanup_old_instances(days_old: int = 30):
    """Clean up old execution records"""
    session = Session()
    try:
        cutoff_date = datetime.now() - timedelta(days=days_old)
        
        old_executions = session.query(WorkflowInstance)\
                               .filter(WorkflowInstance.start_time < cutoff_date)\
                               .all()
        
        for execution in old_executions:
            session.delete(execution)
        
        session.commit()
        print(f"Cleaned up {len(old_executions)} old execution records")
    
    finally:
        session.close()

# Example queries
if __name__ == "__main__":
    print("=== Workflow Execution Summary ===")
    get_workflow_instance_summary()
    
    print("\n=== Step Execution Details ===")
    get_step_instance_details("data_extraction")
