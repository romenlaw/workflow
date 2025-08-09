from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, ForeignKey, Sequence, TypeDecorator
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from enum import Enum
from typing import List
import uuid
from datetime import datetime

class WorkflowStatus(Enum):
    INSTANTIATED = "Initiated"
    RUNNING = "Running"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    PENDING = "Pending"

    def is_final_state(self):
        return self in [self.CANCELLED, self.COMPLETED ]
    
    def next_states(self):
        valid_transitions = {
            self.INSTANTIATED: [self.RUNNING],
            self.RUNNING: [self.FAILED, self.COMPLETED, self.PENDING, self.CANCELLED ],
            self.FAILED: [self.RUNNING, self.CANCELLED],
            self.PENDING: [self.COMPLETED, self.CANCELLED]
        }
        return valid_transitions[self]
    
class WorkStepStatus(Enum):
    INSTANTIATED = "Initiated"
    RUNNING = "Running"
    COMPLETED = "Completed"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    WAITING = "Waiting"
    PENDING_COMPLETION = "Pending Completion"

    def is_final_state(self)->bool:
        return self in [self.CANCELLED, self.COMPLETED ]
    
    def next_states(self)->List['WorkStepStatus']:
        valid_transitions = {
            self.INSTANTIATED: [self.RUNNING],
            self.RUNNING: [self.WAITING, self.FAILED, self.COMPLETED, self.PENDING_COMPLETION, self.CANCELLED ],
            self.WAITING: [self.RUNNING, self.CANCELLED],
            self.FAILED: [self.RUNNING, self.CANCELLED],
            self.PENDING_COMPLETION: [self.COMPLETED, self.CANCELLED]
        }
        return valid_transitions[self]
    


Base = declarative_base()
# Database setup
engine = create_engine('duckdb:///db/workflow.db', echo=False)
# engine = create_engine('duckdb:///:memory:')
Session = sessionmaker(bind=engine)
session = Session()

# Create custom TypeDecorator for Enum
class EnumAsString(TypeDecorator):
    """Custom type to store Enum as string in database"""
    
    impl = String  # Use String as the underlying type
    cache_ok = True
    
    def __init__(self, enum_class, *args, **kwargs):
        self.enum_class = enum_class
        super().__init__(*args, **kwargs)
    
    def process_bind_param(self, value, dialect):
        """Convert Python Enum to string for database storage"""
        if value is None:
            return None
        if isinstance(value, self.enum_class):
            return value.value
        # If it's already a string, return as-is
        return value
    
    def process_result_value(self, value, dialect):
        """Convert string from database back to Python Enum"""
        if value is None:
            return None
        try:
            return self.enum_class(value)
        except ValueError:
            # Handle case where database has invalid enum value
            print(f"Warning: Invalid enum value '{value}' for {self.enum_class.__name__}")
            return None


class WorkflowInstance(Base):
    """A WorkflowInstance record is created for each workflow that was run / executed. """
    __tablename__ = 'workflow_instance'
    
    id = Column(Integer, Sequence('wfi_id_seq'), primary_key=True) # auto-generated primary key of the table. It's a technical field only, no business meaning.
    workflow_id = Column(String(255), nullable=False) # ID of the workflow definition. It's meant for human / business consumption.
    workflow_name = Column(String(255), nullable=False) # name of the workflow definition
    status = Column(EnumAsString(WorkflowStatus), nullable=False)  # status of the workflow instance
    start_time = Column(DateTime, nullable=False) # local date time of when the workflow was instantiated / created.
    end_time = Column(DateTime) # local date time of when the workflow instance reached one of its final states (see WorkStepStatus class for details)
    error_message = Column(Text) # stores the error message if the workflow end up as failed status.
    payload_data = Column(Text)  # JSON message of the business data that was provided as input for the workflow instance.
    
    # Relationship to work steps
    work_steps = relationship(
        "WorkStepInstance", 
        back_populates="workflow",
        cascade="all, delete-orphan",  # Cascade deletes
        lazy="select"  # Explicit loading strategy
    )

    def set_status(self, new_status: WorkflowStatus)->None:
        """everytime the status changes, we want to log it in the workflow_instance_lifecycle table"""
        old_status = self.status
        self.status = new_status
        if old_status != new_status:
            lc = WorkflowLifecycle(
                workflow_instance_id = self.id,
                from_state = old_status,
                to_state = new_status,
                change_dt = datetime.now(),
                changed_by = 'auto',
            )
            session.add(lc)
            session.commit()
    
    def __repr__(self):
        return f"<WorkflowInstance(id={self.id}, name='{self.workflow_name}', status={self.status})>"

class WorkStepInstance(Base):
    """A workflow consists one or more worksteps. A WorkStepInstance is an instantiation of a workstep."""
    __tablename__ = 'workstep_instance'
    
    id = Column(Integer, Sequence('wfs_id_seq'), primary_key=True) # auto-generated primary key of the table. It's a technical field only, no business meaning.
    workflow_instance_id = Column(Integer, ForeignKey('workflow_instance.id')) # points to the workflow_instance record to which this workstep belongs.
    step_id = Column(String(255), nullable=False) # business level ID of the workstep; meant for human consumption
    step_name = Column(String(255), nullable=False) # name of the workstep
    bian_sd = Column(String(255), nullable=False) # identifies which BIAN service domain that owns this step, so that it can be used to raise service management tickets if the step failed.
    status = Column(EnumAsString(WorkStepStatus), nullable=False)  # status of the workstep, see WorkStepStatus class for more details.
    start_time = Column(DateTime) # local date time when the workstep was instantiated.
    end_time = Column(DateTime) # local date time when the workstep reached one of its final statuses - see WorkStepStatus class for details.
    attempt_number = Column(Integer, default=1) # if retry (due to technical failure) was attempted, this field records number of retries made.
    max_retries = Column(Integer, default=0) # maximum number of retries to be made when encountering technical failures in the workstep.
    retry_delay = Column(Integer, default=0) # retry delay in seconds
    error_message = Column(Text) # stores the error message if the workstep end up as failed status.
    result_data = Column(Text)  # JSON message storing the output data of the workstep
    payload_data = Column(Text)  # JSON message storing the input data of the workstep
    
    # Relationship to workflow
    workflow = relationship(
        "WorkflowInstance", 
        back_populates="work_steps"
    )

    def set_status(self, new_status: WorkflowStatus)->None:
        """everytime the status changes, we want to log it in the workstep_instance_lifecycle table"""
        old_status = self.status
        self.status = new_status
        if old_status != new_status:
            lc = WorkstepLifecycle(
                workstep_instance_id = self.id,
                from_state = old_status,
                to_state = new_status,
                change_dt = datetime.now(),
                changed_by = 'auto',
            )
            session.add(lc)
            session.commit()

    def __repr__(self):
        return f"<WorkStepInstance(id={self.id}, name='{self.step_name}', status={self.status})>"

class WorkflowLifecycle(Base):
    """Every time a workflow has its lifecycle transition (i.e. status change), the details of the lifecycle change is stored in this record."""
    __tablename__ = 'workflow_instance_lifecycle'

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4())) # auto-generated primary key of the table. It's a technical field only, no business meaning.
    workflow_instance_id = Column(Integer, ForeignKey('workflow_instance.id')) # reference to the workflow instance record
    from_state = Column(EnumAsString(WorkflowStatus), nullable=False) # the status before the change
    to_state = Column(EnumAsString(WorkflowStatus), nullable=False) # the status after the change
    change_dt = Column(DateTime) # local date time when the status change occurred
    changed_by = Column(Text) # user or system that initiated the change
    notes = Column(Text)  # any notes about the change, which is meant for human consumption

class WorkstepLifecycle(Base):
    __tablename__ = 'workstep_instance_lifecycle'

    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4())) # auto-generated primary key of the table. It's a technical field only, no business meaning.
    workstep_instance_id = Column(Integer, ForeignKey('workstep_instance.id')) # reference to the workstep instance record
    from_state = Column(EnumAsString(WorkStepStatus), nullable=False) # the status before the change
    to_state = Column(EnumAsString(WorkStepStatus), nullable=False) # the status after the change
    change_dt = Column(DateTime) # local date time when the status change occurred
    changed_by = Column(Text) # user or system that initiated the change
    notes = Column(Text) # any notes about the change, which is meant for human consumption

Base.metadata.create_all(engine)
