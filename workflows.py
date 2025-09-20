# @@@SNIPSTART python-money-transfer-project-template-workflows
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from activities import BankingActivities
    from shared import PaymentDetails


@workflow.defn
class MoneyTransfer:
    def __init__(self):
        self.activity_statuses = {}

    @workflow.query
    def get_activity_statuses(self) -> dict:
        return self.activity_statuses
    
    @workflow.run
    async def run(self, payment_details: PaymentDetails) -> str:

        retry_policy = RetryPolicy(
            maximum_attempts=3,
            maximum_interval=timedelta(seconds=2),
            non_retryable_error_types=["InvalidAccountError", "InsufficientFundsError"],
        )

        # Withdraw money
        self.activity_statuses["romen-withdraw"] = "pending"
        withdraw_output = await workflow.execute_activity_method( 
            BankingActivities.withdraw,
            payment_details,
            start_to_close_timeout=timedelta(seconds=5),
            retry_policy=retry_policy,
            activity_id="romen-withdraw",
        )
        self.activity_statuses["romen-withdraw"] = "done"

        # Deposit money
        try:
            self.activity_statuses["romen-deposit"] = "pending"
            deposit_output = await workflow.execute_activity_method( 
                BankingActivities.deposit,
                payment_details,
                start_to_close_timeout=timedelta(seconds=5),
                retry_policy=retry_policy,
                activity_id="romen-deposit",
            )
            self.activity_statuses["romen-deposit"] = "done"

            result = f"Transfer complete (transaction IDs: {withdraw_output}, {deposit_output})"
            return result
        except ActivityError as deposit_err:
            self.activity_statuses["romen-deposit"] = "failed"
            # Handle deposit error
            workflow.logger.error(f"Deposit failed: {deposit_err}")
            # Attempt to refund
            try:
                self.activity_statuses["romen-refund"] = "pending"
                refund_output = await workflow.execute_activity_method(
                    BankingActivities.refund,
                    payment_details,
                    start_to_close_timeout=timedelta(seconds=5),
                    retry_policy=retry_policy,
                    activity_id="romen-refund"
                )
                self.activity_statuses["romen-refund"] = "done"
                workflow.logger.info(
                    f"Refund successful. Confirmation ID: {refund_output}"
                )
            except ActivityError as refund_error:
                self.activity_statuses["romen-refund"] = "failed"
                workflow.logger.error(f"Refund failed: {refund_error}")
                raise refund_error from deposit_err

            # Re-raise deposit error if refund was successful
            raise deposit_err


# @@@SNIPEND
