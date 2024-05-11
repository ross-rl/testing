import abc
from typing import Union, overload

import runloop
from runloop import FunctionCompleteFulfillmentResult, FunctionOutput, FunctionInvocation, Latch, LatchResultType, \
    FunctionCompleteFulfillment, TimeFulfillment, ApiFulfillment, LatchType, TimeFulfillmentResult, ApiFulfillmentResult


@runloop.function
def add(a: int, b: int) -> int:
    return a + b


@runloop.function
def subtract(a: int, b: int) -> int:
    return a - b


class TestLatch(Latch):
    def __init__(self, fulfillment: FunctionCompleteFulfillmentResult):
        self._fulfillment = fulfillment

    def await_result(self) -> LatchResultType:
        return self._fulfillment


class TestScheduler(runloop.Scheduler):
    def __init__(self):
        super().__init__()

    @overload
    @abc.abstractmethod
    def create_latch(self, latch_name: str, fulfillment: TimeFulfillment) -> Latch[TimeFulfillmentResult]:
        raise NotImplementedError()

    @overload
    @abc.abstractmethod
    def create_latch(
        self, latch_name: str, fulfillment: FunctionCompleteFulfillment[FunctionOutput]
    ) -> Latch[FunctionCompleteFulfillmentResult[FunctionOutput]]:
        raise NotImplementedError()

    @overload
    @abc.abstractmethod
    def create_latch(
        self, latch_name: str, fulfillment: ApiFulfillment[LatchType]
    ) -> Latch[ApiFulfillmentResult[LatchType]]:
        raise NotImplementedError()

    def schedule_at_time(self, function_invocation: FunctionInvocation, scheduled_time_ms: int) -> str:
        raise NotImplementedError()

    def launch(
            self,
            function_invocation: FunctionInvocation[FunctionOutput]
    ) -> Latch[FunctionCompleteFulfillmentResult[FunctionOutput]]:
        return TestLatch(
            fulfillment=FunctionCompleteFulfillmentResult(
                output=function_invocation.invoke()
            )
        )


@runloop.function
def schedule_calculations(scheduler: runloop.Scheduler, a: int, b: int) -> str:

    fn1 = add(a, b)
    fn2 = subtract(a, b)

    latches = [scheduler.launch(fn) for fn in [fn1, fn2]]
    results = [latch.await_result() for latch in latches]

    print(f"results: {[x.output for x in results]}")

    return "hello"


if __name__ == "__main__":
    print("Hello, World!")

    scheduler = TestScheduler()
    schedule_calculations(scheduler, 2, 1).invoke()
