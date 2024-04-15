"""
TODO: Add benches for higher number of tasks; blocked by engine deadlocks in CI.
"""

import anyio
import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from prefect import flow, task


def noop_function():
    pass


async def anoop_function():
    pass


def bench_flow_decorator(benchmark: BenchmarkFixture):
    benchmark(flow, noop_function)


@pytest.mark.parametrize("options", [{}, {"timeout_seconds": 10}])
def bench_flow_call(benchmark: BenchmarkFixture, options):
    noop_flow = flow(**options)(noop_function)
    benchmark(noop_flow)


def bench_flow_with_submitted_tasks(benchmark: BenchmarkFixture):
    test_task = task(noop_function)

    @flow
    def benchmark_flow():
        test_task.submit()

    benchmark(benchmark_flow)


def bench_flow_with_called_tasks(benchmark: BenchmarkFixture):
    test_task = task(noop_function)

    @flow
    def benchmark_flow():
        test_task()

    benchmark(benchmark_flow)


def bench_async_flow_with_async_tasks(benchmark: BenchmarkFixture):
    test_task = task(anoop_function)

    @flow
    async def benchmark_flow():
        async with anyio.create_task_group() as tg:
            tg.start_soon(test_task)

    benchmark(anyio.run, benchmark_flow)


def bench_async_flow_with_submitted_sync_tasks(benchmark: BenchmarkFixture):
    test_task = task(noop_function)

    @flow
    async def benchmark_flow():
        test_task.submit()

    benchmark(anyio.run, benchmark_flow)


def bench_flow_with_subflows(benchmark: BenchmarkFixture):
    test_flow = flow(noop_function)

    @flow
    def benchmark_flow():
        test_flow()

    benchmark(benchmark_flow)


def bench_async_flow_with_sequential_subflows(benchmark: BenchmarkFixture):
    test_flow = flow(anoop_function)

    @flow
    async def benchmark_flow():
        await test_flow()

    benchmark(anyio.run, benchmark_flow)


def bench_async_flow_with_concurrent_subflows(benchmark: BenchmarkFixture):
    test_flow = flow(anoop_function)

    @flow
    async def benchmark_flow():
        async with anyio.create_task_group() as tg:
            tg.start_soon(test_flow)

    benchmark(anyio.run, benchmark_flow)
