#!/usr/bin/env python3
import itertools
import pathlib
import sys
from collections import defaultdict

from ya_market import rest

from yapapi import Golem, Task, WorkContext
from yapapi.payload import vm
from yapapi.strategy import SCORE_TRUSTED, MarketStrategy, LeastExpensiveLinearPayuMS, SCORE_REJECTED

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import build_parser, print_env_info, run_golem_example  # noqa

#   Image based on pure python:3.8-alpine
IMAGE_HASH = "5c385688be6ed4e339a43d8a68bfb674d60951b4970448ba20d1934d"

#   This is the task we'll be running on the provider
TASK_CMD = ["/usr/local/bin/python", "-c", "for i in range(10000000): i * 7"]


class MyStrategy(LeastExpensiveLinearPayuMS):
    def __init__(self,
                 expected_time_secs=60,
                 max_fixed_price=None,
                 max_price_for=None,
                 ):
        super().__init__(expected_time_secs, max_fixed_price, max_price_for)

    async def score_offer(self, offer):
        #print(f"Offer: {offer.props['golem.com.pricing.model.linear.coeffs']}")
        #print(f"Offer: {offer.props}")
        #'golem.com.usage.vector': ['golem.usage.cpu_sec', 'golem.usage.duration_sec']
        #'golem.com.pricing.model': 'linear', 'golem.com.pricing.model.linear.coeffs'
        #'golem.node.id.name': 'testnet-c1-8'
        pricing_cooeffs = offer.props['golem.com.pricing.model.linear.coeffs']
        usage_vector = offer.props['golem.com.usage.vector']

        # Find indexes of CPU and env usage in the usage vector
        # to match them with the pricing coefficients later
        price_cpu_idx = -1
        price_env_idx = -1
        for idx, val in enumerate(usage_vector):
            if val == 'golem.usage.cpu_sec':
                price_cpu_idx = idx
            elif val == 'golem.usage.duration_sec':
                price_env_idx = idx
            else:
                print(f"Unused usage vector element: {val}")

        if price_cpu_idx == -1 or price_env_idx == -1:
            print(f"ERROR: CPU or env usage not found in the usage vector")
            return SCORE_REJECTED

        provider_name = offer.props['golem.node.id.name']

        price_cpu = pricing_cooeffs[price_cpu_idx]
        price_env = pricing_cooeffs[price_env_idx]
        # start price is by design the last element of the pricing coefficients
        price_start = pricing_cooeffs[-1]

        print(f"Proposal from: {provider_name}, CPU: {price_cpu}, env {price_env}, START {price_start}")
            
        return await super().score_offer(offer)


async def main(subnet_tag, payment_driver, payment_network):
    payload = await vm.repo(image_hash=IMAGE_HASH)
    counter_caps = {
        "golem.usage.cpu_sec": 0.00001,
        "golem.usage.duration_sec": 0.00001,
    }
    strategy = MyStrategy(
        max_fixed_price=0.0,
        max_price_for=counter_caps
    )

    async def worker(ctx: WorkContext, tasks):
        async for task in tasks:
            script = ctx.new_script()
            future_result = script.run("/usr/bin/time", "-p", *TASK_CMD)
            yield script

            real_time_str = future_result.result().stderr.split()[1]
            real_time = float(real_time_str)

            #strategy.save_execution_time(ctx.provider_id, real_time)
            print("TASK EXECUTED", ctx.provider_name, ctx.provider_id, real_time)

            task.accept_result()

            #   We want to test as many different providers as possible, so here we tell
            #   the Golem engine to stop computations in this agreement (and thus to look
            #   for a new agreement, maybe with a new provider).
            await tasks.aclose()

    async with Golem(
            budget=10,
            strategy=strategy,
            subnet_tag=subnet_tag,
            payment_driver=payment_driver,
            payment_network=payment_network,
    ) as golem:
        print_env_info(golem)

        #   Task generator that never ends
        #tasks = (Task(None) for _ in itertools.count(1))
        tasks = (Task(None) for _ in range(10))
        async for task in golem.execute_tasks(worker, tasks, payload, max_workers=1):
            pass


if __name__ == "__main__":
    parser = build_parser("Select fastest provider using a simple reputation-based market strategy")
    parser.set_defaults(log_file="market-strategy-example.log")
    args = parser.parse_args()

    run_golem_example(
        main(
            subnet_tag=args.subnet_tag,
            payment_driver=args.payment_driver,
            payment_network=args.payment_network,
        ),
        log_file=args.log_file,
    )

# poetry install
# poetry run python examples/market-strategy/market_strategy.py --subnet-tag public --payment-network holesky