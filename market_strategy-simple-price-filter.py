import logging
import pathlib
import sys

from yapapi import Golem, Task, WorkContext
from yapapi.payload import vm
from yapapi.strategy import SCORE_TRUSTED, MarketStrategy, SCORE_NEUTRAL, SCORE_REJECTED

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))

from utils import build_parser, print_env_info, run_golem_example  # noqa

#   Image based on pure python:3.8-alpine
IMAGE_HASH = "5c385688be6ed4e339a43d8a68bfb674d60951b4970448ba20d1934d"

#   This is the task we'll be running on the provider
TASK_CMD = ["/usr/local/bin/python", "-c", "for i in range(10000000): i * 7"]


class MyStrategy(MarketStrategy):
    def __init__(self, max_prices):
        self._max_cpu_price = max_prices['max_cpu_price']
        self._max_dur_price = max_prices['max_dur_price']
        self._max_str_price = max_prices['max_str_price']
        self._logger = logging.getLogger(f"yapapi.my-strategy")

    async def score_offer(self, offer):
        provider_name = offer.props['golem.node.id.name']

        self._logger.info(f"Received offer from: {provider_name}")

        pricing_coeffs = offer.props['golem.com.pricing.model.linear.coeffs']
        usage_vector = offer.props['golem.com.usage.vector']

        # Find indexes of CPU and env usage in the usage vector
        # to match them with the pricing coefficients later
        price_cpu_idx = -1
        price_dur_idx = -1
        for idx, val in enumerate(usage_vector):
            if val == 'golem.usage.cpu_sec':
                self._logger.debug(f"CPU usage found at index {idx}")
                price_cpu_idx = idx
            elif val == 'golem.usage.duration_sec':
                self._logger.debug(f"Duration sec found at index {idx}")
                price_dur_idx = idx
            else:
                self._logger.warning(f"Unused usage vector element: {val}")

        if price_cpu_idx == -1 or price_dur_idx == -1:
            self._logger.error(f"ERROR: CPU or duration_sec not found in the usage vector")
            return SCORE_REJECTED

        price_cpu = pricing_coeffs[price_cpu_idx]
        price_dur = pricing_coeffs[price_dur_idx]
        # start price is by design the last element of the pricing coefficients
        price_start = pricing_coeffs[-1]

        if price_cpu <= self._max_cpu_price and price_dur <= self._max_dur_price and price_start <= self._max_str_price:
            score = SCORE_TRUSTED
        else:
            score = SCORE_REJECTED

        self._logger.info(
            f"Proposal from: {provider_name}, "
            f"cpu: {price_cpu:.6f}, "
            f"dur: {price_dur:.6f}, "
            f"start: {price_start:.6f}), "
            f"score: {score}")

        return score


async def main(subnet_tag, payment_driver, payment_network):
    logger = logging.getLogger("yapapi.example-main")
    payload = await vm.repo(image_hash=IMAGE_HASH)

    max_prices = {
        'max_cpu_price': 0.00001,
        'max_dur_price': 0.00001,
        'max_str_price': 0.0,
    }

    strategy = MyStrategy(max_prices)

    async def worker(ctx: WorkContext, tasks):
        async for task in tasks:
            script = ctx.new_script()
            future_result = script.run("/usr/bin/time", "-p", *TASK_CMD)
            yield script

            real_time_str = future_result.result().stderr.split()[1]
            real_time = float(real_time_str)

            # strategy.save_execution_time(ctx.provider_id, real_time)
            logger.info(f"TASK EXECUTED {ctx.provider_name} {ctx.provider_id} {real_time}")

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
        # tasks = (Task(None) for _ in itertools.count(1))
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
