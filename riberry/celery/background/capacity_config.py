from collections import Counter, defaultdict
from enum import Enum
from typing import Optional, Dict, List, Set

import itertools
from celery.utils.log import logger

from riberry import model


class ConsumerStatus(Enum):
    active = 'active'
    inactive = 'inactive'


class CapacityConsumer:

    def __init__(self, name: str, status: ConsumerStatus, requested_capacity: Optional[int] = None):
        self.name = name
        self.status = status
        self.requested_capacity = requested_capacity

    def __repr__(self):
        return f'CapacityConsumer(name={self.name!r})'

    @staticmethod
    def total_capacity(consumers, default=0):
        return sum(c.requested_capacity or default for c in consumers if c.status == ConsumerStatus.active)

    @classmethod
    def distribute(cls, consumers, total_capacity) -> Dict['CapacityConsumer', List[int]]:
        total_requested_capacity = cls.total_capacity(consumers=consumers, default=total_capacity) or total_capacity
        return {
            consumer: (
                [(consumer.requested_capacity or total_capacity) / total_requested_capacity * total_capacity]
                if consumer.status == ConsumerStatus.active else [0]
            )
            for consumer in consumers
        }

    @classmethod
    def from_weight_parameter(cls, parameter_name: str):
        schedules: List[model.application.ApplicationInstanceSchedule] = \
            model.application.ApplicationInstanceSchedule.query().filter_by(parameter=parameter_name).all()
        instances: Set[model.application.ApplicationInstance] = {sched.instance for sched in schedules}
        schedule_values = {
            instance.internal_name: int(instance.active_schedule_value(name=parameter_name, default=0))
            for instance in instances
        }
        execution_count = execution_count_for_instances(instances=instances)

        return [
            CapacityConsumer(
                name=instance.internal_name,
                status=(
                    ConsumerStatus.active
                    if instance.status == 'online' and execution_count[instance.internal_name]
                    else ConsumerStatus.inactive
                ),
                requested_capacity=schedule_values[instance.internal_name]
            ) for instance in instances
        ]


class CapacityProducer:

    def __init__(self, name: str, capacity: int):
        self.name = name
        self.capacity = capacity

    def __repr__(self):
        return f'CapacityProducer(name={self.name!r}, capacity={self.capacity})'

    @staticmethod
    def total_capacity(producers):
        return sum(p.capacity for p in producers)

    @staticmethod
    def producers_name_pool(producers, distribution_strategy: model.application.CapacityDistributionStrategy):
        name_lists = sorted(
            [[producer.name] * producer.capacity for producer in producers],
            key=len,
            reverse=True
        )

        if distribution_strategy == model.application.CapacityDistributionStrategy.spread:
            name_lists = [filter(None, name_list) for name_list in itertools.zip_longest(*name_lists)]

        return list(itertools.chain.from_iterable(name_lists))


def weighted_schedules(parameter_name: str):
    schedules = model.application.ApplicationInstanceSchedule.query().filter_by(parameter=parameter_name).all()
    return schedules


def execution_count_for_instances(instances, states=('ACTIVE', 'READY')):
    execution_count = defaultdict(int)
    job_executions: List[model.job.JobExecution] = model.job.JobExecution.query().filter(
        model.job.JobExecution.status.in_(states)).all()

    for job_execution in job_executions:
        instance = job_execution.job.instance
        if instance in instances:
            execution_count[instance.internal_name] += 1

    return execution_count


def update_instance_schedule(
        instance: model.application.ApplicationInstance,
        capacity: int,
        producer_allocations: Counter,
        allocation_config_name: str,
        capacity_config_name: str
):
    for sched in list(instance.schedules):
        if sched.parameter in (capacity_config_name, allocation_config_name):
            model.conn.delete(sched)

    schedule_capacity = model.application.ApplicationInstanceSchedule(
        instance=instance,
        parameter=capacity_config_name,
        value=str(capacity),
        priority=100,
    )

    schedule_allocation = model.application.ApplicationInstanceSchedule(
        instance=instance,
        parameter=allocation_config_name,
        value=' '.join(f'{k}|{v}' for k, v in sorted(producer_allocations.items())),
        priority=101,
    )

    model.conn.add(schedule_capacity)
    model.conn.add(schedule_allocation)


def update_instance_capacities(
        producers, weight_parameter, capacity_parameter, producer_parameter, distribution_strategy):

    consumers = CapacityConsumer.from_weight_parameter(parameter_name=weight_parameter)

    total_capacity = CapacityProducer.total_capacity(producers=producers)
    capacity_distribution = CapacityConsumer.distribute(consumers=consumers, total_capacity=total_capacity)
    producer_name_pool = CapacityProducer.producers_name_pool(
        producers=producers, distribution_strategy=distribution_strategy)

    logger.info(f'[{weight_parameter}] Total capacity: {total_capacity}')

    for consumer, capacities in sorted(capacity_distribution.items(), key=lambda x: x[0].name):
        raw_capacity = round(sum(capacities))
        allocated_names = producer_name_pool[:raw_capacity]
        producer_name_pool = producer_name_pool[raw_capacity:]

        capacity = len(allocated_names)
        producer_allocations = Counter(allocated_names)

        instance = model.application.ApplicationInstance.query().filter_by(internal_name=consumer.name).one()
        update_instance_schedule(
            instance=instance,
            capacity=capacity,
            producer_allocations=producer_allocations,
            allocation_config_name=producer_parameter,
            capacity_config_name=capacity_parameter,
        )

        allocations_formatted = ', '.join([f'{k}: {v:2}' for k, v in sorted(producer_allocations.items())]) or '-'
        logger.info(
            f'[{weight_parameter}] {consumer.name} -> capacity: {capacity:2}, allocations: [ {allocations_formatted} ]')
