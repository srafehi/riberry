from collections import defaultdict, namedtuple
from typing import List, DefaultDict

import pendulum
from sqlalchemy import func
from sqlalchemy.orm import Query

from celery.utils.log import logger
import riberry
from riberry.model.job import JobExecutionMetric, JobExecutionStreamStep, JobExecutionStream, Job, JobExecution

MetricKey = namedtuple('MetricKey', [
    'epoch_start',
    'epoch_end',
    'job_execution_id',
    'stream_name',
    'step_name',
])


def round_down(num: int, interval: int) -> int:
    """ Rounds down the given number to the nearest multiple of the given interval. """
    return int(num - (num % interval))


def latest_query_steps(epoch_start: int, limit: int = None) -> Query:
    """ Returns a query instance to retrieve the latest steps to process. """

    start_datetime = pendulum.from_timestamp(epoch_start, tz=pendulum.UTC)
    query: Query = riberry.model.conn.query(
        JobExecutionStreamStep.name,
        JobExecutionStreamStep.started,
        JobExecutionStreamStep.completed,
        JobExecutionStream.name,
        JobExecutionStream.job_execution_id,
        Job.form_id,
    ).filter(
        JobExecutionStreamStep.completed >= start_datetime
    ).order_by(JobExecutionStreamStep.completed.asc()).join(
        JobExecutionStream,
    ).join(
        JobExecution,
    ).join(
        Job,
    )

    if limit:
        query = query.limit(limit)

    return query


def retrieve_latest_epoch_start() -> int:
    """ Retrieves the max processed epoch start value. """
    return riberry.model.conn.query(func.max(JobExecutionMetric.epoch_start)).first()[0] or 0


def updated_metrics(time_interval: int, limit: int) -> List[JobExecutionMetric]:
    """ Updates/creates and returns metrics for the latest created steps. """

    updated: List[JobExecutionMetric] = []
    mapping_metrics: DefaultDict[MetricKey, JobExecutionMetric] = defaultdict(lambda: JobExecutionMetric(
        count=0,
        max_duration=0,
        min_duration=None,
        sum_duration=0,
    ))
    mapping_max_epoch_ends: DefaultDict[MetricKey, int] = defaultdict(lambda: 0)

    latest_epoch_start = retrieve_latest_epoch_start()

    # retrieve all metrics which may still have steps to be updated and populate our mapping
    metrics: List[JobExecutionMetric] = riberry.model.conn.query(JobExecutionMetric).filter_by(
        epoch_start=latest_epoch_start
    ).all()

    for metric in metrics:
        key = MetricKey(
            epoch_start=metric.epoch_start,
            epoch_end=metric.epoch_end,
            job_execution_id=metric.job_execution_id,
            stream_name=metric.stream_name,
            step_name=metric.step_name,
        )
        mapping_metrics[key] = metric

    query_steps = latest_query_steps(epoch_start=latest_epoch_start, limit=limit)
    for step_data in query_steps.yield_per(10_000):
        step_name, step_dt_start, step_dt_completed, stream_name, job_execution_id, form_id = step_data

        # Convert the step's start and completed date-times to timestamps
        epoch_start = pendulum.UTC.convert(pendulum.instance(step_dt_start)).timestamp()
        epoch_end = pendulum.UTC.convert(pendulum.instance(step_dt_completed)).timestamp()

        # Find the duration of the step + its epoch range, rounded up/down with the given interval
        range_start = round_down(epoch_end, time_interval)
        range_end = round_down(epoch_end + time_interval, time_interval)

        key = MetricKey(
            epoch_start=range_start,
            epoch_end=range_end,
            job_execution_id=job_execution_id,
            stream_name=stream_name,
            step_name=step_name,
        )
        metric = mapping_metrics[key]

        # Avoid re-processing the same step by comparing the last epoch value to populate the metric
        if metric.epoch_last and epoch_end <= metric.epoch_last:
            continue

        # Update the max epoch end for each metric
        mapping_max_epoch_ends[key] = max(mapping_max_epoch_ends[key], epoch_end)

        # Populate the metric's attributes
        metric.epoch_start = range_start
        metric.epoch_end = range_end
        metric.form_id = form_id
        metric.job_execution_id = job_execution_id
        metric.stream_name = stream_name
        metric.step_name = step_name
        metric.count += 1
        duration = epoch_end - epoch_start
        metric.sum_duration += duration

        if duration > metric.max_duration:
            metric.max_duration = max(metric.max_duration, duration)
        if metric.min_duration is None or duration < metric.min_duration:
            metric.min_duration = min(metric.min_duration or duration, duration)

    # Collect all update metrics and updated the "updated" timestamp
    updated_timestamp = pendulum.DateTime.utcnow()
    for key, max_epoch in mapping_max_epoch_ends.items():
        metric = mapping_metrics[key]
        metric.epoch_last = max_epoch
        metric.updated = updated_timestamp
        updated.append(metric)

    return updated


def process_metrics(time_interval: int, limit: int):
    """ Updates all metrics with the latest steps for the given interval. """

    updated = updated_metrics(time_interval=time_interval, limit=limit)
    if updated:
        logger.info(f'Updated {len(updated)} metric instances')
        riberry.model.conn.bulk_save_objects(updated)
        riberry.model.conn.commit()
