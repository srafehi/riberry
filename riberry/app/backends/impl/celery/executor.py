from celery import exceptions as celery_exc, current_task

import riberry
from riberry.app import actions
from riberry.app.misc.signals import task_prerun, task_postrun
from .extension import RiberryTask


class ExecutionComplete(Exception):
    pass


IGNORE_EXCEPTIONS = (
    celery_exc.Retry,
    celery_exc.SoftTimeLimitExceeded,
    celery_exc.TimeLimitExceeded,
)


def _retry_types(task_options):
    return tuple(list(IGNORE_EXCEPTIONS) + task_options.get('autoretry_for', []))


def _attempt_fallback(exc, task_options):
    fallback_on_error_provided, fallback_on_error = (
        'rib_fallback' in task_options, task_options.get('rib_fallback')
    )

    if not fallback_on_error_provided:
        actions.artifacts.create_artifact_from_traceback(category='Fatal')
        raise exc

    try:
        result = fallback_on_error() if callable(fallback_on_error) else fallback_on_error
        actions.artifacts.create_artifact_from_traceback(category='Intercepted')
        return result
    except:
        actions.artifacts.create_artifact_from_traceback(category='Fatal (intercept failed)')
        raise exc


class TaskExecutor:

    @property
    def riberry_app(self):
        return riberry.app.env.current_riberry_app

    def external_task_executor(self):
        def _external_task_executor(external_task_id, validator):
            external_task: riberry.model.job.JobExecutionExternalTask = riberry.model.job.JobExecutionExternalTask.query().filter_by(
                task_id=external_task_id,
            ).first()
            task: RiberryTask = self.riberry_app.context.current.task

            if external_task:
                if external_task.status == 'WAITING':
                    raise task.retry(countdown=1)
                elif external_task.status == 'READY':
                    output_data = external_task.output_data
                    if isinstance(output_data, bytes):
                        output_data = output_data.decode()

                    outcomes = self.riberry_app.context.event_registry.call(
                        event_type=self.riberry_app.context.event_registry.types.on_external_result_received,
                        key=validator,
                        kwargs=dict(
                            external_task=external_task,
                            result=output_data,
                        )
                    )

                    if outcomes:
                        assert len(outcomes) == 1, f'Multiple callbacks triggered for {validator}'
                        outcome = outcomes[0]
                        if outcome and outcome.retry:
                            external_task.status = 'WAITING'
                            external_task.input_data = outcome.input_data
                            riberry.model.conn.commit()
                            raise task.retry(countdown=1)

                    external_task.status = 'COMPLETE'
                    riberry.model.conn.commit()
                    return output_data

        return _external_task_executor

    def entry_point_executor(self):
        def _entry_point_executor(execution_id, form: str):
            entry_point = self.riberry_app.entry_points[form]
            actions.executions.execution_started(
                task=self.riberry_app.context.current.task,
                job_id=execution_id,
                primary_stream=entry_point.stream,
            )
            entry_point.func()

        return _entry_point_executor

    def riberry_task_executor(self, func, func_args, func_kwargs, task_options):
        riberry_properties = {}
        for key, value in list(func_kwargs.items()):
            if key.startswith('__rib_'):
                riberry_properties[key.replace('__rib_', '', 1)] = func_kwargs.pop(key)

        with riberry.model.conn:
            with self.riberry_app.context.scope(
                root_id=current_task.request.root_id,
                task_id=current_task.request.id,
                stream=riberry_properties.get('stream'),
                step=riberry_properties.get('step'),
                category=riberry_properties.get('category'),
            ):
                state = None
                mark_workflow_complete = False
                try:
                    task_prerun(context=self.riberry_app.context, props=riberry_properties)
                    result = self._execute_task(func, func_args, func_kwargs)
                    state = 'SUCCESS'
                    return result
                except ExecutionComplete:
                    state = 'FAILURE'
                    raise
                except celery_exc.Ignore:
                    state = 'IGNORED'
                    raise
                except Exception as exc:
                    state = 'FAILURE'
                    mark_workflow_complete = True
                    if isinstance(exc, _retry_types(task_options=task_options)) and not self._max_retries_reached(exc):
                        state = None
                        mark_workflow_complete = False
                        raise

                    result = _attempt_fallback(exc, task_options=task_options)
                    mark_workflow_complete = False
                    state = 'SUCCESS'
                    return result
                finally:
                    if mark_workflow_complete:
                        actions.executions.execution_complete(
                            task_id=self.riberry_app.context.current.task_id,
                            root_id=self.riberry_app.context.current.root_id,
                            status=state,
                            stream=None,
                        )
                    if state is not None:
                        task_postrun(context=self.riberry_app.context, props=riberry_properties, state=state)

    def _max_retries_reached(self, exc):
        active_task = self.riberry_app.context.current.task
        return bool(
            not isinstance(exc, celery_exc.Ignore) and
            active_task.max_retries is not None and
            active_task.request.retries >= active_task.max_retries
        )

    def _execute_task(self, func, args, kwargs):
        job_execution = self.riberry_app.context.current.job_execution
        if job_execution.status in ('FAILURE', 'SUCCESS'):
            raise ExecutionComplete(f'Execution {job_execution!r} is already marked as complete')
        return func(*args, **kwargs)

    def riberry_task_executor_wrapper(self, func, task_options):
        def wrapped_function(*args, **kwargs):
            return self.riberry_task_executor(
                func=func,
                func_args=args,
                func_kwargs=kwargs,
                task_options=task_options,
            )

        if 'name' not in task_options:
            task_options['name'] = riberry.app.util.misc.function_path(func=func)
        task_options['base'] = task_options.get('base') or RiberryTask

        return wrapped_function, task_options
