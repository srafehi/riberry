from typing import List

from celery.utils.log import logger

from riberry import model
from riberry.model.misc import ResourceRetention


def process_retentions():
    """ Removes all resources which exceeded the retention period.

    All retentions are executed in a separate session to avoid one
    failure prevent other retentions to execute.
    """

    # Fetch all retention instance IDs in one session
    with model.conn:
        retention_ids: List[int] = [result[0] for result in model.conn.query(ResourceRetention.id).all()]

    for retention_id in retention_ids:
        with model.conn:
            try:
                retention: ResourceRetention = ResourceRetention.query().filter_by(id=retention_id).first()
                if retention:
                    retention.run()
                    model.conn.commit()
            except:
                logger.exception(f'Failed to run {retention}')
            else:
                logger.info(f'Successfully ran {retention}')
