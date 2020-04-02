from nycodex import db, inference
from nycodex.logging import get_logger

logger = get_logger(__name__)


def find_inclusions():
    log = logger.bind()
    with db.engine.connect() as c:
        while True:
            try:
                with db.queue.next_row_to_process(c) as (tconn, dataset_id):
                    if dataset_id is None:
                        break
                    log = logger.bind(dataset_id=dataset_id)
                    inference.find_all_inclusions(tconn, dataset_id)
            except Exception as e:
                log.critical(
                    "Failed to find inclusions with unknown exception",
                    exc_info=e,
                )


if __name__ == "__main__":
    pass
