
from cfg.loader import cfg
import pika
from scheduling import Scheduling
from db import MongoConnection


def start_session(db_conn):
    """
    Reset all repository states to zero ready for indexing session.
    :param db_conn:
    :return:
    """
    db_conn.repositories.update(
        {},
        {
            '$set': {
                'state': 0
            }
        },
        upsert=False,
        multi=True
    )


def main():
    # Establish Connections to External Systems. Mongo and MQ

    try:

        print '> connecting to MQ @ {} ..'\
            .format(cfg.settings.mq.connection.host),
        try:
            mq_conn = pika.BlockingConnection(pika.ConnectionParameters(
                host=cfg.settings.mq.connection.host
            ))
            if mq_conn:
                print 'done'
        except pika.exceptions.AMQPConnectionError:
            raise Exception("Could not connect to MQ.")

        print '> connecting to Mongo ..',
        db_conn = MongoConnection().get_db()
        if db_conn:
            print 'done'
        else:
            raise Exception("Could not connect to DB.")

        start_session(db_conn)

        # Run the feeder
        scheduler = Scheduling(db_conn=db_conn, mq_conn=mq_conn)
        scheduler.schedule()

    except Exception as e:
        print 'Scheduler boot failure:', e

if __name__ == "__main__":
    main()