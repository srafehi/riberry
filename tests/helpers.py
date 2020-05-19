import contextlib


def recreate_database():
    from riberry import model
    model.base.Base.metadata.drop_all(model.conn.raw_engine)
    model.base.Base.metadata.create_all(model.conn.raw_engine)
    yield
    model.base.Base.metadata.drop_all(model.conn.raw_engine)


def empty_database():
    from riberry import model
    with contextlib.closing(model.conn.raw_engine.connect()) as connection:
        transaction = connection.begin()
        for table in reversed(model.base.Base.metadata.sorted_tables):
            connection.execute(table.delete())
        transaction.commit()
