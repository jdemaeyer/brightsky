import os


def test_migrate(db):
    assert len(db.table('migrations')) == len(os.listdir('migrations'))
