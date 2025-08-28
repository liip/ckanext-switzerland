import pytest

from ckanext.harvest import queue


@pytest.fixture
def clean_db(reset_db, migrate_db_for):
    """Copied from ckanext-harvester."""
    reset_db()
    migrate_db_for("harvest")
    migrate_db_for("activity")


@pytest.fixture
def clean_queues():
    """Copied from ckanext-harvester."""
    queue.purge_queues()
