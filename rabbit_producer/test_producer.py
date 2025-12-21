from rmq_custom_pack.tests import test_producer


def test_for_producer(monkeypatch):
    test_producer.test_producer_handler(monkeypatch)