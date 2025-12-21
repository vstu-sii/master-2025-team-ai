from rmq_custom_pack.tests import test_consumer


def test_for_consumer(monkeypatch):
    test_consumer.test_start_consuming()
    test_consumer.test_consumer_handler(monkeypatch)