from confluent_kafka import Consumer, KafkaException
from multiprocessing import Pool


TOPIC_NAME = 'mles.announcements'


def get_consumer(is_subscribe=True):
    consumer = Consumer({
        'bootstrap.servers': '10.156.0.3:6667,10.156.0.4:6667,10.156.0.5:6667',
        'group.id': 'dborisov_hw1_task3',
    })
    if is_subscribe:
        consumer.subscribe([TOPIC_NAME]) 
    return consumer


def consume_data(_):
    consumer = get_consumer()
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            with open(f'{msg.topic()}_{msg.partition()}', 'a') as f:
                f.write(f'{msg.value().decode("utf-8")}\n')
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    consumer = get_consumer(is_subscribe=False)
    n_partitions = len(consumer.list_topics().topics[TOPIC_NAME].partitions)

    with Pool(processes=n_partitions) as pool:            
        pool.map(consume_data, range(n_partitions))
