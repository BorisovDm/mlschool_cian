from multiprocessing import Pool

from confluent_kafka import Consumer, KafkaException


BOOTSTRAP_SERVERS = '10.156.0.3:6667,10.156.0.4:6667,10.156.0.5:6667'
GROUP_ID = 'dborisov_hw1_task3'
TOPIC_NAME = 'mles.announcements'


def get_consumer():
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
    }) 
    return consumer


def consume_data(partition_number):
    consumer = get_consumer()
    consumer.subscribe([TOPIC_NAME])

    output_file = open(f'{TOPIC_NAME}_{partition_number}', 'a')

    try:
        while True:
            msg = consumer.poll(timeout=0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            output_file.write(f'{msg.value().decode("utf-8")}\n')
    except KeyboardInterrupt:
        pass
    finally:
        output_file.close()
        consumer.close()


if __name__ == '__main__':
    consumer = get_consumer()
    n_partitions = len(consumer.list_topics().topics[TOPIC_NAME].partitions)

    with Pool(processes=n_partitions) as pool:            
        try:    
            pool.map(consume_data, range(n_partitions))
        except KeyboardInterrupt:
            pool.terminate()
