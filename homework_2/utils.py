import datetime
import json
import os

from confluent_kafka import Consumer


BOOTSTRAP_SERVERS = '10.156.0.3:6667,10.156.0.4:6667,10.156.0.5:6667'


def validate_offset_dump(topic_name, filename, n_partitions):
    with open(filename, 'r') as f:
        for line in f:
            pass

    last_record = line.split('\t')[1].strip()
    partitions_config = json.loads(last_record)
    
    current_partitions = set(partitions_config[topic_name].keys())
    necessary_partitions = {str(i) for i in range(n_partitions)}
    
    if current_partitions == necessary_partitions:
        return ('OK', last_record)

    elif n_partitions > len(current_partitions):
        for partition in range(len(current_partitions), n_partitions):
            partitions_config[topic_name][str(partition)] = -2

        current_partitions = set(partitions_config[topic_name].keys())
        if current_partitions == necessary_partitions:
            return ('NEW PARTITIONS', json.dumps(partitions_config))
        else:
            return ('ERROR', 'last record is incorrect')
    else:
        return ('ERROR', 'the number of partitions decreased')


def get_topic_offsets_filename(topic_name):
    return f'{topic_name}_offsets_history.txt'


def get_starting_offsets(topic_name):
    offsets_history_filename = get_topic_offsets_filename(topic_name)

    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'borisov_get_partitions_number',
    })

    n_partitions = len(consumer.list_topics().topics[topic_name].partitions)

    if os.path.isfile(offsets_history_filename):
        status, message = validate_offset_dump(topic_name, offsets_history_filename, n_partitions)
        if status == 'ERROR':
            raise Exception(message)
        else:
            return message

    else:
        starting_offsets_dict = {
            topic_name: {
                str(partition): -2
                for partition in range(n_partitions)
            }
        }
        return json.dumps(starting_offsets_dict)


def dump_offsets(topic_name, partition_offsets_mapping):
    processing_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    filename = get_topic_offsets_filename(topic_name)
    message = json.dumps({topic_name: partition_offsets_mapping})

    with open(filename, 'a') as f:
        f.write(f'{processing_time}\t{message}\n')
