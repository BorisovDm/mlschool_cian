import datetime
import json
import random
import string
import time

from confluent_kafka import Producer


OUTPUT_TOPIC = 'dborisov'


def get_message():
    key = random.randrange(101)
    message = ''.join(random.choice(string.ascii_lowercase) for _ in range(key))
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    
    response = json.dumps({
        'timestamp': timestamp,
        'message': message,
    })
    
    return str(key), response


if __name__ == '__main__':
    p = Producer({'bootstrap.servers': '10.156.0.3:6667,10.156.0.4:6667,10.156.0.5:6667'})
    try:
        while True:
            key, value = get_message()
            p.produce(topic=OUTPUT_TOPIC, key=key, value=value)
            p.poll(timeout=0)
            time.sleep(random.randrange(5))
    except:
        pass
    finally:
        p.flush()
