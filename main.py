#
#
#

import os
import sys
import time
import logging
import random
import string

from pathlib import Path
from queue import Empty, Full
from signal import signal, SIGINT, SIGTERM, SIG_IGN
from multiprocessing import Process, Event, current_process, Queue

# logging config
app_name = Path(__file__).stem
log_file = "{}.log".format(app_name)
logging.basicConfig(format="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s", filename=log_file, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')  # noqa E501
log = logging.getLogger(app_name)
log.addHandler(logging.StreamHandler(sys.stdout))

# shutdown evt used by the ctrl-c handler.
halt_producer = None
halt_consumer = None

producer_procs = {}
consumer_procs = {}

sleep_time = 0.5
queue_size = 2
producer_worker_count = 2
consumer_worker_count = 1


def producer(halt_producer, queue):
    for signame in [SIGINT, SIGTERM]:
        signal(signame, SIG_IGN)

    pid = current_process().pid

    try:
        while True:
            log.debug("producer {} is alive".format(pid))

            if halt_producer.is_set():
                log.debug("**** producer {} shutdown started ****".format(pid))
                break

            chars = []
            for i in range(0, 1024 * 1024):
                chars.append(random.choice(string.ascii_letters))

            try:
                queue.put_nowait("".join(chars))
                log.debug("producer {} PUT {}...{}".format(pid, "".join(chars[:4]), "".join(chars[-4:])))
            except Full:
                log.debug("FULL")

            time.sleep(sleep_time)

    except Exception as e:
        log.error("producer {} - {}".format(pid, e))
        raise e

    log.debug("producer exited on pid {}".format(current_process().pid))

    sys.exit(0)


def consumer(halt_consumer, queue):
    for signame in [SIGINT, SIGTERM]:
        signal(signame, SIG_IGN)

    pid = current_process().pid

    try:
        while True:
            log.debug("consumer {} is alive".format(pid))

            if halt_consumer.is_set():
                log.debug("**** consumer {} shutdown started ****".format(pid))
                break

            try:
                chars = queue.get_nowait()
                log.debug("consumer {} GET {}...{}".format(pid, chars[:4], chars[-4:]))
            except Empty:
                log.debug("EMPTY")

            time.sleep(sleep_time)

    except Exception as e:
        log.error("consumer {} - {}".format(pid, e))
        raise e

    log.debug("consumer exited on pid {}".format(pid))

    sys.exit(0)


def shutdown():
    global halt_producer
    global halt_consumer

    # start pruducer shutdown
    halt_producer.set()

    while True:
        producers_exist = False
        for p in producer_procs.values():
            time.sleep(0.5)
            if p.is_alive():
                producers_exist = True
                log.debug("producer {} is still alive".format(p.pid))
                break

        if not producers_exist:
            break

    log.debug("producer shutdown complete")

    # start consumer shutdown
    halt_consumer.set()

    while True:
        consumers_exist = False
        for p in consumer_procs.values():
            time.sleep(0.5)
            if p.is_alive():
                consumers_exist = True
                log.debug("consumer {} is still alive".format(p.pid))
                break

        if not consumers_exist:
            break

    log.debug("consumer shutdown complete")


def main_signal_handler(sig, frame):
    if sig in [SIGINT, SIGTERM]:
        log.debug("main received a signal({})".format(sig))
        shutdown()
    else:
        log.debug("main received a unhandled signal({})".format(sig))


def main():
    global producer_procs
    global consumer_procs

    for signame in [SIGINT, SIGTERM]:
        signal(signame, main_signal_handler)

    log.debug("{} started on pid {}".format(os.path.basename(__file__), os.getpid()))

    queue = Queue(queue_size)

    # start producers
    for n in range(0, producer_worker_count):
        p_name = "producer_{}".format(n)
        p_producer = Process(target=producer, name=p_name, args=(halt_producer, queue))
        producer_procs[p_name] = p_producer
        p_producer.start()
        time.sleep(.1)
        log.debug("started {} on pid {}".format(p_name, p_producer.pid))

    # start consumers
    for n in range(0, consumer_worker_count):
        p_name = "consumer_{}".format(n)
        p_consumer = Process(target=consumer, name=p_name, args=(halt_consumer, queue))
        consumer_procs[p_name] = p_consumer
        p_consumer.start()
        time.sleep(.1)
        log.debug("started {} on pid {}".format(p_name, p_consumer.pid))

    # wait for process to finish
    while True:
        for p in list(producer_procs.values()) + list(consumer_procs.values()):
            if p.is_alive():
                break
        if not p.is_alive():  # did we reach the end of the proc list?
            break

    log.debug("main exit")


if __name__ == '__main__':
    halt_producer = Event()
    halt_consumer = Event()
    main()
