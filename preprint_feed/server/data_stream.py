from atproto import AtUri, CAR, firehose_models, FirehoseSubscribeReposClient, models, parse_subscribe_repos_message
from atproto.exceptions import FirehoseError

from server.database import SubscriptionState
from server.logger import logger

from time import sleep
import threading

_INTERESTED_RECORDS = {
    models.AppBskyFeedLike: models.ids.AppBskyFeedLike,
    models.AppBskyFeedRepost: models.ids.AppBskyFeedRepost,
    models.AppBskyFeedPost: models.ids.AppBskyFeedPost,
}

def create_operations_dict():
    """
    Creates a dictionary structure for all interested record types.
    Each record type gets a dictionary with 'created' and 'deleted' lists.
    """
    operations = {}
    for _, record_nsid in _INTERESTED_RECORDS.items():
        operations[record_nsid] = {'created': [], 'deleted': []}
    return operations

def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit):
    # Initialize our dictionary with all interested record types
    operation_by_type = create_operations_dict()

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        if op.action == 'update':
            continue

        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

        if op.action == 'create':
            if not op.cid:
                continue

            create_info = {'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo}

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = models.get_or_create(record_raw_data, strict=False)
            for record_type, record_nsid in _INTERESTED_RECORDS.items():
                if uri.collection == record_nsid and models.is_record_type(record, record_type):
                    operation_by_type[record_nsid]['created'].append({'record': record, **create_info})
                    break

        if op.action == 'delete':
            # Check if this collection type is one we're interested in
            if uri.collection in operation_by_type:
                operation_by_type[uri.collection]['deleted'].append({'uri': str(uri)})

    return operation_by_type


def run(name, operations_callback, stream_stop_event=None):
    run_count = 0
    last_print_time = datetime.now()
    while stream_stop_event is None or not stream_stop_event.is_set():
        run_count += 1
        # print runcount every 10 minutes
        if datetime.now() - last_print_time > timedelta(minutes=10):
            print(f"Data stream run count: {run_count} at {datetime.now()}")
            last_print_time = datetime.now()
        try:
            _run(name, operations_callback, stream_stop_event)
        except FirehoseError as e:
            # here we can handle different errors to reconnect to firehose
            logger.error("Encountered a Firehose exception, sleeping for 2s...")
            print("Encountered a Firehose exception, sleeping for 2s...")
            
            sleep(2)
        except Exception as e:
            logger.error(f"Unexpected error in stream processing: {e}") 
            print(f"Unexpected error in stream processing: {e}") 
            sleep(5)
            
    # Code doesn't get here either when the firehose has stopped
    while True:
        print(f"Data stream run for loop HAS ENDED, NOT PROCESSING NEW STREAM ITEMS AT {datetime.now()}")
        sleep(60)  # Check every minute

from datetime import datetime, timedelta


def _run(name, operations_callback, stream_stop_event=None):

    state = SubscriptionState.get_or_none(SubscriptionState.service == name)

    params = None
    if state:
        params = models.ComAtprotoSyncSubscribeRepos.Params(cursor=state.cursor)

    client = FirehoseSubscribeReposClient(params)

    if not state:
        SubscriptionState.create(service=name, cursor=0)
        
    last_message_time = datetime.now()
    watchdog_stop = threading.Event()
    
    def watchdog():
        number_restarts = 0
        while not watchdog_stop.is_set():
            sleep(60)  # Check every minute
            if datetime.now() - last_message_time > timedelta(minutes=5):  # No messages for 5 minutes
                number_restarts += 1
                logger.error(f"Watchdog: No messages received for {name} in 5 minutes, stopping client")
                print(f"Watchdog: No messages received for {name} in 5 minutes, stopping client")
                try:
                    client.stop()
                except:
                    print(f"Watchdog: Error stopping client for {name}, continuing...")
                    pass
                break
            else:
                print(f"Watchdog: Last message received for {name} at {last_message_time}, continuing...")
                print(f'Number of restarts by watchdog: {number_restarts}')
    # Start watchdog thread
    watchdog_thread = threading.Thread(target=watchdog, daemon=True)
    watchdog_thread.start()

    def on_message_handler(message: firehose_models.MessageFrame) -> None:
        nonlocal last_message_time
        last_message_time = datetime.now()
        # stop on next message if requested
        if stream_stop_event and stream_stop_event.is_set():
            client.stop()
            return

        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        # update stored state every ~20 events #I think this is 1000 events now...
        if commit.seq % 1000 == 0:
            #logger.info(f'Updated cursor for {name} to {commit.seq}')
            client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=commit.seq))
            SubscriptionState.update(cursor=commit.seq).where(SubscriptionState.service == name).execute()
            
            if commit.seq % 1000000 == 0:
                print(f"Updated cursor for {name} to {commit.seq} at {datetime.now()} (only printing every 1 million events)")


        if not commit.blocks:
            return

        operations_callback(_get_ops_by_type(commit))

    try:
        logger.info(f"Starting firehose client for {name}")
        client.start(on_message_handler)
    finally:
        watchdog_stop.set()
        logger.warning(f"client.start() returned for {name}")
    
    logger.warning(f"client.start() returned unexpectedly for {name}")
    print(f"client.start() returned unexpectedly for {name}")
