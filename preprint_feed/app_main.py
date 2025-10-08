# if just making it from a python script

import multiprocessing
from time import sleep, time

from server import config
from server import data_stream
from server.data_filter import operations_callback
from server.logger import logger

import signal
import sys

# Define the maximum queue size
MAX_QUEUE_SIZE = 10000
work_queue = multiprocessing.Queue(MAX_QUEUE_SIZE)

def prepare_record(record):
    """
    Transforms a Record object into a dictionary, capturing all fields we need for paper detection.
    """
    # First, carefully extract the timestamp
    created_at = None
    if hasattr(record, 'createdAt'):
        created_at = record.createdAt
    elif hasattr(record, 'created_at'):
        created_at = record.created_at
    
    if not created_at:
        print(f"Warning: Missing timestamp in record. Available attributes: {dir(record)}")
        raise ValueError("Record must have a creation timestamp")

    # Create base record dictionary
    record_dict = {
        'text': record.text if hasattr(record, 'text') else '',
        'created_at': created_at
    }
    
    # Extract all features from facets (mentions, links, tags)
    if hasattr(record, 'facets') and record.facets is not None:
        record_dict['mentions'] = []
        record_dict['urls'] = []
        record_dict['facet_tags'] = []
        
        for facet in record.facets:
            if hasattr(facet, 'features'):
                for feature in facet.features:
                    # Check if the feature is a mention
                    if hasattr(feature, 'did'):
                        record_dict['mentions'].append(feature.did)
                    # Check if the feature is a link
                    elif hasattr(feature, 'uri'):
                        record_dict['urls'].append(feature.uri)
                    # Check if the feature is a tag
                    elif hasattr(feature, 'tag'):
                        record_dict['facet_tags'].append(feature.tag)
                        
        # Extract tags
    if hasattr(record, 'tags') and record.tags is not None:
        record_dict['tags'] = record.tags
    
    # Extract labels (self-labels)
    if hasattr(record, 'labels') and record.labels is not None:
        record_dict['label_values'] = []
        if hasattr(record.labels, 'values'):
            for label in record.labels.values:
                # If it's a simple string or has a val property
                if isinstance(label, str):
                    record_dict['label_values'].append(label)
                elif hasattr(label, 'val'):
                    record_dict['label_values'].append(label.val)
                
    # Handle embed data comprehensively
    if hasattr(record, 'embed'):
        record_dict['embed'] = {}
        
        # Handle external links
        if hasattr(record.embed, 'external') and record.embed.external is not None:
            record_dict['embed']['external'] = {
                'uri': record.embed.external.uri if hasattr(record.embed.external, 'uri') else '',
                'title': record.embed.external.title if hasattr(record.embed.external, 'title') else '',
                'description': record.embed.external.description if hasattr(record.embed.external, 'description') else ''
            }
        
        # Handle images
        if hasattr(record.embed, 'images') and record.embed.images is not None:
            alt_texts = []
            for image in record.embed.images:
                if hasattr(image, 'alt') and image.alt:
                    alt_texts.append(image.alt)
            if alt_texts:
                record_dict['embed']['images_alt_texts'] = alt_texts
        
        #attempt at quote tweet text
        if hasattr(record.embed, 'record'):
        # Just get the reference
            if hasattr(record.embed.record, 'uri') and record.embed.record.uri is not None:
                record_dict['embed']['record'] = {
                    'uri': record.embed.record.uri
                }
            elif hasattr(record.embed.record, 'record') and record.embed.record.record is not None:
                record_dict['embed']['record'] = {
                    'uri': record.embed.record.record.uri if hasattr(record.embed.record.record, 'uri') else ''
                }
            
            # If there's a view available (which contains the actual content)
            if hasattr(record.embed.record, 'view') and record.embed.record.view is not None:
                if hasattr(record.embed.record.view, 'record'):
                    view_record = record.embed.record.view.record
                    
                    # Handle different types of view records
                    if hasattr(view_record, 'value'):
                        # This is a viewRecord type
                        if hasattr(view_record.value, 'text'):
                            record_dict['embed']['record']['text'] = view_record.value.text
    
    # Handle reply data
    if hasattr(record, 'reply'):
        try:
            record_dict['reply'] = {
                'root': {'uri': record.reply.root.uri} if hasattr(record.reply, 'root') else None,
                'parent': {'uri': record.reply.parent.uri} if hasattr(record.reply, 'parent') else None
            }
        except AttributeError:
            record_dict['reply'] = None

    # Handle subject (for likes)
    if hasattr(record, 'subject') and record.subject is not None:
        try:
            record_dict['subject'] = {
                'uri': record.subject.uri
            }
        except AttributeError:
            record_dict['subject'] = None

    return record_dict

def queue_operations_callback(ops):
    """
    Prepares firehose data for processing, ensuring all required fields are present.
    We're especially careful with the timestamp since it's a DynamoDB key.
    """
    if work_queue:
        try:
            prepared_ops = {}
            for record_type, actions in ops.items():
                prepared_ops[record_type] = {
                    'created': [],
                    'deleted': actions['deleted']
                }
                
                for post in actions['created']:
                    try:
                        prepared_record = prepare_record(post['record'])
                        
                        # Create the post dictionary that will eventually go to DynamoDB
                        prepared_post = {
                            'uri': post['uri'],
                            'cid': post['cid'],
                            'author': post['author'],
                            'CreatedDate': prepared_record['created_at'],  # Use the verified timestamp
                            'record': prepared_record
                        }
                        
                        prepared_ops[record_type]['created'].append(prepared_post)
                    except ValueError as e:
                        # Skip posts with missing timestamps rather than failing
                        print(f"Skipping post due to missing timestamp: {e}")
                        continue
            
            work_queue.put(prepared_ops)
        except Exception as e:
            print(f"Error preparing data for queue: {e}")

def worker_process_queue(work_queue, worker_id = 0):
    processed_count = 0
    success_count = 0
    last_print_time = time()

    try: 
        while True:
            try:
                ops = work_queue.get(timeout=1)
                success_count += operations_callback(ops)
                processed_count += 1

                # Print the count every 30 seconds
                current_time = time()
                if current_time - last_print_time >= 30:
                    queue_length = work_queue.qsize()
                    print(f"Worker {worker_id} processed {processed_count} items and has found {success_count} paper posts; Queue length: {queue_length}")
                    last_print_time = current_time

            except multiprocessing.queues.Empty:
                print(f"Worker {worker_id} queue is empty, waiting for new work...")
                sleep(30)
                continue
            except Exception as e:
                print(f"Error processing work: {e}")
                sleep(1)
    finally:
        # Ensure we return the connection to the pool
        print(f"Worker {worker_id} finished processing. Total processed: {processed_count}, Success: {success_count}")

from datetime import datetime
def data_stream_with_restart(service_did, callback, stop_event):
    """Run data_stream with automatic restart on hang"""
    while True:
        print(f"Starting data stream at {datetime.now()}")
        
        # Create a process for the data stream
        p = multiprocessing.Process(
            target=data_stream.run,
            args=(service_did, callback, stop_event)
        )
        p.start()
        
        # Monitor the process
        last_check = time()
        while p.is_alive():
            print(f"Data stream process is alive at {datetime.now()}")
            sleep(60)  # Check every minute
            
            # You could check queue size here to see if data is flowing
            # If no data for too long, kill and restart
            
        logger.error(f"Data stream process died at {datetime.now()}, restarting...")
        sleep(5)  # Brief pause before restart 


def main():
    num_workers = 6  # Specify the number of worker processes you want
    
    # Replace the direct data_stream.run with our wrapper
    stream_process = multiprocessing.Process(
        target=data_stream_with_restart,
        args=(config.SERVICE_DID, queue_operations_callback, None)
    )
    
    stream_process.start()
    
    # Start the worker processes
    worker_processes = []
    for en in range(num_workers):
        process = multiprocessing.Process(target=worker_process_queue, args=(work_queue,en))
        process.start()
        worker_processes.append(process)

    # Wait for the worker processes to finish (if necessary)
    for process in worker_processes:
        process.join()

    # Wait for the stream process to finish (if necessary)
    stream_process.join()

    def signal_handler(sig, frame):
        print("Shutting down gracefully...")
        
        # Send shutdown signal to worker processes
        for _ in range(num_workers):
            work_queue.put(None)  # None signals workers to stop
        
        # Wait for workers to finish
        for p in worker_processes:
            p.join(timeout=5)  # Give them 5 seconds
            if p.is_alive():
                p.terminate()
        
        # Close the connection pool
        sys.exit(0)

    # Register the signal handler
    signal.signal(signal.SIGINT, signal_handler)

if __name__ == '__main__':
    main()