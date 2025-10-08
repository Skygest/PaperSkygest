# stream_manager.py
from multiprocessing import Queue, Process, Event, Value, Manager
import threading
from server import data_stream
from server import config
import ctypes
import time
from queue import Full
from datetime import datetime
from server.logger import logger

class StreamStats:
    def __init__(self):
        # Shared counters for monitoring stream processing
        self.total_posts_queued = Value(ctypes.c_int, 0)
        self.total_posts_processed = Value(ctypes.c_int, 0)
        
        # Use a Manager to create a shared dictionary for worker-specific stats
        manager = Manager()
        self.worker_stats = manager.dict()
        
        # Track timing information
        self.start_time = Value(ctypes.c_double, time.time())
        
    def log_queued_post(self):
        with self.total_posts_queued.get_lock():
            self.total_posts_queued.value += 1
            
    def log_processed_post(self, worker_name, post_uri):
        # Update total processed count
        with self.total_posts_processed.get_lock():
            self.total_posts_processed.value += 1
            
        # Update worker-specific stats
        if worker_name not in self.worker_stats:
            self.worker_stats[worker_name] = {'processed': 0, 'last_post': None}
        
        stats = self.worker_stats[worker_name]
        stats['processed'] += 1
        stats['last_post'] = post_uri
        self.worker_stats[worker_name] = stats  # Required for Manager dict update
        
    def print_stats(self):
        """Prints a detailed monitoring report"""
        elapsed_time = time.time() - self.start_time.value
        posts_per_second = self.total_posts_processed.value / elapsed_time if elapsed_time > 0 else 0
        
        print("\n=== Stream Processing Statistics ===")
        print(f"Time Running: {elapsed_time:.1f} seconds")
        print(f"Total Posts Queued: {self.total_posts_queued.value}")
        print(f"Total Posts Processed: {self.total_posts_processed.value}")
        print(f"Processing Rate: {posts_per_second:.2f} posts/second")
        print("\nWorker Statistics:")
        
        for worker, stats in self.worker_stats.items():
            print(f"\n{worker}:")
            print(f"  Posts Processed: {stats['processed']}")
            print(f"  Last Post: {stats['last_post']}")
            worker_share = (stats['processed'] / self.total_posts_processed.value * 100 
                          if self.total_posts_processed.value > 0 else 0)
            print(f"  Share of Work: {worker_share:.1f}%")

class StreamManager:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = StreamManager()
        return cls._instance
    
    def __init__(self):
        self.work_queue = Queue(maxsize=1000)
        self.stop_event = Event()
        self.stream_process = None
        self.stats = StreamStats()
        self.last_activity = Value('d', time())
        
    def _check_health(self):
        while not self.stop_event.is_set():
            time.sleep(300)
            with self.last_activity.get_lock():
                if time() -self.last_activity.value > 600: # no activity for 10 minutes
                    logger.error("No activity detected for 10 minutes, restarting stream")
                    self.restart()

    def resetart(self):
        self.stop()
        time.sleep(5)
        self.start()

    def start(self):
        if not self.stream_process or not self.stream_process.is_alive():
            self.stream_process = Process(
                target=self._run_stream,
                args=(self.work_queue, self.stop_event, self.stats, self.last_activity)
            )
            self.stream_process.start()
            
            health_thread = threading.Thread(target=self._check_health, daemon=True)
            health_thread.start()

            # Start stats printing thread
            self._start_stats_thread()
    
    def _start_stats_thread(self):
        def print_periodic_stats():
            while not self.stop_event.is_set():
                self.stats.print_stats()
                # Sleep for 30 seconds before next report
                for _ in range(30):
                    if self.stop_event.is_set():
                        break
                    time.sleep(1)
                    
        stats_thread = threading.Thread(target=print_periodic_stats, daemon=True)
        stats_thread.start()
    
    def _run_stream(self, queue, stop_event, stats):
        def queue_callback(ops):
            if not stop_event.is_set():
                for record_type, actions in ops.items():
                    for post in actions['created']:
                        try:
                            queue.put((record_type, post), timeout=30)
                            stats.log_queued_post()
                        except Full:
                            logger.error(f"Work queue full, dropping message for post {post}")
        
        data_stream.run(config.SERVICE_DID, queue_callback, stop_event)
    
    def get_work(self, timeout=1):
        return self.work_queue.get(timeout=timeout)
    
    def log_processed_post(self, worker_name, post_uri):
        self.stats.log_processed_post(worker_name, post_uri)
    
    def stop(self):
        self.stop_event.set()
        if self.stream_process:
            self.stream_process.join()