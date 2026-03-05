import logging
from traceback import format_exc
from queue import Queue
from threading import Thread, Lock
import time
from typing import Dict
import os

logger = logging.getLogger(__name__)

TEMP_FOLDER = os.getenv('TEMP_FOLDER', './temp')

class UploadManager:
    def __init__(self):
        # Create temp directory if it doesn't exist
        os.makedirs(TEMP_FOLDER, exist_ok=True)
        self.upload_queue = Queue()
        self.active_uploads: Dict[str, bool] = {}
        self.lock = Lock()
        self._start_worker()

    def _start_worker(self):
        def worker():
            while True:
                try:
                    upload_task = self.upload_queue.get()
                    if upload_task is None:
                        break
                        
                    task_id, push_func, args = upload_task
                    logger.debug(f"Processing upload task {task_id}")
                    
                    try:
                        push_func(*args)
                        with self.lock:
                            self.active_uploads[task_id] = True
                        logger.debug(f"Upload task {task_id} completed successfully")
                    except Exception as e:
                        logger.error(f"Upload failed for {task_id}: {str(e)}")
                        logger.error(format_exc())
                        with self.lock:
                            self.active_uploads[task_id] = False
                    finally:
                        self.upload_queue.task_done()
                        
                except Exception as e:
                    logger.error(f"Worker error: {str(e)}")
                    logger.error(format_exc())
                    # Clean up temp files in case of error
                    try:
                        for file in os.listdir(TEMP_FOLDER):
                            os.remove(os.path.join(TEMP_FOLDER, file))
                    except:
                        pass

        self.worker_thread = Thread(target=worker, daemon=True)
        self.worker_thread.start()

    def queue_upload(self, task_id: str, push_func, *args):
        """Add an upload task to the queue"""
        try:
            logger.debug(f"Queueing upload task {task_id}")
            logger.debug(f"Args: {args}")
            
            with self.lock:
                self.active_uploads[task_id] = False
            self.upload_queue.put((task_id, push_func, args))
            return task_id
            
        except Exception as e:
            logger.error(f"Error queueing upload: {str(e)}")
            logger.error(format_exc())
            raise

    def check_status(self, task_id: str) -> bool:
        """Check if a specific upload is complete"""
        with self.lock:
            return self.active_uploads.get(task_id, False)

    def get_pending_count(self) -> int:
        """Get number of pending uploads"""
        return self.upload_queue.qsize()
