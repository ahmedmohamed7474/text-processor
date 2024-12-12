import os
import concurrent.futures
import mmap
import threading
from typing import List, Dict, Callable
from functools import reduce
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
)


class DocumentProcessor:
    def __init__(self, chunk_size: int = 10000, max_threads: int = 4):
        """
        Initialize the DocumentProcessor with thread safety and user-defined threads.
        
        :param chunk_size: Number of characters per chunk
        :param max_threads: Maximum number of threads to use
        """
        self.chunk_size = chunk_size
        self.max_threads = max_threads
        self._results_lock = threading.Lock()

    def split_document(self, file_path: str) -> List[str]:
        """
        Split document into chunks using mmap for efficient file reading.

        :param file_path: Path to the document
        :return: List of document chunks
        """
        try:
            chunks = []
            with open(file_path, 'r+b') as file:  # Open in binary mode for mmap
                # Memory-map the file
                with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mmapped_file:
                    text = mmapped_file.read().decode('utf-8')  # Decode the mmap content
                    
                    # Split into chunks as before
                    for i in range(0, len(text), self.chunk_size):
                        chunk = text[i:i+self.chunk_size]
                        if i + self.chunk_size < len(text):
                            last_space = chunk.rfind(' ')
                            if last_space != -1:
                                chunk = chunk[:last_space]
                        chunks.append(chunk)
            
            return chunks
        except IOError as e:
            logging.error(f"Error reading file {file_path}: {e}")
            raise

    def process_chunk(self, chunk: str, processor: Callable) -> Dict:
        """
        Process a single chunk.
        
        :param chunk: Text chunk to process
        :param processor: Processing function
        :return: Processed chunk results
        """
        try:
            return processor(chunk)
        except Exception as e:
            logging.error(f"Chunk processing error: {e}")
            return {}

    def process_document(self, file_path: str, processor: Callable, threads: int = 4, progress_callback=None) -> Dict:
        """
        Process a document in parallel with thread safety.
        
        :param file_path: Path to the document
        :param processor: Processing function
        :param threads: Number of threads to use
        :param progress_callback: Optional callback for progress updates
        :return: Merged results from all chunks
        """
        shared_results = {
            'total_processed': 0,
            'aggregated_data': {}
        }

        def merge_dicts(dict1: Dict, dict2: Dict) -> Dict:
            """Helper for merging dictionaries."""
            for key, value in dict2.items():
                if key in dict1:
                    if isinstance(value, int):
                        dict1[key] += value
                    elif isinstance(value, list):
                        dict1[key].extend(value)
                else:
                    dict1[key] = value
            return dict1

        # Split document into chunks
        chunks = self.split_document(file_path)
        total_chunks = len(chunks)
        results_lock = threading.Lock()

        # Process chunks using a ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            future_to_chunk = {executor.submit(self.process_chunk, chunk, processor): chunk for chunk in chunks}

            for i, future in enumerate(concurrent.futures.as_completed(future_to_chunk), 1):
                try:
                    result = future.result()
                    with results_lock:
                        shared_results['total_processed'] += 1
                        merge_dicts(shared_results['aggregated_data'], result)

                    if progress_callback:
                        progress_callback((i / total_chunks) * 100)

                except Exception as exc:
                    logging.error(f'Chunk processing error: {exc}')
        
        return shared_results['aggregated_data']
