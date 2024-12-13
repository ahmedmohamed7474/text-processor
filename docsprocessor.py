# docsprocessor.py
import os
import concurrent.futures
import mmap
import threading
import time
from typing import List, Dict, Callable
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
)


class DocumentProcessor:
    def __init__(self, chunk_size: int = 10000):
        """
        Initialize the DocumentProcessor with efficient chunk-based processing.
        
        :param chunk_size: Number of characters per chunk
        """
        self.chunk_size = chunk_size

    def split_document_for_threads(self, file_path: str, num_threads: int) -> List[List[str]]:
        """
        Split document into chunks for each thread, ensuring even distribution.

        :param file_path: Path to the document
        :param num_threads: Number of threads to distribute chunks across
        :return: List of chunk lists, one for each thread
        """
        try:
            # Read entire file using mmap for efficiency
            with open(file_path, 'r+b') as file:
                with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mmapped_file:
                    text = mmapped_file.read().decode('utf-8')
            
            # Calculate total chunks and chunks per thread
            total_chunks = (len(text) + self.chunk_size - 1) // self.chunk_size
            chunks_per_thread = total_chunks // num_threads
            
            # Distribute chunks across threads
            thread_chunks = [[] for _ in range(num_threads)]
            for i in range(total_chunks):
                thread_index = i // chunks_per_thread
                # Ensure last thread gets any remaining chunks
                if thread_index >= num_threads:
                    thread_index = num_threads - 1
                
                start = i * self.chunk_size
                end = min((i + 1) * self.chunk_size, len(text))
                chunk = text[start:end]
                
                # Trim to last whole word
                if end < len(text):
                    last_space = chunk.rfind(' ')
                    if last_space != -1:
                        chunk = chunk[:last_space]
                
                thread_chunks[thread_index].append(chunk)
            
            return thread_chunks
        
        except IOError as e:
            logging.error(f"Error reading file {file_path}: {e}")
            raise

    def process_chunks(self, chunks: List[str], processor: Callable) -> Dict:
        """
        Process a list of chunks within a single thread.
        
        :param chunks: List of text chunks to process
        :param processor: Processing function
        :return: Processed results for this thread
        """
        thread_results = {}
        
        for chunk in chunks:
            try:
                chunk_result = processor(chunk)
                # Merge results
                for key, value in chunk_result.items():
                    if key not in thread_results:
                        thread_results[key] = value
                    elif isinstance(value, int):
                        thread_results[key] += value
                    elif isinstance(value, set):
                       if key not in thread_results:
                            thread_results[key] = value
                       else:
                         thread_results[key].update(value)

                    elif isinstance(value, dict):
                        if key not in thread_results:
                            thread_results[key] = value
                        else:
                            # Merge dictionaries
                            for subkey, subvalue in value.items():
                                if subkey not in thread_results[key]:
                                    thread_results[key][subkey] = subvalue
                                elif isinstance(subvalue, int):
                                    thread_results[key][subkey] += subvalue
            
            except Exception as e:
                logging.error(f"Chunk processing error: {e}")
        
        return thread_results

    def process_document_parallel(self, file_path: str, processor: Callable, num_threads: int = 4) -> Dict:
        """
        Process a document in parallel with even chunk distribution.
        
        :param file_path: Path to the document
        :param processor: Processing function
        :param num_threads: Number of threads to use
        :return: Merged results from all chunks
        """
        start_time = time.time()
        
        # Split chunks for threads
        thread_chunks = self.split_document_for_threads(file_path, num_threads)
        
        # Process chunks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit chunks for each thread
            futures = [
                executor.submit(self.process_chunks, chunks, processor) 
                for chunks in thread_chunks
            ]
            
            # Collect and merge results
            all_results = [future.result() for future in futures]
            
            final_results = {}
            for thread_result in all_results:
                # Merge thread results into final results
                for key, value in thread_result.items():
                    if key not in final_results:
                        final_results[key] = value
                    elif isinstance(value, int):
                        final_results[key] += value
                    elif isinstance(value, set):
                        if key not in final_results:
                          final_results[key] = value
                        else:
                          final_results[key].update(value)

                    elif isinstance(value, dict):
                        if key not in final_results:
                            final_results[key] = value
                        else:
                            # Merge dictionaries
                            for subkey, subvalue in value.items():
                                if subkey not in final_results[key]:
                                    final_results[key][subkey] = subvalue
                                elif isinstance(subvalue, int):
                                    final_results[key][subkey] += subvalue

        
        end_time = time.time()
        
        # Format unique_words list
        if 'unique_words' in final_results and isinstance(final_results['unique_words'], set):
            final_results['unique_words'] = list(final_results['unique_words'])

        return {
            'thread_results': all_results, # List of results from each thread
            'merged_results': final_results, # Merged results
            'processing_time': end_time - start_time # Processing Time
        }