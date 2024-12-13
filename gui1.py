# gui1.py
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading
import queue
import logging
from docsprocessor import DocumentProcessor
from doc_processor import word_count_processor, char_count_processor, unique_words_processor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
)

class ThreadSafeDocumentProcessorGUI:
    def __init__(self, master):
        self.master = master
        master.title("Advanced Parallel Document Processor")
        master.geometry("800x600")

        self.processor = DocumentProcessor()
        self.results_queue = queue.Queue()
        self.thread_windows = []
        
        # Add a flag to track processing status
        self.is_processing = threading.Event()

        self.create_widgets()

    def create_widgets(self):
        # File selection frame
        file_frame = tk.Frame(self.master)
        file_frame.pack(padx=10, pady=10, fill=tk.X)

        tk.Label(file_frame, text="Select File:").pack(side=tk.LEFT)
        self.file_path = tk.StringVar()
        tk.Entry(file_frame, textvariable=self.file_path, width=60).pack(side=tk.LEFT, padx=10)
        tk.Button(file_frame, text="Browse", command=self.browse_file).pack(side=tk.RIGHT)

        # Processing options frame
        options_frame = tk.Frame(self.master)
        options_frame.pack(padx=10, pady=10, fill=tk.X)

        # Threads selection
        tk.Label(options_frame, text="Number of Threads:").pack(side=tk.LEFT)
        self.num_threads = tk.IntVar(value=4)
        thread_spinner = tk.Spinbox(options_frame, from_=1, to=16, textvariable=self.num_threads, width=5)
        thread_spinner.pack(side=tk.LEFT, padx=10)

        # Processing type selection
        self.processing_var = tk.StringVar(value="word_count")
        processing_types = [
            ("Word Count", "word_count"), 
            ("Character Count", "char_count"), 
            ("Unique Words", "unique_words")
        ]
        for text, value in processing_types:
            tk.Radiobutton(options_frame, text=text, variable=self.processing_var, value=value).pack(side=tk.LEFT, padx=5)

        # Processing button
        self.process_button = tk.Button(self.master, text="Start Processing", command=self.start_processing)
        self.process_button.pack(pady=10)

        # Results frame
        self.results_frame = tk.Frame(self.master)
        self.results_frame.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)
        
        # Text box for the merged results
        self.merged_result_text = tk.Text(self.results_frame, wrap=tk.WORD, height=10, width=80)
        self.merged_result_text.pack(expand=True, fill=tk.BOTH, padx=5, pady=5)

        # Total processing time label
        self.time_var = tk.StringVar(value="Total Processing Time: N/A")
        tk.Label(self.master, textvariable=self.time_var, font=('Helvetica', 10, 'bold')).pack(pady=5)

    def browse_file(self):
        file = filedialog.askopenfilename(filetypes=[("Text files", "*.txt")])
        if file:
            self.file_path.set(file)

    def start_processing(self):
        # Prevent multiple simultaneous processing attempts
        if self.is_processing.is_set():
            messagebox.showinfo("Processing", "A file is currently being processed.")
            return

        # Clear previous results
        self.merged_result_text.delete(1.0, tk.END)
        # Destroy existing thread windows
        for window in self.thread_windows:
            window.destroy()
        self.thread_windows = []

        file_path = self.file_path.get()
        if not file_path or not os.path.exists(file_path):
            messagebox.showerror("Error", "Please select a valid file.")
            return

        # Disable the process button during processing
        self.process_button.config(state=tk.DISABLED)
        
        # Reset processing flag
        self.is_processing.clear()

        # Prepare thread results windows
        threads = self.num_threads.get()
        processor_func = self.get_processor_function()

        # Create result windows for each thread
        self.thread_windows = []
        for thread_id in range(threads):
            window = tk.Toplevel(self.master)
            window.title(f"Thread {thread_id + 1} Results")
            
            result_box = tk.Text(window, wrap=tk.WORD, height=10, width=60)
            result_box.pack(expand=True, fill=tk.BOTH, padx=5, pady=5)
            self.thread_windows.append(result_box)

        # Start processing in a separate thread
        processing_thread = threading.Thread(
            target=self.safe_process_file, 
            args=(file_path, processor_func, threads), 
            daemon=True
        )
        processing_thread.start()

    def safe_process_file(self, file_path, processor_func, threads):
        try:
            # Mark processing as started
            self.is_processing.set()

            # Process document in parallel
            results = self.processor.process_document_parallel(
                file_path, 
                processor_func, 
                num_threads=threads
            )

            # Use queue to pass results back to main thread
            self.results_queue.put(results)

            # Signal main thread to update UI
            self.master.event_generate("<<ProcessingComplete>>")

        except Exception as e:
            # Put exception in queue for main thread to handle
            self.results_queue.put(e)
            self.master.event_generate("<<ProcessingError>>")

    def on_processing_complete(self, event):
        try:
            # Get results from queue
            results = self.results_queue.get(block=False)

            # Re-enable process button
            self.process_button.config(state=tk.NORMAL)

            # Clear processing flag
            self.is_processing.clear()

            # Update time 
            processing_time = results.pop('processing_time', 0)
            self.time_var.set(f"Total Processing Time: {processing_time:.4f} seconds")
           
            thread_results = results['thread_results']
            merged_results = results['merged_results']

            # Display results for each thread
            for thread_id, thread_result in enumerate(thread_results):
                 if 0 <= thread_id < len(self.thread_windows):
                       result_box = self.thread_windows[thread_id]
                       result_box.delete(1.0, tk.END)  # Clear previous results
                       for res_key, res_value in thread_result.items():
                            result_box.insert(tk.END, f"{res_key}: {res_value}\n")

            # Clear merged results
            self.merged_result_text.delete(1.0, tk.END)

            # Display merged results
            for key, value in merged_results.items():
                if key != 'processing_time':
                    self.merged_result_text.insert(tk.END, f"{key}: {value}\n")

        except queue.Empty:
            pass

    def on_processing_error(self, event):
        try:
            error = self.results_queue.get(block=False)
            logging.error(f"Processing error: {error}")
            messagebox.showerror("Error", str(error))
        except queue.Empty:
            pass
        finally:
            # Always re-enable process button and clear processing flag
            self.process_button.config(state=tk.NORMAL)
            self.is_processing.clear()

    def get_processor_function(self):
        processors = {
            "word_count": word_count_processor,
            "char_count": char_count_processor,
            "unique_words": unique_words_processor
        }
        return processors[self.processing_var.get()]

def main():
    root = tk.Tk()
    app = ThreadSafeDocumentProcessorGUI(root)
    
    # Bind custom events for thread-safe UI updates
    root.bind("<<ProcessingComplete>>", app.on_processing_complete)
    root.bind("<<ProcessingError>>", app.on_processing_error)
    
    root.mainloop()

if __name__ == "__main__":
    main()