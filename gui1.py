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
        master.title("Thread-Safe Document Processor")
        master.geometry("700x500")

        self.processor = DocumentProcessor()
        self.results_queue = queue.Queue()
        self.thread_windows = []

        self.create_widgets()

    def create_widgets(self):
        frame = tk.Frame(self.master)
        frame.pack(padx=10, pady=10, fill=tk.X)

        self.file_path = tk.StringVar()
        tk.Label(frame, text="Select File:").pack(side=tk.LEFT)
        tk.Entry(frame, textvariable=self.file_path, width=50).pack(side=tk.LEFT, padx=10)
        tk.Button(frame, text="Browse", command=self.browse_file).pack(side=tk.RIGHT)

        thread_frame = tk.Frame(self.master)
        thread_frame.pack(padx=10, pady=10, fill=tk.X)

        tk.Label(thread_frame, text="Number of Threads:").pack(side=tk.LEFT)
        self.num_threads = tk.IntVar(value=4)
        tk.Spinbox(thread_frame, from_=1, to=16, textvariable=self.num_threads, width=5).pack(side=tk.LEFT, padx=10)

        self.processing_var = tk.StringVar(value="word_count")
        for text, value in [("Word Count", "word_count"), ("Character Count", "char_count"), ("Unique Words", "unique_words")]:
            tk.Radiobutton(thread_frame, text=text, variable=self.processing_var, value=value).pack(side=tk.LEFT, padx=5)

        tk.Button(self.master, text="Start Processing", command=self.start_processing).pack(pady=10)

    def browse_file(self):
        file = filedialog.askopenfilename(filetypes=[("Text files", "*.txt")])
        if file:
            self.file_path.set(file)

    def start_processing(self):
        file_path = self.file_path.get()
        if not file_path or not os.path.exists(file_path):
            messagebox.showerror("Error", "Please select a valid file.")
            return

        self.results_queue = queue.Queue()
        threads = self.num_threads.get()
        processor_func = self.get_processor_function()

        for thread_id in range(threads):
            window = tk.Toplevel(self.master)
            window.title(f"Thread {thread_id + 1}")
            result_box = tk.Text(window, wrap=tk.WORD, height=15, width=60)
            result_box.pack(expand=True, fill=tk.BOTH)
            threading.Thread(target=self.process_file, args=(file_path, processor_func, thread_id, result_box), daemon=True).start()

    def process_file(self, file_path, processor_func, thread_id, result_box):
        try:
            results = self.processor.process_document(file_path, processor_func, threads=1)
            result_box.insert(tk.END, f"Results for Thread {thread_id + 1}:\n")
            for key, value in results.items():
                result_box.insert(tk.END, f"{key}: {value}\n")
        except Exception as e:
            logging.error(f"Error in Thread {thread_id + 1}: {e}")

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
    root.mainloop()

if __name__ == "__main__":
    main()
