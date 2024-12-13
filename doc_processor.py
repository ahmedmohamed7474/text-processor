# doc_processor.py
from typing import Dict, Callable, List
import multiprocessing
from functools import reduce
import mmap
import os

def word_count_processor(text: str) -> Dict:
    """Process chunk to count words."""
    words = text.lower().replace(',', '').replace('.', '').split()
    return {
        'total_words': len(words),
        'word_frequencies': {word: words.count(word) for word in set(words)},
    }


def char_count_processor(text: str) -> Dict:
    """Process chunk to count characters."""
    return {
        'total_chars': len(text),
        'char_frequencies': {char: text.count(char) for char in set(text)},
    }


def unique_words_processor(text: str) -> Dict:
    """Process chunk to count unique words."""
    words = text.lower().replace(',', '').replace('.', '').split()
    return {
        'unique_words_count': len(set(words)),
        'unique_words': set(words), # Changed to set
    }