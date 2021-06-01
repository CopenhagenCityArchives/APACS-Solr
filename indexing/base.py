from abc import ABC, abstractmethod
import sys, traceback
from datetime import datetime
from time import time
import pysolr

from config import Config
from sns import SNS_Notifier

class IndexerBase(ABC):
    """Abstract Indexer class to be extended for each collection type."""


    def __init__(self):
        """Initialize variables used for progress logging, and logging."""
        self.rate_entries = 0
        self.rate_time = 0.0
        self.progress = 0.0
        self.progress_threshold = 0.05
        self.progress_threshold_next = self.progress_threshold
        self.log_prefixes = []
        self.total = None
        self.commit_threshold = 10000
        self.documents = []
    

    def log(self, msg):
        """Prints a message prefixed with a timestamp, and a number of log prefixes."""
        print("> ".join([datetime.now().isoformat()] + self.log_prefixes + [msg]), flush=True)


    def index(self):
        """Main method for performing the indexing, tying together all abstracted methods."""
        self.log_prefixes.append(self.collection_info())

        self.log_prefixes.append("solr")
        self.log("Connecting to Solr... ")
        self.solr = pysolr.Solr(Config['solr']['url'], auth=(Config['solr']['user'], Config['solr']['password']), timeout=300)
        self.log("OK.")
        self.log_prefixes.pop()

        self.log(f"Setting up indexing {self.collection_info()} (id={self.collection_id()})")
        try:
            self.log_prefixes.append("setup")
            self.setup()
            self.log_prefixes.pop()
        except Exception as e:
            self.handle_error("Exception occured during setup()!", e)
        
        try:
            self.log_prefixes.append("total")
            self.log("Counting total documents...")
            self.total = self.get_total()
            self.log(f"OK. Counted {self.total} documents.")
            self.log_prefixes.pop()
        except Exception as e:
            self.handle_error("Exception occured during get_total()!", e)

        if Config["index-delete"]:
            try:
                self.log_prefixes.append("delete")
                self.log(f"Deleting existing documents for {self.collection_info()}...")
                self.delete()
                self.log("OK.")
                self.log_prefixes.pop()
            except Exception as e:
                self.handle_error("Exception occured during delete()!", e)

        self.log(f"Beginning indexing...")
        self.log_prefixes.append("indexing")
        self.rate_time = time()
        last_progress_i = None
        last_i = None
        try:
            for i, entry in enumerate(self.get_entries(), start=1):
                last_i = i
                self.progress = i / self.total
                try:
                    self.log_prefixes.append("handle_entry")
                    self.handle_entry(entry)
                    self.log_prefixes.pop()
                    if len(self.documents) >= self.commit_threshold:
                        self.solr.add(self.documents, commit=True)
                        self.documents = []
                    if self.progress > self.progress_threshold_next:
                        last_progress_i = i
                        self.progress_threshold_next += self.progress_threshold
                        self.print_progress(i)
                    self.rate_entries += 1
                except Exception as e:
                    self.handle_error("Exception occured during handle_entry()!", e, entry=entry)

            # commit any remaining documents
            self.solr.add(self.documents, commit=True)
            self.documents = []

            # only print progress after, if we didn't print already
            if last_progress_i < last_i:
                self.print_progress(last_i)
        except Exception as e:
            self.handle_error("Exception occured during index()!", e)
        self.log_prefixes.pop()


    def delete(self):
        self.solr.delete(q=f"collection_id:{self.collection_id()}", commit=True)


    def handle_error(self, message, exception, entry=None):
        """Called whenever a fatal error occurred that needs to be reported and logged."""
        # We move one level out of log prefixes
        # since the stage clearly failed.
        self.log_prefixes.pop()

        # print to stdout
        type_, value_, traceback_ = sys.exc_info()
        stack = traceback.format_exception(type_, value_, traceback_)
        self.log(f"Indexing aborted: {message}")
        self.log_prefixes.append(type(exception).__name__)
        self.log(str(exception))
        if entry is not None:
            self.log(str(entry))
        self.log_prefixes.append("stack")
        for line in ("\n".join(stack)).rstrip().split("\n"):
            self.log(line)
        
        # report error
        sns_message = f"An error occured during indexing of {self.collection_info()}:\n{message}"
        sns_message += type(exception).__name__
        
        if entry is not None:
            sns_message += f"\n{entry}"

        SNS_Notifier.error(sns_message)

        sys.exit(1)

    def print_progress(self, i):
        """Print a progress message with the current progress, and the processing rate."""
        current_time = time()
        rate = self.rate_entries / (current_time - self.rate_time)
        self.log(f"[{self.progress * 100:.0f} %] {i}/{self.total} ({rate:.0f} docs/sec)")
        self.rate_entries = 0
        self.rate_time = current_time

    @abstractmethod
    def collection_info(self):
        """Returns a text string representing the collection."""
        pass
    

    @abstractmethod
    def collection_id(self):
        """Returns the collection identification number as an integer."""
        pass
    

    @abstractmethod
    def setup(self):
        """Perform setup, ie. connect to databases etc."""
        pass

    
    @abstractmethod
    def get_total(self):
        """Count the total number of documents to be created."""
        pass

    @abstractmethod
    def get_entries(self):
        """Get the entries to be handled. Returns an enumerable, ie. a generator."""
        pass
    

    @abstractmethod
    def handle_entry(self, entry):
        """Handle one of the entries returned by get_entries."""
        pass
