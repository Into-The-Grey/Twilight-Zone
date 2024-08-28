import os
import shutil
import time
import csv
import logging
import nltk
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pdfplumber

# Load configuration from config.json
with open("config.json", "r") as config_file:
    config = json.load(config_file)


# Ensure all necessary directories exist
def ensure_directory_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        logging.info(f"Created missing directory: {path}")


# Ensure directory paths exist
ensure_directory_exists(config["history_file"]["directory"])
ensure_directory_exists(config["log_file"]["directory"])
ensure_directory_exists(config["watched_folder"])
ensure_directory_exists(config["processing_folder"])
ensure_directory_exists(config["output_folder"])
ensure_directory_exists(config["issues_folder"])
ensure_directory_exists(config["compressed_folder"])

# Setup NLTK if specified in config
if config.get("nltk", {}).get("download_data", False):
    nltk.download("punkt")
    if config.get("nltk", {}).get("stopwords", False):
        nltk.download("stopwords")

# Construct file paths using config settings
history_file_path = os.path.join(
    config["history_file"]["directory"], config["history_file"]["name"]
)
log_file_path = os.path.join(
    config["log_file"]["directory"], config["log_file"]["name"]
)


# Ensure that the history and log files exist
def ensure_file_exists(file_path):
    if not os.path.exists(file_path):
        with open(file_path, "w") as f:
            pass
        logging.info(f"Created missing file: {file_path}")


ensure_file_exists(history_file_path)
ensure_file_exists(log_file_path)

# Setup logging
logging.basicConfig(
    filename=log_file_path,
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logging.info(f"{config['app_name']} app started.")

# Directories from config
watched_folder = config["watched_folder"]
processing_folder = config["processing_folder"]
output_folder = config["output_folder"]
issues_folder = config["issues_folder"]
compressed_folder = config["compressed_folder"]


class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        file_name, file_ext = os.path.splitext(event.src_path)
        if file_ext.lower() == ".pdf":
            logging.info(f"New PDF detected: {event.src_path}")
            self.process_pdf(event.src_path)

    def process_pdf(self, pdf_path):
        try:
            # Copy the PDF to the processing folder and compressed folder
            pdf_name = os.path.basename(pdf_path)
            if compressed_folder:
                compressed_pdf = shutil.copy2(pdf_path, compressed_folder)
            else:
                logging.error("Compressed folder is not set in config.json.")
                return
            if processing_folder:
                pdf_copy = shutil.copy2(pdf_path, processing_folder)
            else:
                logging.error("Processing folder is not set in config.json.")
                return
            os.remove(pdf_path)

            logging.info(f"PDF copied to processing and compressed folders: {pdf_name}")

            start_time = time.time()
            with pdfplumber.open(pdf_copy) as pdf:
                num_pages = len(pdf.pages)
                text_content = []
                topics = []

                for i, page in enumerate(pdf.pages):
                    text = page.extract_text()
                    if text:
                        text_content.append(text)
                        topics += nltk.word_tokenize(text)

                        output_file = os.path.join(
                            output_folder or "", f"{pdf_name}_page_{i + 1}.txt"
                        )
                        with open(output_file, "w", encoding="utf-8") as f:
                            f.write(text)

                    logging.info(f"Processed page {i + 1}/{num_pages} of {pdf_name}")

                # Generate metadata review
                metadata = pdf.metadata or {}
                review_file = os.path.join(
                    output_folder or "", f"{pdf_name}_##DocReview##.txt"
                )
                with open(review_file, "w", encoding="utf-8") as f:
                    f.write(f"PDF Name: {pdf_name}\n")
                    f.write(f"Page Count: {num_pages}\n")
                    f.write(f"Topics: {', '.join(set(topics))}\n")
                    f.write(f"Metadata: {metadata}\n")

                logging.info(f"Metadata review generated for {pdf_name}")

            # Log to History.csv
            elapsed_time = time.time() - start_time
            with open(history_file_path, "a", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(
                    [
                        pdf_name,
                        num_pages,
                        time.strftime("%Y-%m-%d %H:%M:%S"),
                        elapsed_time,
                    ]
                )

            logging.info(
                f"History log updated for {pdf_name} with processing time {elapsed_time:.2f}s"
            )

        except Exception as e:
            logging.error(f"Error processing {pdf_name}: {e}")
            if issues_folder:
                shutil.move(
                    pdf_path, os.path.join(issues_folder, os.path.basename(pdf_path))
                )
            else:
                logging.error("Issues folder is not set in config.json.")


if __name__ == "__main__":
    event_handler = PDFHandler()
    observer = Observer()
    if watched_folder:
        observer.schedule(event_handler, watched_folder, recursive=False)
    observer.start()

    logging.info(f"Watching folder: {watched_folder} for new PDFs...")

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        observer.stop()
        logging.info("Twilight Zone app stopped by user.")

    observer.join()
