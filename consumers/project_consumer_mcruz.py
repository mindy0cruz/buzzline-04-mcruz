"""
project_consumer_mcruz.py

Displays real-time keyword counts per author from Kafka.
"""

import os
import json
from collections import defaultdict, Counter
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# -------------------------------
# Environment
# -------------------------------
load_dotenv()
TOPIC = os.getenv("PROJECT_TOPIC", "buzzline-topic")
GROUP_ID = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")

# -------------------------------
# Data
# -------------------------------
keyword_counts = defaultdict(Counter)

# -------------------------------
# Matplotlib setup
# -------------------------------
fig, ax = plt.subplots()
plt.ion()

def update_chart():
    ax.clear()
    authors = list(keyword_counts.keys())
    if not authors:
        return

    #keywords from all authors
    all_keywords = set()
    for author in authors:
        all_keywords.update(keyword_counts[author].keys())
    all_keywords = sorted(all_keywords)

    # Build the bar chart
    bar_width = 0.8 / max(1, len(authors))
    for i, author in enumerate(authors):
        values = [keyword_counts[author].get(kw, 0) for kw in all_keywords]
        ax.bar([x + i*bar_width for x in range(len(all_keywords))],
               values, width=bar_width, label=author)

    ax.set_xticks([x + bar_width*(len(authors)/2) for x in range(len(all_keywords))])
    ax.set_xticklabels(all_keywords, rotation=45, ha="right")
    ax.set_ylabel("Keyword Count")
    ax.set_xlabel("Keyword")
    ax.set_title("Keyword by Author- Mindy Cruz")
    ax.legend()
    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)

# -------------------------------
# Message processing
# -------------------------------
def process_message(message):
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON: {message}")
            return

    if isinstance(message, dict):
        author = message.get("author", "unknown")
        keyword = message.get("keyword_mentioned", "other")
        keyword_counts[author][keyword] += 1
        update_chart()
    else:
        logger.error(f"Unexpected message type: {type(message)}")

# -------------------------------
# Main
# -------------------------------
def main():
    logger.info(f"Starting consumer for topic '{TOPIC}', group '{GROUP_ID}'")
    consumer = create_kafka_consumer(TOPIC, GROUP_ID)

    try:
        while True:
            
            records = consumer.poll(timeout_ms=1000)
            for partition_records in records.values():
                for record in partition_records:
                    process_message(record.value)
    except KeyboardInterrupt:
        logger.warning("Stopping consumer...")
    finally:
        consumer.close()
        plt.ioff()
        plt.show()
        logger.info("Consumer closed.")

if __name__ == "__main__":
    main()
