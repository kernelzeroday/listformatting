import argparse
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define a function to process each chunk
def process_chunk(chunk, file_handles):
    for line in chunk:
        try:
            # Split the line into hash and value
            hash_part, value_part = line.strip().split(':')

            # Get the length of the hash
            hash_length = len(hash_part)

            # Check if we already have a file open for this hash length
            if hash_length not in file_handles:
                # Open a new file for this hash length
                file_handles[hash_length] = open(f'hash_length_{hash_length}.txt', 'w')

            # Write the line to the appropriate file
            file_handles[hash_length].write(line)
        except ValueError as e:
            logging.warning(f"Skipping bad line: {line.strip()} - Error: {e}")

def process_file(filename, chunk_size=1000):
    file_handles = {}

    try:
        with open(filename, 'r') as file:
            chunk = []
            for line in file:
                chunk.append(line)
                if len(chunk) >= chunk_size:
                    process_chunk(chunk, file_handles)
                    chunk = []
            # Process the last chunk
            if chunk:
                process_chunk(chunk, file_handles)
    except FileNotFoundError as e:
        logging.error(f"File not found: {filename} - Error: {e}")
        return
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return
    finally:
        # Close all open files
        for f in file_handles.values():
            f.close()
        logging.info("Processing complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process a file of hash:value pairs.')
    parser.add_argument('filename', type=str, help='The filename to process')
    args = parser.parse_args()

    process_file(args.filename)

