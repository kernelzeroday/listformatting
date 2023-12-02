import argparse
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_chunk(chunk, output_file):
    for line in chunk:
        try:
            # Split the line into hash and value
            hash_part, value_part = line.strip().split(':')

            # Check if the hash part contains an '@' symbol
            if '@' not in hash_part:
                # Write the line to the output file
                output_file.write(line)
        except ValueError as e:
            logging.warning(f"Skipping bad line: {line.strip()} - Error: {e}")

def process_file(input_filename, output_filename, chunk_size=1000):
    try:
        with open(input_filename, 'r') as input_file, open(output_filename, 'w') as output_file:
            chunk = []
            for line in input_file:
                chunk.append(line)
                if len(chunk) >= chunk_size:
                    process_chunk(chunk, output_file)
                    chunk = []
            # Process the last chunk
            if chunk:
                process_chunk(chunk, output_file)
    except FileNotFoundError as e:
        logging.error(f"File not found: {input_filename} - Error: {e}")
        return
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return
    finally:
        logging.info("Processing complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Remove lines with @ symbol in the first field.')
    parser.add_argument('input_file', type=str, help='The input filename')
    parser.add_argument('output_file', type=str, help='The output filename')
    args = parser.parse_args()

    process_file(args.input_file, args.output_file)

