import nltk
nltk.download('punkt')  # Download the punkt tokenizer data (if not already downloaded)

from nltk.tokenize import word_tokenize

file_path = 'generated_queries.sql'  # Replace with the path to your text file

def count_tokens_in_file(file_path):
    with open(file_path, 'r') as file:
        text = file.read()
        tokens = word_tokenize(text)
        token_count = len(tokens)
        return token_count

token_count = count_tokens_in_file(file_path)
print(f'Total tokens in {file_path}: {token_count}')
