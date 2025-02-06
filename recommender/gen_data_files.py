import os
import glob
import pandas as pd
from utils import show_dims
from datasets import load_dataset


# If podcast_segment.csv is not present (if statement)
if not os.path.exists("podcast_segment.csv"):
    print("podcast_segment.csv not found. Generating podcast_segment.csv")

    document_files = glob.glob("documents/*.jsonl")

    document_dfs =[]
    for document_path in document_files:
        print(f"Processing {document_path}")
        chunks = pd.read_json(document_path, lines=True, chunksize=10000)
        file_df = pd.concat(chunks, ignore_index=True)
        document_dfs.append(file_df)

    document_df = pd.concat(document_dfs, ignore_index=True)
    formatted_document_df = pd.DataFrame({
        'id': document_df['custom_id'],
        'podcast_id': document_df['body'].apply(lambda x: x['metadata']['podcast_id']),
        'start_time': document_df['body'].apply(lambda x: x['metadata']['start_time']),
        'end_time': document_df['body'].apply(lambda x: x['metadata']['stop_time']),
        'content': document_df['body'].apply(lambda x: x['input'])
    })
    show_dims(formatted_document_df)

    embedding_files = glob.glob("embedding/*.jsonl")

    embedding_dfs = []
    for embedding_path in embedding_files:
        print(f"Processing {embedding_path}")
        chunks = pd.read_json(embedding_path, lines=True, chunksize=10000)
        file_df = pd.concat(chunks, ignore_index=True)
        embedding_dfs.append(file_df)

    embedding_df = pd.concat(embedding_dfs, ignore_index=True)
    formatted_embedding_df = pd.DataFrame({
        'id': embedding_df['custom_id'],
        'embedding': embedding_df['response'].apply(lambda x: x['body']['data'][0]['embedding'])
    })
    show_dims(formatted_embedding_df)

    podcast_segment_df = pd.merge(formatted_document_df, formatted_embedding_df, on='id', how='inner')
    show_dims(podcast_segment_df)

    print("Saving podcast_segment.csv")
    podcast_segment_df.to_csv("podcast_segment.csv", index=False)
    print("podcast_segment.csv saved successfully")

if not os.path.exists("dataset.csv"):
    print("dataset.csv not found. Generating dataset.csv")

    print("Loading dataset")
    ds = load_dataset("Whispering-GPT/lex-fridman-podcast")
    ds_frame = pd.DataFrame(ds['train'])
    show_dims(ds_frame)
    ds_frame.to_csv("dataset.csv", index=False)
    print("dataset.csv saved successfully")
