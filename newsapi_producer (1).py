from newsapi import NewsApiClient
import json
from kafka import KafkaProducer

# Get your free API key from https://newsapi.org/, just need to sign up for an account
key = "850861d02f974d04b83c9146870f6db3"

# Initialize API endpoint
newsapi = NewsApiClient(api_key=key)

# Define the list of media sources
sources = 'bbc-news,cnn,fox-news,nbc-news,the-guardian-uk,the-new-york-times,the-washington-p>

# /v2/everything
all_articles = newsapi.get_everything(q='nepal',
                                      sources=sources,
                                      language='en')

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Function to remove null values from an article
def remove_null_values(article):
    return {k: v for k, v in article.items() if v is not None}

# Clean the articles and filter out those containing "[Removed]"
for article in all_articles['articles']:
    if '[Removed]' in article['title']:
        continue  # Skip articles with "[Removed]" in the title
    
    clean_article = remove_null_values(article)
    print(clean_article['title'])
    producer.send('my-news', json.dumps(clean_article).encode('utf-8'))

# Close the producer after sending all messages
producer.close()

