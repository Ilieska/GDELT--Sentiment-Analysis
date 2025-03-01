import asyncio
import aiohttp
import pandas as pd
import csv
import os
from tqdm.asyncio import tqdm  
from newspaper import Article
from bs4 import BeautifulSoup
import random

SEM_LIMIT = 30
SAVE_FILE = "clean_articles.csv" 
SAMPLE_PERCENTAGE = 0.10  

async def get_article_text_newspaper(url):
    try:
        article = Article(url)
        article.download()
        article.parse()
        return article.title, article.text
    except:
        return None, None

async def get_article_text_bsoup(session, url):
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            article_tag = soup.find('article') or \
                          soup.find('div', class_='article-content') or \
                          soup.find('div', id='main-content')

            if article_tag:
                return soup.title.string.strip(), article_tag.get_text(separator=' ', strip=True)
            else:
                return None, None
    except Exception as e:
        print(f"❌ Error processing {url}: {e}")
        return None, None

async def get_article_content(session, url, semaphore, progress):
    async with semaphore:
        title, text = await get_article_text_newspaper(url)
        if not text:
            title, text = await get_article_text_bsoup(session, url)

        if title and text:
            save_article(url, title, text)  
        
        progress.update(1) 

def save_article(url, title, text):
    file_exists = os.path.isfile(SAVE_FILE) 
    with open(SAVE_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["URL", "Title", "Text"])
        writer.writerow([url, title, text])

def get_processed_urls():
    if not os.path.isfile(SAVE_FILE):
        return set()  
    
    try:
        df = pd.read_csv(SAVE_FILE, usecols=["URL"])
        return set(df["URL"].dropna().tolist()) 
    except Exception as e:
        print(f"⚠️ Error reading {SAVE_FILE}: {e}")
        return set()

def sample_urls(url_list, percentage):
    sample_size = int(len(url_list) * percentage)
    return random.sample(url_list, sample_size)

async def process_urls(url_list):

    semaphore = asyncio.Semaphore(SEM_LIMIT)
    async with aiohttp.ClientSession() as session:
        with tqdm(total=len(url_list), desc="🔄 Processing URLs", unit="article") as progress:
            tasks = [get_article_content(session, url, semaphore, progress) for url in url_list]

            try:
                await asyncio.gather(*tasks) 
            except asyncio.CancelledError:
                print("\n⏹️ Process interrupted! Data saved.")


data_cleaned = pd.read_csv('dataset4_cleaned.csv')
all_urls = set(data_cleaned['SOURCEURL'].dropna().tolist()) 

sampled_urls = sample_urls(list(all_urls), SAMPLE_PERCENTAGE)

processed_urls = get_processed_urls()
remaining_urls = list(set(sampled_urls) - processed_urls)

if remaining_urls:
    print(f"🔄 Resuming from last stop... Processing {len(remaining_urls)} URLs.")
    try:
        asyncio.run(process_urls(remaining_urls))
    except KeyboardInterrupt:
        print("\n⏹️ Keyboard interrupt detected. Data saved to 'clean_articles.csv'.")
else:
    print("✅ All URLs are already processed. Nothing to do!")
