import asyncio
import time
from datetime import datetime, timedelta
import csv
from twikit import Client, TooManyRequests
from random import randint

since_date = '2024-12-01'
until_date = '2024-12-12'
MINIMUM_TWEETS = 1000
QUERY_TEMPLATE = '#SaveBangladeshiHindus min_faves:5 until:{until_date} since:{since_date}'
print(QUERY_TEMPLATE)
csv_file_name = f"tweets_{since_date}to{until_date}.csv"

async def get_tweets(client, query, tweets=None):
    if tweets is None:
        print(f'{datetime.now()} - Getting tweets...')
        tweets = await client.search_tweet(query, product='Top')
    else:
        wait_time = randint(5, 60)
        print(f'{datetime.now()} - Getting next tweets after {wait_time} seconds ...')
        await asyncio.sleep(wait_time)
        tweets = await tweets.next()

    return tweets

async def main():
    # Dynamic date range for the query
    today = datetime.today()
    #query = QUERY_TEMPLATE.format(today.strftime('%Y-%m-%d'), (today - timedelta(days=120)).strftime('%Y-%m-%d'))

    query = QUERY_TEMPLATE.format(until_date=until_date, since_date=since_date)
    # Initialize client
    client = Client(language='en-US')
    client.load_cookies('cookies.json')

    # Prepare CSV file
    with open(csv_file_name, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Tweet_count', 'Username', 'Text', 'Created At', 'Retweets', 'Likes'])

    tweet_count = 0
    tweets = None

    while tweet_count < MINIMUM_TWEETS:
        try:
            tweets = await get_tweets(client, query, tweets)
        except TooManyRequests as e:
            rate_limit_reset = datetime.fromtimestamp(e.rate_limit_reset)
            print(f'{datetime.now()} - Rate limit reached. Waiting until {rate_limit_reset}')
            wait_time = rate_limit_reset - datetime.now()
            await asyncio.sleep(wait_time.total_seconds())
            continue
        except Exception as e:
            print(f'{datetime.now()} - An error occurred: {e}')
            break

        if not tweets:
            print(f'{datetime.now()} - No more tweets found')
            break

        with open(csv_file_name, 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for tweet in tweets:
                tweet_count += 1
                tweet_data = [
                    tweet_count,
                    tweet.user.name,
                    tweet.text,
                    tweet.created_at,
                    tweet.retweet_count,
                    tweet.favorite_count,
                ]
                writer.writerow(tweet_data)

        print(f'{datetime.now()} - Got {tweet_count} tweets')

    print(f'{datetime.now()} - Done! Total tweets collected: {tweet_count}')

if __name__ == "__main__":
    asyncio.run(main())