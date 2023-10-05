import asyncio
from dataclasses import dataclass, asdict

import aiohttp
import redis

######### CONFIGURATION #########
EPOCH = 3
BATCH_SIZE = 10
SLEEP_INTERVAL_SECONDS = 0.5
SLEEP_INTERVAL_EPOCH = 50
REDIS_HOST = "localhost"
REDIS_PORT = 6379

GANJOOR_POET_API = "http://c.ganjoor.net/beyt-json.php"
#################################


@dataclass
class Beyt:
    m1: str
    m2: str
    poet: str
    url: str

    @property
    def key(self):
        return self.m1 + self.m2


# Create a Redis connection
r = redis.Redis(REDIS_HOST, REDIS_PORT, db=4)


def save_to_redis(data: Beyt):
    r.hmset(data.key, asdict(data))


async def get_data(session):
    async with session.get(GANJOOR_POET_API) as response:
        data = await response.json()
    save_to_redis(Beyt(**data))


async def main():
    epoch_num = 0

    # Create a ClientSession which will hold the TCP connection
    # open for multiple requests
    async with aiohttp.ClientSession() as session:
        while True:
            for _ in range(EPOCH):
                tasks = [
                    asyncio.create_task(get_data(session)) 
                    for _ in range(BATCH_SIZE)
                ]
                await asyncio.gather(*tasks)
            print(f"{epoch_num=:03} Done ")
            epoch_num += 1
            if epoch_num % SLEEP_INTERVAL_EPOCH == 0:
                print(f"Sleeping for {SLEEP_INTERVAL_SECONDS} seconds")
                await asyncio.sleep(
                    SLEEP_INTERVAL_SECONDS
                )  # Use asyncio.sleep instead of time.sleep


if __name__ == "__main__":
    asyncio.run(main())
