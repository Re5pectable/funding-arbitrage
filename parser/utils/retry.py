from asyncio import sleep

def retry(retries=3, delay=1, catch=(Exception,)):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < retries:
                try:
                    return await func(*args, **kwargs)
                except catch as e:
                    attempts += 1
                    if attempts >= retries:
                        raise e
                    else:
                        await sleep(delay)
        return wrapper
    return decorator