from .interfaces import okx

async def main():
    print(await okx.get_funding_rate())
