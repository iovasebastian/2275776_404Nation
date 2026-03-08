import asyncio
from app import get_actuators

async def main():
	actuators = await get_actuators()
	print(actuators)

if __name__ == "__main__":
	asyncio.run(main())