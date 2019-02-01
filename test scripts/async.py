import asyncio
import time

loop = asyncio.get_event_loop()  

async def get_chat_id(name):
    time.sleep(3)
    # await asyncio.sleep(3)
    end_run()

async def main():
    # loop.run_until_complete(get_chat_id("django"))  
    # await get_chat_id('python')
    # print("in main()")
    # time.sleep(4)
    print("in main()")

def end_run():
    print("End Run")

# task = [asyncio.ensure_future(main())]

# loop.run_until_complete(asyncio.wait(task))
def run():
    loop.create_task(get_chat_id("idiot"))
    loop.create_task(main())
    # return "test"
    # asyncio.ensure_future(get_chat_id("idiot"))
    # asyncio.ensure_future(main())
    # return "test"
    loop.run_forever()

output = run()
print(output)