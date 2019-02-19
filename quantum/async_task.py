import asyncio
from threading import Thread

class AsyncTask():

   def __init__(self):
      self.new_loop = asyncio.new_event_loop()
      t = Thread(target=self.start_loop, args=(self.new_loop,))
      t.start()

   def create_task(self, func, *args):
      self.new_loop.call_soon_threadsafe(func, *args)

   def start_loop(self, loop):
      asyncio.set_event_loop(loop)
      loop.run_forever()