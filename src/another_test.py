import rx
from rx import operators as rxop
from rx.subject import ReplaySubject, Subject
import asyncio

async def main():
    interval = rx.interval(2.0)
    subject = Subject()
    interval.subscribe(subject, scheduler=rx.scheduler.eventloop.AsyncIOScheduler(asyncio.get_running_loop()))

    print(await subject.pipe(rxop.take(3), rxop.to_list()))

asyncio.run(main())