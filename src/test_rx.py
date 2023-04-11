import rx
from rx import operators as op
import asyncio
import traceback
import random
import logging
from contextlib import suppress
import time

counter = 0

def sync_mapper(v):
    global counter
    counter += 1
    sleep = random.randrange(1, 5)
    print(f'mapper counter={counter} v={v} sleep={sleep}')
    time.sleep(sleep)
    if counter == 1:
        print('Raise exception!')
        raise RuntimeError('exception during')
    return f'{v}+v'

# def subject():
#     subject = rx.subject.AsyncSubject()
#     subject.pipe(
#         op.do_action(lambda i: print(f'subject pipe {i}')),
#         op.flat_map(lambda i: print('subjet async') or rx.from_future(asyncio.create_task(mapper(i))).pipe(op.retry(3))),
#         op.do_action(lambda i: print(f'subject pipe after async {i}')),
#     ).subscribe(
#         on_next=lambda i: print(f'subject on_next {i}'),
#         on_completed=lambda: print(f'subject on_completed'),
#         on_error=lambda error: logging.error(f'on_error {error.__class__.__name__}: {error}') or traceback.print_tb(error.__traceback__) or print('on_error #2'),
#     )
#     return subject

async def run():
    # await rx.of(1, 2, 3).pipe(
    #     op.subscribe_on(rx.scheduler.eventloop.AsyncIOScheduler(asyncio.get_running_loop())),
    #     op.flat_map(lambda i: rx.from_future(asyncio.create_task(mapper(i))).pipe(op.do_action(lambda i: print(f'future {i}')))),
    #     op.do_action(lambda i: print(f'do {i}')),
    #     op.do(subject()),
    #     op.do_action(on_next=lambda i: print(f'do after subject {i}'), on_completed=lambda: print('on_completed'))
    # ).share
    # await asyncio.gather(*asyncio.all_tasks() - {asyncio.current_task()})
    # print('done')

    def main_obs():
        print('main_obs')
        yield 1

    async def async_request(transactions):
        return await rx.of(transactions).pipe(
            op.map(sync_mapper),
            op.retry(2)
        )

    scheduler = rx.scheduler.eventloop.AsyncIOScheduler(asyncio.get_running_loop())

    def subject():
        
        subj = rx.subject.Subject()
        subj.pipe(
            op.do_action(on_next=lambda i: print(f'subj on_next before request {i}')),
            op.flat_map(lambda i: rx.from_future(asyncio.create_task(async_request(i)))),
        ).subscribe(
            on_error=lambda error: logging.error(f'subj on_error {error.__class__.__name__}: {error}') or traceback.print_tb(error.__traceback__) or print('subj on_error #2'),
            on_next=lambda i: print(f'subj on_next {i}'),
            on_completed=lambda: print('subj on_completed'),
        )
        return subj
    
    print('start')
    rx.using(lambda: None, lambda _: print('create') or rx.of(1)).pipe(
        op.subscribe_on(scheduler),
        op.do_action(
            on_error=lambda error: logging.error(f'on_error {error.__class__.__name__}: {error}') or traceback.print_tb(error.__traceback__) or print('on_error #2'),
            on_next=lambda i: print(f'on_next {i}'),
            on_completed=lambda: print('on_completed'),
        )
    ).subscribe(subject())
    print('done')

def main():
    loop = asyncio.new_event_loop()
    loop.create_task(run())
    aws = asyncio.all_tasks(loop)
    logging.info('Running loop tasks')
    while len(aws) > 0:
        print(aws)
        loop.run_until_complete(asyncio.gather(*aws))
        aws = asyncio.all_tasks(loop)

if __name__ == '__main__':
    main()