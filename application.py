import asyncio
import functools
import json
import os
import signal
import sys
from datetime import datetime
from multiprocessing import Process
from pathlib import Path

import sdnotify
import toml

from setproctitle import setproctitle
import configuration.settings as cs

from modules.reports.producer.plates import PlatesReportProducer
from modules.reports.notifier.consumables import ConsumablesNotifier
from modules.reports.notifier.incomings import IncomingsNotifier


class Application:
    def __init__(self):
        self.__processes = []
        self.__logger: object = None
        self.__dbconnector_is: object = None
        self.__eventloop = None
        self.__name = 'reports'

    def _initialize(self):
        n = sdnotify.SystemdNotifier()
        setproctitle('reports-main')
        config = toml.load(cs.CONFIG_FILE)
        self.__logger = AsyncLogger(f'{cs.LOG_PATH}/integration.log').getlogger()
        try:
            self.__logger = self.__logger.getlogger()
            await self.__logger.info('Starting...')
            self.__dbconnector_is = AsyncDBPool(host=config['integration']['rdbs']['host'],
                                                port=config['integration']['rdbs']['port'],
                                                login=config['integration']['rdbs']['login'],
                                                password=config['integration']['rdbs']['password'],
                                                database=config['integration']['rdbs']['database'])
            await self.__dbconnector_is.connect()
            plates_report = PlatesReportProducer()
            plates_report_proc = Process(target=plates_report.run, name='plates_reporting')
            self.__processes.append(plates_report_proc)
            plates_report_proc.start()
            consumables_notifier = ConsumablesNotifier()
            consumables_notifier_proc = Process(target=consumables_notifier.run, name='consumables_notifier')
            self.__processes.append(consumables_notifier_proc)
            consumables_notifier_proc.start()
            incomings_notifier = IncomingsNotifier()
            incomings_notifier_proc = Process(target=incomings_notifier.run, name='incomings_notifier')
            self.__processes.append(incomings_notifier_proc)
        except:
            self.__logger.exception({'main': self.__name})


    async def _signal_cleanup(self):
        await self.__logger.warning('Shutting down...')
        await self.__dbconnector_is.disconnect()
        await self.__dbconnector_wp.disconnect()
        await self.__logger.shutdown()

    async def _signal_handler(self, signal):
        # stop while loop coroutine
        self.eventsignal = True
        tasks = [task for task in asyncio.all_tasks(self.eventloop) if task is not
                 asyncio.tasks.current_task()]
        for t in tasks:
            t.cancel()
        asyncio.ensure_future(self._signal_cleanup())
        # perform eventloop shutdown
        try:
            self.eventloop.stop()
            self.eventloop.close()
        except:
            pass
        # close process
        os._exit(0)

    def run(self):
        # use policy for own event loop
        self.eventloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.eventloop)
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(s, functools.partial(asyncio.ensure_future, self._signal_handler(s)))
        # # try-except statement for signals
        self.eventloop.run_until_complete(self._initialize())
        self.eventloop.run_until_complete(self._dispatch())


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    app = Application(loop)
    loop.run_until_complete(app.start(loop))
    loop.run_until_complete(app.dispatch())
