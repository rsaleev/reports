import asyncio
import functools
import json
import os
import signal
import sys
from datetime import date, datetime, timedelta

import uvloop
from setproctitle import setproctitle
import toml

import configuration.settings as cs
from utils.asyncsql import AsyncDBPool
from utils.asynclog import AsyncLogger
import pycron


class PlatesReportProducer:
    def __init__(self):
        self.__logger: object = None
        self.__dbconnector_ws: object = None
        self.__dbconnector_is: object = None
        self.__eventsignal = False
        self.__eventloop = None
        self.name = 'PlateDataMiner'
        self.type = 'gather'
        self.alias = 'plates'
        self.__schedule = None

    @property
    def eventloop(self):
        return self.__eventloop

    @eventloop.setter
    def eventloop(self, value):
        self.__eventloop = value

    @eventloop.getter
    def eventloop(self):
        return self.__eventloop

    @property
    def eventsignal(self):
        return self.__eventsignal

    @eventsignal.setter
    def eventsignal(self, value):
        self.__eventsignal = value

    @eventsignal.getter
    def eventsignal(self):
        return self.__eventsignal

    # subprocessing coroutine for fetching and storing data
    # processes 1 device object and one date
    async def _fetch(self, device: dict, date: datetime):
        data_out = await self.__dbconnector_ws.callproc('rep_grz', rows=1, values=[device['terId'], date])
        if data_out is None:
            data_out = {'date': date, 'totalTransits': 0, 'more6symbols': 0, 'less6symbols': 0, 'less6symbols': 0, 'noSymbols': 0, 'accuracy': 0}
        else:
            data_out['noSymbols'] = data_out['totalTransits'] - data_out['more6symbols'] - data_out['less6symbols']
            if data_out['totalTransits'] > 0:
                data_out['accuracy'] = int(round(data_out['more6symbols']/data_out['totalTransits']*100, 2))
        await self.__dbconnector_is.callproc('rep_plates_ins', rows=0,
                                             values=[device['terAddress'], device['terType'], device['terDescription'], data_out['totalTransits'], data_out['more6symbols'], data_out['less6symbols'],
                                                     data_out['noSymbols'], data_out['accuracy'], device['camPlateMode'], data_out['date']])

    # coroutine for iteration over dates
    async def _process(self, device: dict, dates: list):
        tasks = []
        for d in dates:
            tasks.append(self._fetch(device, d))
        await asyncio.gather(*tasks)

    # initialization
    # replaces results
    async def _initialize(self):
        try:
            configuration = toml.load(cs.CONFIG_FILE)
            reports_settings = toml.load(cs.REPORTS_FILE)
            self.__schedule = reports_settings['gather'][self.alias]['schedule']
            setproctitle('rep-plates')
            self.__logger = await AsyncLogger('reports').getlogger()
            await self.__logger.info({'module': self.name, 'msg': 'Starting...'})
            self.__dbconnector_is = AsyncDBPool(host=configuration['integration']['rdbs']['host'],
                                                port=configuration['integration']['rdbs']['port'],
                                                login=configuration['integration']['rdbs']['login'],
                                                password=configuration['integration']['rdbs']['password'],
                                                database=configuration['integration']['rdbs']['database'])
            self.__dbconnector_ws = AsyncDBPool(host=configuration['wisepark']['rdbs']['host'],
                                                port=configuration['wisepark']['rdbs']['port'],
                                                login=configuration['wisepark']['rdbs']['login'],
                                                password=configuration['wisepark']['rdbs']['password'],
                                                database=configuration['wisepark']['rdbs']['database'])
            connection_tasks = []
            connection_tasks.append(self.__dbconnector_is.connect())
            connection_tasks.append(self.__dbconnector_ws.connect())
            await self.__logger.info({'module': self.name, 'msg': 'Polling...'})
            await asyncio.gather(*connection_tasks)
            report_from_dt = datetime.now()
            report_to_dt = datetime.now()
            preparation_tasks = []
            preparation_tasks.append(self.__dbconnector_is.callproc('is_column_get', rows=-1, values=[None]))
            preparation_tasks.append(self.__dbconnector_is.callproc('rep_plates_last_get', rows=1, values=[]))
            columns, last_report = await asyncio.gather(*preparation_tasks)
            dates = []
            if last_report is None or last_report['repDate'] is None:
                report_to_dt = date.today() - timedelta(days=1)
                report_from_dt = report_to_dt - timedelta(days=30)
                days_interval = report_to_dt-report_from_dt
                dates = [report_from_dt + timedelta(days=x) for x in range(0, days_interval.days+1)]
            else:
                date_today = date.today()
                days_interval = date_today-last_report['repDate']
                dates = [last_report['repDate'] + timedelta(days=x) for x in range(0, days_interval.days+1)]
            tasks = []
            for c in columns:
                tasks.append(self._process(c, dates))
            await asyncio.gather(*tasks)
            await self.__dbconnector_is.callproc('is_processes_ins', rows=0, values=[self.name, 1, os.getpid(), datetime.now()])
            await self.__logger.info({'module': self.name, 'msg': 'Started'})
            return self
        except:
            sys.exit(1)

    async def _dispatch(self):
        while not self.eventsignal:
            await self.__dbconnector_is.callproc('is_processes_upd', rows=0, values=[self.name, 1, 0, datetime.now()])
            if pycron.is_now(self.__schedule):
                try:
                    last_rep = await self.__dbconnector_is.callproc('rep_plates_last_get', rows=1, values=[])
                    if last_rep['repDate'] < date.today():
                        columns = await self.__dbconnector_is.callproc('is_column_get', rows=-1, values=[None])
                        date_today = date.today()
                        days_interval = date_today-last_rep['repDate']
                        dates = [last_rep['repDate'] + timedelta(days=x) for x in range(0, days_interval.days)]
                        dates.append(date_today)
                        tasks = []
                        for c in columns:
                            tasks.append(self._process(c, dates))
                        await asyncio.gather(*tasks)
                except:
                    await self.__logger.exception({'module': self.name})
            await asyncio.sleep(60)

    async def _signal_cleanup(self):
        await self.__logger.warning({'module': self.name, 'msg': 'Shutting down'})
        closing_tasks = []
        closing_tasks.append(self.__dbconnector_is.disconnect())
        closing_tasks.append(self.__dbconnector_ws.disconnect())
        closing_tasks.append(self.__logger.shutdown())
        await asyncio.gather(*closing_tasks, return_exceptions=True)

    async def _signal_handler(self, signal):
        # stop while loop coroutine
        self.eventsignal = True
        tasks = [task for task in asyncio.all_tasks(self.eventloop) if task is not
                 asyncio.tasks.current_task()]
        for t in tasks:
            t.cancel()
        await asyncio.gather(self._signal_cleanup(), return_exceptions=True)
        # perform eventloop shutdown
        try:
            self.eventloop.stop()
            self.eventloop.close()
        except:
            pass
        # close process
        sys.exit(0)

    def run(self):
        # use own loop
        uvloop.install()
        self.eventloop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.eventloop)
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        # add signal handler to loop
        for s in signals:
            self.eventloop.add_signal_handler(s, functools.partial(asyncio.ensure_future,
                                                                   self._signal_handler(s)))
        # try-except statement for signals
        try:
            self.eventloop.run_until_complete(self._initialize())
            self.eventloop.run_until_complete(self._dispatch())
        except asyncio.CancelledError:
            pass
