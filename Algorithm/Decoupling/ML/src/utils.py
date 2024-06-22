from typing import Any, Literal, Awaitable, Optional

from config import *

import sys
import uuid
from time import time, sleep
from datetime import datetime
import itertools
import asyncio
from concurrent import futures
from queue import Queue

import pandas as pd
import numpy as np
from outliers import smirnov_grubbs as grubbs
from pyod.models import lof, cof, inne, ecod, lscp
import sranodec as anom
from statsmodels.tsa.api import adfuller, VAR
from statsmodels.tsa.vector_ar import vecm
from statsmodels.tsa.arima.model import ARIMA

import taos
import redis.asyncio as redis
import aiomqtt

import orjson as json
from dependency_injector import containers, providers
from dependency_injector.wiring import Provide, inject

if sys.platform.lower() == "win32" or os.name.lower() == "nt":
    from asyncio import set_event_loop_policy, WindowsSelectorEventLoopPolicy
    set_event_loop_policy(WindowsSelectorEventLoopPolicy())

import warnings
warnings.filterwarnings("ignore")

INTERVAL = 10
FORECAST_STEP = 5
HISTORY_STEP = 25
DECIMALS = 3
ADFULLER_THRESHOLD_VAL = 0.05
MAX_QUEUE_SIZE = 10

# 机理30% 诊断70% 主40% 提纯30% 制氢30% lscp70% grubbs15% sra15%
ANOMALY_SCORE = 70
ALERT_SCORE = 30
PER_ALERT_SCORE = 10
ANOMALY_COL_RATIO = 3
LSCP_SCORE = ANOMALY_SCORE * 0.4 * 0.7
LSCP_PG_SCORE = ANOMALY_SCORE * 0.3 * 0.7
LSCP_PEM_SCORE = ANOMALY_SCORE * 0.3 * 0.7
GRUBBS_SCORE = ANOMALY_SCORE * 0.4 * 0.15
GRUBBS_PG_SCORE = ANOMALY_SCORE * 0.3 * 0.15
GRUBBS_PEM_SCORE = ANOMALY_SCORE * 0.3 * 0.15
SRA_SCORE = ANOMALY_SCORE * 0.4 * 0.15
SRA_PG_SCORE = ANOMALY_SCORE * 0.3 * 0.15
SRA_PEM_SCORE = ANOMALY_SCORE * 0.3 * 0.15

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

a_df = [pd.DataFrame() for _ in range(2)]
a_df_PEM = [pd.DataFrame() for _ in range(2)]
a_df_PG = [pd.DataFrame() for _ in range(2)]
f_df = [pd.DataFrame() for _ in range(2)]
vrfData = [{} for _ in range(2)]
arfData = [{} for _ in range(2)]
healthQ = [Queue(maxsize=MAX_QUEUE_SIZE) for _ in range(2)]
currentTime = [datetime.now() for _ in range(2)]
now = ["" for _ in range(2)]


class TaosService:

    def __init__(self, taos: taos.TaosConnection) -> None:
        self._taos = taos

    def query(self, sql: str) -> taos.TaosResult:
        return self._taos.query(sql)

    def execute(self, sql: str) -> int:
        return self._taos.execute(sql)


class TaosContainer(containers.DeclarativeContainer):

    def __init_taos(): # type: ignore
        conn = taos.connect(
            host=TAOS_IP,
            port=TAOS_PORT,
            user=TAOS_USERNAME,
            password=TAOS_PASSWORD,
            database=TAOS_DATABASE,
        )
        print("----build new Taos connection----")
        # conn.execute(
        #     "create table if not exists alert (ts TIMESTAMP, A INT, PEM INT, PG INT)"
        # )
        return conn
    
    _taos_conn = providers.Resource(
        __init_taos,
    )

    service = providers.Factory(
        TaosService,
        taos=_taos_conn,
    )


class RedisService:

    def __init__(self, redis: redis.Redis) -> None:
        self._redis = redis

    async def hget(self, name: str, key: str) -> Awaitable[Optional[str]]:
        return await self._redis.hget(name, key) # type: ignore

    async def hgetall(self, name: str) -> Awaitable[dict]:
        return await self._redis.hgetall(name) # type: ignore

    async def hset(self, name: str, key: str, value: str) -> Awaitable[int]:
        return await self._redis.hset(name, key, value) # type: ignore


class RedisContainer(containers.DeclarativeContainer):
    # config = providers.Configuration()

    def _init_redis() -> redis.Redis: # type: ignore
        pool = redis.ConnectionPool.from_url(
            f"redis://{REDIS_USERNAME}:{REDIS_PASSWORD}@{REDIS_IP}:{REDIS_PORT}/{REDIS_DB}",
            encoding="utf-8",
            decode_responses=True,
        )
        conn = redis.Redis.from_pool(pool)
        print("----build new Redis connection----")
        return conn

    _redis_conn = providers.Resource(
        _init_redis,
    )

    service = providers.Factory(
        RedisService,
        redis=_redis_conn,
    )


class AppContainer(containers.DeclarativeContainer):
    redis_package = providers.Container(RedisContainer)
    taos_package = providers.Container(TaosContainer)


class GetData:

    def __init__(
        self,
        unit: str,
        conn: TaosContainer = Provide[AppContainer.taos_package.service],
    ) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.fore_SQL = self._forecast_sql(unit)
        self.anom_SQL = self._anomaly_df_sql(unit)
        self.anomPEM_SQL = self._anomaly_df_PEM_sql(unit)
        self.anomPG_SQL = self._anomaly_df_PG_sql(unit)
        self.taos_conn = conn

    def _forecast_sql(self, unit) -> str:
        cols = "c0,c5,c6,c2,c34,c35,c36,c37,c38,c39,c27,c28,c29,c30,c31,c32,c33,c138,c140,c187,c201"
        sql = f"select {cols} from s_analog limit 100"
        return sql

    def _anomaly_df_sql(self, unit) -> str:
        cols = ",".join([f"c{i}" for i in range(0, 40)])
        sql = f"select {cols} from s_analog limit 100"
        return sql

    def _anomaly_df_PEM_sql(self, unit) -> str:
        cols = ",".join(
            [
                f"c{i}"
                for i in itertools.chain(
                    range(138, 144), range(182, 202), range(203, 218)
                )
            ]
        )
        sql = f"select {cols} from s_analog limit 100"
        return sql

    def _anomaly_df_PG_sql(self, unit) -> str:
        cols = ",".join([f"c{i}" for i in range(144, 154)])
        sql = f"select {cols} from s_analog limit 100"
        return sql

    async def get_forecast_df(self) -> None:
        result = self.taos_conn.query(self.fore_SQL)
        data = result.fetch_all_into_dict()
        f_df[self.iUnit] = pd.DataFrame(data)
        f_df[self.iUnit] += np.random.uniform(0, 0.001, f_df[self.iUnit].shape)
        # print("f_df: ", f_df[self.iUnit].shape)

    async def get_anomaly_df(self) -> None:
        result = self.taos_conn.query(self.anom_SQL)  # windows不支持异步查询，适合一行一行立即处理的场景
        data = result.fetch_all_into_dict()
        a_df[self.iUnit] = pd.DataFrame(data)
        a_df[self.iUnit] += np.random.uniform(0, 0.001, a_df[self.iUnit].shape)  # 以免除数为0和全常数线性相关
        # print("df: ", a_df[self.iUnit].shape)

    async def get_anomaly_df_PEM(self) -> None:
        result = self.taos_conn.query(self.anomPEM_SQL)  # windows不支持异步查询，适合一行一行立即处理的场景
        data = result.fetch_all_into_dict()
        a_df_PEM[self.iUnit] = pd.DataFrame(data)
        a_df_PEM[self.iUnit] += np.random.uniform(0, 0.001, a_df_PEM[self.iUnit].shape)  # 以免除数为0和全常数线性相关
        # print("df_PEM: ", a_df_PEM[self.iUnit].shape)

    async def get_anomaly_df_PG(self) -> None:
        result = self.taos_conn.query(self.anomPG_SQL)  # windows不支持异步查询，适合一行一行立即处理的场景
        data = result.fetch_all_into_dict()
        a_df_PG[self.iUnit] = pd.DataFrame(data)
        a_df_PG[self.iUnit] += np.random.uniform(0, 0.001, a_df_PG[self.iUnit].shape)  # 以免除数为0和全常数线性相关
        # print("df_PG: ", a_df_PG[self.iUnit].shape)


class AnomalyDetection:

    @inject
    def __init__(self, unit: str) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1

    def _create_detector_list(self) -> list[Any]:
        return [
            lof.LOF(),
            cof.COF(),
            inne.INNE(),
            ecod.ECOD(),
        ]

    def grubbs_t(self, obj: str) -> float:
        if obj == "PEM":
            df = a_df_PEM[self.iUnit]
        elif obj == "PG":
            df = a_df_PG[self.iUnit]
        else:
            df = a_df[self.iUnit]

        if df.shape[0] < 24:
            print("Not enough data for grubbs")
            return 0

        x = 0
        try:
            for i in range(df.shape[1]):
                result = grubbs.two_sided_test_indices(df.iloc[:, i].values, alpha=0.05)
                if result:
                    x += len(result) > 0

            final = x / df.shape[1]
            return final
        except Exception as e:
            print(e)
            return 0

    def lscp_t(self, obj: str) -> float:
        if obj == "PEM":
            df = a_df_PEM[self.iUnit]
        elif obj == "PG":
            df = a_df_PG[self.iUnit]
        else:
            df = a_df[self.iUnit]

        if df.shape[0] < 45:
            print("Not enough data for lscp")
            return 0

        # 不能放到类变量，因为不用obj的特征数量不同
        clf = lscp.LSCP(
            # contamination=COMB(),
            detector_list=self._create_detector_list(),
            n_bins=len(self._create_detector_list()),
        )

        try:
            clf.fit(df)
            result = clf.predict(df)

            final = np.sum(result == 1) / df.shape[1]
            return final
        except Exception as e:
            print(e)
            return 0

    def spectral_residual_saliency(self, obj: str) -> float:
        if obj == "PEM":
            df = a_df_PEM[self.iUnit]
        elif obj == "PG":
            df = a_df_PG[self.iUnit]
        else:
            df = a_df[self.iUnit]

        if df.shape[0] < 24:
            print("Not enough data for srs")
            return 0

        score_window_size = min(df.shape[0], 100)
        spec = anom.Silency(
            amp_window_size=24, series_window_size=24, score_window_size=score_window_size
        )

        try:
            abnormal = 0
            for i in range(df.shape[1]):
                score = spec.generate_anomaly_score(df.values[:, i])
                abnormal += np.sum(score > np.percentile(score, 99)) > 0

            result = abnormal / df.shape[1]
            return float(result)
        except Exception as e:
            print(e)
            return 0


class Forecast:

    def __init__(self, unit: str) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.targetList = [
            ["c5", "c6"],
            ["c2"],
            ["c34", "c35"],
            ["c36", "c37", "c38", "c39", "c27", "c28", "c29", "c30"],
        ]
        self.exogList = [[], ["c201"], [], []]

    def _VECM_forecast(self, data: pd.DataFrame) -> Any:
        # Johansen 检验方法确定协整关系阶数
        rank = vecm.select_coint_rank(data, det_order=0, k_ar_diff=1, signif=0.1)
        model = vecm.VECM(data, k_ar_diff=1, coint_rank=rank.rank)
        yhat = model.fit().predict(steps=FORECAST_STEP)
        # print(yhat)
        return yhat

    def _VAR_forecast(self, data: pd.DataFrame) -> Any:
        # data的索引必须是递增的 data = data.sort_index()
        model = VAR(data, exog=None)
        # 数据量太小maxlags选择10会报错，数据量不变维度多也会报错
        res = model.select_order(maxlags=5)
        best_lag = res.selected_orders["aic"]
        # print(f"best_lag={best_lag}")
        yhat = model.fit(maxlags=int(max(best_lag, 10))).forecast(
            data.values, steps=FORECAST_STEP
        )
        # print(yhat)
        return yhat

    async def vr_forecast(self, MQTTClient: aiomqtt.Client) -> None:
        vrfData[self.iUnit] = {}
        if f_df[self.iUnit].shape[0] < 40:
            print("Not enough data for vr_forecast")
            return

        targetListLen = len(self.targetList)
        assert len(self.exogList) == targetListLen
        for i in range(targetListLen):
            vrdf = f_df[self.iUnit][self.targetList[i] + self.exogList[i]]

            # 随便取一列平稳性测试
            try:
                p = adfuller(vrdf.iloc[:, 0], regression="ct")[1]  # ct: 常数项和趋势项
            except:  # 全常数
                p = 0
            # print(f"p={p}")

            targetLen = len(self.targetList[i])
            try:
                if p <= ADFULLER_THRESHOLD_VAL:  # 平稳的
                    yhat = self._VAR_forecast(vrdf)
                else:
                    yhat = self._VECM_forecast(vrdf)
            except Exception as e:
                print("Exception: ", e)
            else:
                yhat = pd.DataFrame(yhat, columns=vrdf.columns)
                result = pd.concat(
                    [vrdf.iloc[-HISTORY_STEP:, :], yhat], axis=0, ignore_index=True
                )
                vrfData[self.iUnit].update(
                    {
                        col: result[col].round(DECIMALS).values.tolist()
                        for col in result.columns[:targetLen]
                    }
                )
        await MQTTClient.publish(f"H2_{self.unit}/Forecast/VR", json.dumps(vrfData[self.iUnit]))
        # print(vrfData[self.iUnit].keys())

    async def ar_forecast1(self, MQTTClient: aiomqtt.Client) -> None:
        arfData[self.iUnit] = {}
        if f_df[self.iUnit].shape[0] < 40:
            print("Not enough data for ar_forecast1")
            return

        order = (10, 1, 3)
        yhat = []
        ardf = f_df[self.iUnit][["c0", "c31", "c32", "c33", "c138", "c140", "c187"]]

        for i in range(ardf.shape[1]):
            model = ARIMA(ardf.iloc[:, i], order=order)
            forecast = model.fit().forecast(steps=FORECAST_STEP)
            yhat.append(forecast)
        yhatdf = pd.concat(yhat, axis=1)
        yhatdf.columns = ardf.columns
        result = pd.concat(
            [ardf.iloc[-HISTORY_STEP:, :], yhatdf], axis=0, ignore_index=True
        )
        arfData[self.iUnit].update(
            {col: result[col].round(DECIMALS).tolist() for col in result.columns}
        )
        # print(arfData[self.iUnit].keys())
        await MQTTClient.publish(f"H2_{self.unit}/Forecast/AR", json.dumps(arfData[self.iUnit]))


class Health:

    def __init__(
        self,
        unit: str,
        conn: RedisContainer = Provide[AppContainer.redis_package.service],
    ) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1
        self.redis_conn = conn
        self.keys = [
            f"H2_{self.unit}:Mechanism:H2Quality",
            f"H2_{self.unit}:Mechanism:H2Leakage",
            f"H2_{self.unit}:Mechanism:liquidLeakage",
        ]

    def getNow(self) -> None:
        global currentTime, now
        currentTime[self.iUnit] = datetime.now()
        now[self.iUnit] = currentTime[self.iUnit].strftime(DATE_FORMAT)

    async def _machanism_alarm_nums(self) -> int:
        count = 0
        for key in self.keys:
            dic = await self.redis_conn.hgetall(key)
            for v in dic.values():
                # print(v)
                if v != '0':
                    count += 1
                    break
        return count

    async def health_score(
        self,
        MQTTClient: aiomqtt.Client,
        lscp: float,
        lscp_PEM: float,
        lscp_PG: float,
        grubbs: float,
        grubbs_PEM: float,
        grubbs_PG: float,
        SRA: float,
        SRA_PEM: float,
        SRA_PG: float,
    ) -> None:
        score = ANOMALY_SCORE + ALERT_SCORE
        nums = await self._machanism_alarm_nums()
        score -= PER_ALERT_SCORE * nums
        remainder = score - ANOMALY_COL_RATIO
        if lscp > ANOMALY_COL_RATIO:
            score -= LSCP_SCORE * (lscp - remainder) / remainder
        if lscp_PG > ANOMALY_COL_RATIO:
            score -= LSCP_PG_SCORE * (lscp_PG - remainder) / remainder
        if lscp_PEM > ANOMALY_COL_RATIO:
            score -= LSCP_PEM_SCORE * (lscp_PEM - remainder) / remainder
        if grubbs > ANOMALY_COL_RATIO:
            score -= GRUBBS_SCORE * (grubbs - remainder) / remainder
        if grubbs_PG > ANOMALY_COL_RATIO:
            score -= GRUBBS_PG_SCORE * (grubbs_PG - remainder) / remainder
        if grubbs_PEM > ANOMALY_COL_RATIO:
            score -= GRUBBS_PEM_SCORE * (grubbs_PEM - remainder) / remainder
        if SRA > ANOMALY_COL_RATIO:
            score -= SRA_SCORE * (SRA - remainder) / remainder
        if SRA_PG > ANOMALY_COL_RATIO:
            score -= SRA_PG_SCORE * (SRA_PG - remainder) / remainder
        if SRA_PEM > ANOMALY_COL_RATIO:
            score -= SRA_PEM_SCORE * (SRA_PEM - remainder) / remainder

        data = {
            "timestamp": now[self.iUnit],
            "healthscore": round(score, DECIMALS),
        }
        # print(data)
        if healthQ[self.iUnit].full():
            healthQ[self.iUnit].get()
        healthQ[self.iUnit].put(data)

        result_list = list(healthQ[self.iUnit].queue)
        healthscores = []
        timestamps = []

        for item in result_list:
            healthscores.append(item["healthscore"])
            timestamps.append(item["timestamp"])

        new_data = {
            "healthscore": healthscores,
            "timestamp": timestamps
        }

        await MQTTClient.publish(f"H2_{self.unit}/Health", json.dumps(new_data))


class Logic:

    def __init__(self, unit:str) -> None:
        self.unit = unit
        self.iUnit = int(unit) - 1

    def H2Quality(self) -> Literal[0, 1]:
        if vrfData[self.iUnit]["c34"][-1] < 0.96 or vrfData[self.iUnit]["c35"][-1] < 0.96:
            return 1
        else:
            return 0

    def H2Leakage(self) -> Literal[0, 1]:
        if arfData[self.iUnit]["c31"][-1] > 1:
            return 1
        else:
            return 0

    def liquidLeakage(self) -> Literal[0, 1]:
        if (
            vrfData[self.iUnit]["c36"][-1] > 650
            or vrfData[self.iUnit]["c37"][-1] > 650
            or vrfData[self.iUnit]["c38"][-1] > 650
            or vrfData[self.iUnit]["c39"][-1] > 650
            or vrfData[self.iUnit]["c27"][-1] > 10
            or vrfData[self.iUnit]["c28"][-1] > 10
            or vrfData[self.iUnit]["c29"][-1] > 10
            or vrfData[self.iUnit]["c30"][-1] > 10
        ):
            return 1
        else:
            return 0


class Tasks:

    def __init__(self, unit: str) -> None:
        app = AppContainer()
        app.wire(modules=[__name__])

        self.unit = unit
        self.exec = futures.ThreadPoolExecutor()
        self.count = 0

        self.gd = GetData(unit)
        self.ad = AnomalyDetection(unit)
        self.fore = Forecast(unit)
        self.logic = Logic(unit)
        self.heal = Health(unit)
        self.identifier = str(uuid.uuid4())

    async def _get_data(self):
        tasks = []
        tasks.append(await asyncio.to_thread(self.gd.get_forecast_df))
        tasks.append(await asyncio.to_thread(self.gd.get_anomaly_df))
        tasks.append(await asyncio.to_thread(self.gd.get_anomaly_df_PEM))
        tasks.append(await asyncio.to_thread(self.gd.get_anomaly_df_PG))
        await asyncio.gather(*tasks)

    async def _get_fore(self, MQTTClient: aiomqtt.Client):
        tasks = []
        tasks.append(await asyncio.to_thread(self.fore.vr_forecast, MQTTClient))
        tasks.append(await asyncio.to_thread(self.fore.ar_forecast1, MQTTClient))
        await asyncio.gather(*tasks)

    async def _heal_logic(self, MQTTClient: aiomqtt.Client):
        fs = [
            self.exec.submit(self.ad.grubbs_t, ""),
            self.exec.submit(self.ad.grubbs_t, "PEM"),
            self.exec.submit(self.ad.grubbs_t, "PG"),
            self.exec.submit(self.ad.lscp_t, ""),
            self.exec.submit(self.ad.lscp_t, "PEM"),
            self.exec.submit(self.ad.lscp_t, "PG"),
            self.exec.submit(self.ad.spectral_residual_saliency, ""),
            self.exec.submit(self.ad.spectral_residual_saliency, "PEM"),
            self.exec.submit(self.ad.spectral_residual_saliency, "PG"),
            self.exec.submit(self.logic.H2Quality),
            self.exec.submit(self.logic.H2Leakage),
            self.exec.submit(self.logic.liquidLeakage),
            self.exec.submit(self.heal.getNow)
        ]
        futures.wait(fs)
        futureResults = [f.result() for f in fs[:-1]]

        await self.heal.health_score(MQTTClient, *futureResults[:9])

        data = {
            "H2Quality": futureResults[9],
            "H2Leakage": futureResults[10],
            "liquidLeakage": futureResults[11],
        }
        await MQTTClient.publish(f"H2_{self.unit}/Forecast/Logic", json.dumps(data))

    async def _task(self, MQTTClient: aiomqtt.Client):
        await self._get_data()
        await self._get_fore(MQTTClient)
        await self._heal_logic(MQTTClient)

    async def run(self):
        try:
            async with aiomqtt.Client(
                hostname=MQTT_IP,
                port=MQTT_PORT,
                username=MQTT_USERNAME,
                password=MQTT_PASSWORD,
                identifier=self.identifier,
            ) as MQTTClient:
                print("----build new MQTT connection----")

                while 1:
                    start = time()

                    await self._task(MQTTClient)

                    end = time()
                    elapsed_time = int((end - start) * 1000000)
                    self.count += 1
                    print(f"Loop {self.count} time used: {elapsed_time} microseconds")
                    sleep(max(INTERVAL - elapsed_time / 1000000, 0))
        except aiomqtt.MqttError:
            print(f"MQTT connection lost; Reconnecting in 5 seconds ...")
            await asyncio.sleep(5)
        except KeyboardInterrupt:
            print("KeyboardInterrupt received. Shutting down the executor...")


async def test(unit: str) -> None:
    count = 0
    identifier = str(uuid.uuid4())
    try:
        async with aiomqtt.Client(
            hostname=MQTT_IP,
            port=MQTT_PORT,
            username=MQTT_USERNAME,
            password=MQTT_PASSWORD,
            identifier=identifier,
        ) as MQTTClient:
            print("----build new MQTT connection----")
            gd = GetData(unit)
            ad = AnomalyDetection(unit)
            fore = Forecast(unit)
            heal = Health(unit)
            logic = Logic(unit)
            while 1:
                start = time()

                await gd.get_anomaly_df()
                await gd.get_anomaly_df_PEM()
                await gd.get_anomaly_df_PG()
                await gd.get_forecast_df()

                r = (
                    ad.grubbs_t(""),
                    ad.grubbs_t("PEM"),
                    ad.grubbs_t("PG"),
                    ad.lscp_t(""),
                    ad.lscp_t("PEM"),
                    ad.lscp_t("PG"),
                    ad.spectral_residual_saliency(""),
                    ad.spectral_residual_saliency("PEM"),
                    ad.spectral_residual_saliency("PG"),
                )

                await fore.vr_forecast(MQTTClient)
                await fore.ar_forecast1(MQTTClient)

                heal.getNow()
                await heal.health_score(MQTTClient, *r)
                print(logic.H2Quality(), logic.H2Leakage(), logic.liquidLeakage())

                end = time()
                elapsed_time = int((end - start) * 1000000)
                count += 1
                print(f"Loop {count} time used: {elapsed_time} microseconds")
                sleep(max(INTERVAL - elapsed_time / 1000000, 0))
    except aiomqtt.MqttError:
        print(f"MQTT connection lost; Reconnecting in 5 seconds ...")
        await asyncio.sleep(5)
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Shutting down the executor...")


if __name__ == "__main__":
    app = AppContainer()
    app.wire(modules=[__name__])

    asyncio.run(test('1'))
