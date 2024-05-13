from celery import Celery, group
import celeryconfig
import pandas as pd
import numpy as np
import taos
from statsmodels.tsa.api import VARMAX, adfuller, VAR
from statsmodels.tsa.vector_ar import vecm
from statsmodels.tsa.arima.model import ARIMA
import time
from datetime import timedelta, datetime
import orjson as json
import paramiko
from pythresh.thresholds.comb import COMB
import redis
from outliers import smirnov_grubbs as grubbs
from pyod.models import lof, cof, inne, ecod, lscp, sos
import sranodec as anom
import itertools
import warnings
from alert_count_sql import (
    generate_sql,
    cols_A,
    cols_PEM,
    cols_PG,
    alias_A,
    alias_PEM,
    alias_PG,
)
import paho.mqtt.client as mqtt
import requests

warnings.filterwarnings("ignore")

config_file = r"../DataAcquisition/config.json"
with open(config_file, "rb") as file:
    parsed_json = json.loads(file.read())

mqtt_addr = parsed_json["mqtt_addr"].split("//")[1].split(":")
mqtt_ip = mqtt_addr[0]
mqtt_port = int(mqtt_addr[1])
taos_ip = parsed_json["taos_ip"]
taos_port = int(parsed_json["taos_port"])
taos_username = parsed_json["taos_user"]
taos_password = parsed_json["taos_pw"]
taos_database = parsed_json["taos_db"]
redis_ip = parsed_json["redis_ip"]
redis_port = parsed_json["redis_port"]
redis_db = 0
step = 5
dbserver_ip = taos_ip
dbserver_username = "administrator"
token = parsed_json["grafana_token"]
url = parsed_json["grafana_addr"] + "/api/alertmanager/grafana/api/v2/alerts"

app = Celery()
app.config_from_object(celeryconfig)
df = pd.DataFrame()
df_PEM = pd.DataFrame()
df_PG = pd.DataFrame()
f_df = pd.DataFrame()


class RedisSingleton:
    _instance = None

    def __new__(cls, host=redis_ip, port=redis_port, db=redis_db):
        if cls._instance is None:
            print("----build new Redis connection----")
            cls._instance = object.__new__(cls)
            cls._instance.conn = redis.StrictRedis(host=host, port=port, db=db)
        return cls._instance.conn

    def __del__(self):
        self._instance.conn.close()


redis_conn = RedisSingleton()


class TaosConnSingleton:
    _instance = None

    def __new__(
        cls,
        host=taos_ip,
        port=taos_port,
        user=taos_username,
        password=taos_password,
        database=taos_database,
    ):
        if cls._instance is None:
            print("----build new Taos connection----")
            cls._instance = object.__new__(cls)
            cls._instance.conn = taos.connect(
                host=host, port=port, user=user, password=password, database=database
            )
        return cls._instance.conn

    def __del__(self):
        self._instance.conn.close()


taos_conn = TaosConnSingleton()
taos_conn.execute(
    "create table if not exists alert (ts TIMESTAMP, A INT, PEM INT, PG INT)"
)


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
    else:
        print("Failed to connect to MQTT broker")


def on_publish(client, userdata, mid):
    pass


def on_disconnect(client, userdata, rc):
    if rc != 0:
        try:
            print("Connection lost. Reconnecting...")
            client.reconnect()
        except:
            print("Failed to reconnect to MQTT broker")
            time.sleep(5)


class MQTTClientSingleton:
    _instance = None

    def __new__(cls, host=mqtt_ip, port=mqtt_port, user="", password=""):
        print("----build new MQTT connection----")
        if cls._instance is None:
            cls._instance = mqtt.Client()
            cls._instance.on_connect = on_connect
            cls._instance.on_publish = on_publish
            cls._instance.on_disconnect = on_disconnect
            if user and password:
                cls._instance.username_pw_set(user, password)
            cls._instance.connect(host, port)
        return cls._instance

    def __del__(self):
        self._instance.disconnect()


MQTTClient = MQTTClientSingleton()


def clean():
    redis_conn.close()
    taos_conn.close()
    MQTTClient.disconnect()


def sigint_handler(sig, frame):
    clean()
    print("Exit by KeyboardInterrupt")


def sigterm_handler(sig, frame):
    clean()
    print("Terminated by Kill")


def calculate_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"函数 {func.__name__} 的执行时间为: {execution_time} 秒")
        return result

    return wrapper


@app.task
def alertStat():
    def foo(cols, alias):
        sql = generate_sql(cols, alias)
        result = taos_conn.query(sql)
        return result.fetch_all()[0][0]

    dataA = foo(cols_A, alias_A)
    dataPEM = foo(cols_PEM, alias_PEM)
    dataPG = foo(cols_PG, alias_PG)
    sql = f"insert into alert values (now, {dataA}, {dataPEM}, {dataPG})"
    affected_rows = taos_conn.execute(sql)
    print("affected_rows", affected_rows)
    # select sum(a) from alert where ts > 1 - 1n;


@app.task
def deleteTask():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    private_key_path = "/root/.ssh/id_rsa"
    private_key = paramiko.RSAKey.from_private_key_file(private_key_path)
    client.connect(dbserver_ip, username=dbserver_username, pkey=private_key)

    stdin, stdout, stderr = client.exec_command(
        "du -s /var/lib/taos | awk '{print $1}'"
    )
    occupy_kb = float(stdout.read().decode())
    print(f"{occupy_kb=}")

    stdin, stdout, stderr = client.exec_command(
        """df -h | awk '$NF=="/"{printf "%d", $5}'"""
    )
    disk_per = int(stdout.read().decode())
    print(f"{disk_per=}")

    if disk_per > 90 and occupy_kb > 1e9:
        t = taos_conn.query(
            f"SELECT ts from {taos_database}.s_bool limit 1"
        ).fetch_all()[0][0]
        taos_conn.execute(
            f"DELETE from {taos_database}.s_bool where ts < '{t + timedelta(weeks=1)}'"
        )
        t = taos_conn.query(
            f"SELECT ts from {taos_database}.s_analog limit 1"
        ).fetch_all()[0][0]
        taos_conn.execute(
            f"DELETE from {taos_database}.s_analog where ts < '{t + timedelta(weeks=1)}'"
        )


@app.task
def forecast_df():
    global f_df
    cols = "c0,c5,c6,c2,c34,c35,c36,c37,c38,c39,c27,c28,c29,c30,c31,c32,c33,c138,c140,c187,c201"
    sql = f"select {cols} from s_analog limit 100"
    result = taos_conn.query(sql)
    data = result.fetch_all_into_dict()
    f_df = pd.DataFrame(data)
    f_df += np.random.uniform(0, 0.001, f_df.shape)
    print("f_df: ", f_df.shape)


@app.task
def anomaly_df():
    global df
    cols = ",".join([f"c{i}" for i in range(0, 40)])
    sql = f"select {cols} from s_analog limit 100"
    result = taos_conn.query(sql)  # windows不支持异步查询，适合一行一行立即处理的场景
    data = result.fetch_all_into_dict()
    df = pd.DataFrame(data)
    df += np.random.uniform(0, 0.001, df.shape)  # 以免除数为0和全常数线性相关
    print("df: ", df.shape)


@app.task
def anomaly_df_PEM():
    global df_PEM
    cols = ",".join(
        [
            f"c{i}"
            for i in itertools.chain(range(138, 144), range(182, 202), range(203, 218))
        ]
    )
    sql = f"select {cols} from s_analog limit 100"
    result = taos_conn.query(sql)  # windows不支持异步查询，适合一行一行立即处理的场景
    data = result.fetch_all_into_dict()
    df_PEM = pd.DataFrame(data)
    df_PEM += np.random.uniform(0, 0.001, df_PEM.shape)  # 以免除数为0和全常数线性相关
    print("df_PEM: ", df_PEM.shape)


@app.task
def anomaly_df_PG():
    global df_PG
    cols = ",".join([f"c{i}" for i in range(144, 154)])
    sql = f"select {cols} from s_analog limit 100"
    result = taos_conn.query(sql)  # windows不支持异步查询，适合一行一行立即处理的场景
    data = result.fetch_all_into_dict()
    df_PG = pd.DataFrame(data)
    df_PG += np.random.uniform(0, 0.001, df_PG.shape)  # 以免除数为0和全常数线性相关
    print("df_PG: ", df_PG.shape)


@app.task
# @calculate_execution_time
def grubbs_t():
    x = 0
    for i in range(df.shape[1]):
        x += len(grubbs.two_sided_test_indices(df.iloc[:, i].values, alpha=0.05)) > 0

    result = x / df.shape[1]
    redis_conn.hset("anomaly", "grubbs", result)
    return result


@app.task
# @calculate_execution_time
def grubbs_t_PEM():
    x = 0
    for i in range(df_PEM.shape[1]):
        x += (
            len(grubbs.two_sided_test_indices(df_PEM.iloc[:, i].values, alpha=0.05)) > 0
        )

    result = x / df_PEM.shape[1]
    redis_conn.hset("anomaly", "grubbs_PEM", result)
    return result


@app.task
# @calculate_execution_time
def grubbs_t_PG():
    x = 0
    for i in range(df_PG.shape[1]):
        x += len(grubbs.two_sided_test_indices(df_PG.iloc[:, i].values, alpha=0.05)) > 0

    result = x / df_PG.shape[1]
    redis_conn.hset("anomaly", "grubbs_PG", result)
    return result


@app.task
# @calculate_execution_time
def sos_t():
    clf = sos.SOS(contamination=0.1)
    result = clf.fit_predict(df)

    return np.sum(result) / df.shape[1]


@app.task
# @calculate_execution_time
def lscp_t():
    if df.shape[0] < 45:
        print("Not enough data for lscp")
        return 0

    detector_list = [
        lof.LOF(),
        cof.COF(),
        inne.INNE(),
        # ocsvm.OCSVM(),
        ecod.ECOD(),
    ]
    # clf = lscp.LSCP(contamination=COMB(), detector_list=detector_list)
    # result = clf.fit_predict(df)
    clf = lscp.LSCP(
        contamination=COMB(), detector_list=detector_list, n_bins=len(detector_list)
    )
    clf.fit(df)
    result = clf.predict(df)

    final = np.sum(result == 1) / df.shape[1]
    redis_conn.hset("anomaly", "lscp", final)
    return final


@app.task
# @calculate_execution_time
def lscp_t_PEM():
    if df_PEM.shape[0] < 45:
        print("Not enough data for lscp")
        return 0

    detector_list = [
        lof.LOF(),
        cof.COF(),
        inne.INNE(),
        # ocsvm.OCSVM(),
        ecod.ECOD(),
    ]
    clf = lscp.LSCP(contamination=COMB(), detector_list=detector_list)
    result = clf.fit_predict(df_PEM)

    final = np.sum(result == 1) / df_PEM.shape[1]
    redis_conn.hset("anomaly", "lscp_PEM", final)
    return final


@app.task
# @calculate_execution_time
def lscp_t_PG():
    if df_PG.shape[0] < 45:
        print("Not enough data for lscp")
        return 0

    detector_list = [
        lof.LOF(),
        cof.COF(),
        inne.INNE(),
        # ocsvm.OCSVM(),
        ecod.ECOD(),
    ]
    clf = lscp.LSCP(contamination=COMB(), detector_list=detector_list)
    result = clf.fit_predict(df_PG)

    final = np.sum(result == 1) / df_PG.shape[1]
    redis_conn.hset("anomaly", "lscp_PG", final)
    return final


@app.task
# @calculate_execution_time
def spectral_residual_saliency():
    if df.shape[0] < 24:
        print("Not enough data for srs")
        return 0
    score_window_size = min(df.shape[0], 100)
    spec = anom.Silency(
        amp_window_size=24, series_window_size=24, score_window_size=score_window_size
    )

    abnormal = 0
    for i in range(df.shape[1]):
        score = spec.generate_anomaly_score(df.values[:, i])
        abnormal += np.sum(score > np.percentile(score, 99)) > 0

    result = abnormal / df.shape[1]
    redis_conn.hset("anomaly", "SRA", result)
    return result


@app.task
# @calculate_execution_time
def spectral_residual_saliency_PEM():
    if df_PEM.shape[0] < 24:
        print("Not enough data for srs")
        return 0
    score_window_size = min(df_PEM.shape[0], 100)
    spec = anom.Silency(
        amp_window_size=24, series_window_size=24, score_window_size=score_window_size
    )

    abnormal = 0
    for i in range(df_PEM.shape[1]):
        score = spec.generate_anomaly_score(df_PEM.values[:, i])
        abnormal += np.sum(score > np.percentile(score, 99)) > 0

    result = abnormal / df_PEM.shape[1]
    redis_conn.hset("anomaly", "SRA_PEM", result)
    return result


@app.task
# @calculate_execution_time
def spectral_residual_saliency_PG():
    if df_PG.shape[0] < 24:
        print("Not enough data for srs")
        return 0
    score_window_size = min(df_PG.shape[0], 100)
    spec = anom.Silency(
        amp_window_size=24, series_window_size=24, score_window_size=score_window_size
    )

    abnormal = 0
    for i in range(df_PG.shape[1]):
        score = spec.generate_anomaly_score(df_PG.values[:, i])
        abnormal += np.sum(score > np.percentile(score, 99)) > 0

    result = abnormal / df_PG.shape[1]
    redis_conn.hset("anomaly", "SRA_PG", result)
    return result


def VECM_forecast(data):
    # Johansen 检验方法确定协整关系阶数
    rank = vecm.select_coint_rank(data, det_order=0, k_ar_diff=1, signif=0.1)
    model = vecm.VECM(data, k_ar_diff=1, coint_rank=rank.rank)
    yhat = model.fit().predict(steps=step)
    print(yhat)
    return yhat


def VAR_forecast(data):
    model = VAR(data, exog=None)
    # 数据量太小maxlags选择10会报错，数据量不变维度多也会报错
    res = model.select_order(maxlags=5)
    best_lag = res.selected_orders["aic"]
    print(f"{best_lag=}")
    yhat = model.fit(maxlags=max(best_lag, 10)).forecast(data.values, steps=step)
    print(yhat)
    return yhat


@app.task
def vr_forecast():
    if f_df.shape[0] < 40:
        print("Not enough data for forecast")
        return

    targetList = [
        ["c5", "c6"],
        ["c2"],
        ["c34", "c35"],
        ["c36", "c37", "c38", "c39", "c27", "c28", "c29", "c30"],
    ]
    # exogList = [["密封油含水量", "真空油箱真空度"], ["制氢流量计", 发电机内温度", "机组功率", "温度调节阀状态", "氢冷器进出水温度"], ["真空油箱真空度"], ["水箱液位", "油氢差压"]]
    exogList = [[], ["c201"], [], []]

    targetListLen = len(targetList)
    data = {}
    for i in range(targetListLen):
        vrdf = f_df[targetList[i] + exogList[i]]

        # 随便取一列平稳性测试
        try:
            p = adfuller(vrdf.iloc[:, 0], regression="ct")[1]  # ct: 常数项和趋势项
        except:  # 全常数
            p = 0
        print(f"{p=}")

        targetLen = len(targetList[i])
        try:
            if p <= 0.05:  # 平稳的
                yhat = VAR_forecast(vrdf)
                # yhat = json.dumps(VARMAX_forecast().values.tolist())
            else:
                yhat = VECM_forecast(vrdf)
        except Exception as e:
            print("Exception: ", e)
        else:
            yhat = pd.DataFrame(yhat, columns=vrdf.columns)
            result = pd.concat([vrdf.iloc[-25:, :], yhat], axis=0, ignore_index=True)
            data.update(
                {col: list(result[col].round(3)) for col in result.columns[:targetLen]}
            )

    print(data.keys())
    redis_conn.hset("forecast", "vr", json.dumps(data))


@app.task
# @calculate_execution_time
def ar_forecast1():
    if f_df.shape[0] < 40:
        print("Not enough data for forecast")
        return

    order = (10, 1, 3)
    yhat = []
    ardf = f_df[["c0", "c31", "c32", "c33", "c138", "c140", "c187"]]

    for i in range(ardf.shape[1]):
        model = ARIMA(ardf.iloc[:, i], order=order)
        forecast = model.fit().forecast(steps=step)
        yhat.append(forecast)
    yhatdf = pd.concat(yhat, axis=1)
    yhatdf.columns = ardf.columns
    result = pd.concat([ardf.iloc[-25:, :], yhatdf], axis=0, ignore_index=True)
    data = {col: list(result[col].round(3)) for col in result.columns}
    print(data.keys())
    redis_conn.hset("forecast", "ar1", json.dumps(data))


@app.task
# @calculate_execution_time
def ar_forecast2():
    if f_df.shape[0] < 40:
        print("Not enough data for forecast")
        return

    order = (10, 1, 3)
    yhat = []
    ardf = f_df[["c0", "c31", "c32", "c33", "c138", "c140", "c187"]]

    for i in range(ardf.shape[1]):
        model = ARIMA(ardf.iloc[:, i], order=order)
        forecast = model.fit().forecast(steps=step)
        yhat.append(forecast)
    yhatdf = pd.concat(yhat, axis=1)
    yhatdf.columns = ardf.columns
    result = pd.concat([ardf.iloc[-25:, :], yhatdf], axis=0, ignore_index=True)
    data = {col: list(result[col].round(3)) for col in result.columns}
    print(data.keys())
    redis_conn.hset("forecast", "ar2", json.dumps(data))


def queryAlert():
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    response = requests.get(url, headers=headers, verify=False)

    data = response.json()

    alerts_list = []

    for alert in data:
        alert_dict = {
            "name": alert["labels"]["alertname"],
            "diagnosis": alert["annotations"].get("原因分析", ""),
            "advice": alert["annotations"].get("处理建议", ""),
            "datetime": datetime.strptime(
                alert["startsAt"], "%Y-%m-%dT%H:%M:%S.%fZ"
            ).strftime("%Y-%m-%d %H:%M:%S"),
        }
        alerts_list.append(alert_dict)
    return alerts_list


def healthScore():
    r = redis_conn.hgetall("anomaly")
    # 主40 提纯30 制氢30 lscp70% grubbs15% sra15%
    lscp = float(r[b"lscp"])
    lscp_PG = float(r[b"lscp_PG"])
    lscp_PEM = float(r[b"lscp_PEM"])
    grubbs = float(r[b"grubbs"])
    grubbs_PG = float(r[b"grubbs_PG"])
    grubbs_PEM = float(r[b"grubbs_PEM"])
    SRA = float(r[b"SRA"])
    SRA_PG = float(r[b"SRA_PG"])
    SRA_PEM = float(r[b"SRA_PEM"])

    score = 100
    if lscp > 3:
        score -= 28 * (lscp - 3) / 97
    if lscp_PG > 3:
        score -= 21 * (lscp_PG - 3) / 97
    if lscp_PEM > 3:
        score -= 21 * (lscp_PEM - 3) / 97
    if grubbs > 3:
        score -= 6 * (grubbs - 3) / 97
    if grubbs_PG > 3:
        score -= 4.5 * (grubbs_PG - 3) / 97
    if grubbs_PEM > 3:
        score -= 4.5 * (grubbs_PEM - 3) / 97
    if SRA > 3:
        score -= 6 * (SRA - 3) / 97
    if SRA_PG > 3:
        score -= 4.5 * (SRA_PG - 3) / 97
    if SRA_PEM > 3:
        score -= 4.5 * (SRA_PEM - 3) / 97
    return round(score, 3)


def H2Quality():
    r1 = json.loads(redis_conn.hget("forecast", "vr"))
    r2 = taos_conn.query("select c34, c35 from s_analog limit 1").fetch_all()[0]
    if r1["c34"][-1] < 0.96 or r1["c35"][-1] < 0.96 or r2[0] < 0.96 or r2[1] < 0.96:
        return 1
    else:
        return 0


def H2Leakage():
    r1 = json.loads(redis_conn.hget("forecast", "ar1"))
    r2 = taos_conn.query("select c158, c173 from s_bool limit 1").fetch_all()[0]
    if r1["c31"][-1] > 1 or r2[0] == True or r2[1] == True:
        return 1
    else:
        return 0


def liquidLeakage():
    r1 = json.loads(redis_conn.hget("forecast", "vr"))
    r2 = taos_conn.query(
        "select c36, c37, c38, c39, c27, c28, c29, c30 from s_analog limit 1"
    ).fetch_all()[0]
    if (
        r1["c36"][-1] > 650
        or r2[0] > 650
        or r1["c37"][-1] > 650
        or r2[1] > 650
        or r1["c38"][-1] > 650
        or r2[2] > 650
        or r1["c39"][-1] > 650
        or r2[3] > 650
        or r1["c27"][-1] > 10
        or r2[4] > 10
        or r1["c28"][-1] > 10
        or r2[5] > 10
        or r1["c29"][-1] > 10
        or r2[6] > 10
        or r1["c30"][-1] > 10
        or r2[7] > 10
    ):
        return 1
    else:
        return 0


@app.task
def homeInfo():
    # print(taos_conn.query("select c36, c37, c38, c39, c27, c28, c29, c30 from s_analog limit 1").fetch_all()[0])
    # print(json.loads(redis_conn.hget("forecast", "vr"))["c29"][-1])
    data = {
        "animation": {
            "fillCO2": "1",
            "exhaustH2": "1",
            "fillH2fromPowerPlant": "1",
            "fillH2fromConfluence": "1",
            "operationNormally": "1",
            "purificationH2": "1",
            "makeH2": "1",
        },
        "Status": {
            "unitStatus": "1",
            "sysStatus ": int(
                taos_conn.query("select c97 from s_analog limit 1").fetch_all()[0][0]
            ),
            "unitLoad": "960.18MW",
        },
        "healthLevel": {
            "percentage": healthScore(),
            "H2Quality": H2Quality(),
            "H2Leakage": H2Leakage(),
            "liquidLeakage": liquidLeakage(),
            "PEMpower": "制氢功率c138",
            "PEMSys": taos_conn.query(
                "select cast(c159 as int) from s_bool limit 1"
            ).fetch_all()[0][0],
            "PGSys": taos_conn.query(
                "select cast(c160 as int) from s_bool limit 1"
            ).fetch_all()[0][0],
        },
        "operationData": {
            "pressure": round(
                taos_conn.query("select c1 from s_analog limit 1").fetch_all()[0][0], 3
            ),
            "purity": round(
                taos_conn.query("select c34 from s_analog limit 1").fetch_all()[0][0], 3
            ),
            "dew": round(
                taos_conn.query("select c5 from s_analog limit 1").fetch_all()[0][0], 3
            ),
            "makeFlow": round(
                taos_conn.query("select c201 from s_analog limit 1").fetch_all()[0][0],
                3,
            ),
        },
        "pressure": [
            round(x[0], 3)
            for x in taos_conn.query(
                "select avg(c1) from s_analog interval(1h) limit 7"
            ).fetch_all()
        ],
        "purity": [
            round(x[0], 3)
            for x in taos_conn.query(
                "select avg(c34) from s_analog interval(1h) limit 7"
            ).fetch_all()
        ],
        "dew": [
            round(x[0], 3)
            for x in taos_conn.query(
                "select avg(c5) from s_analog interval(1h) limit 7"
            ).fetch_all()
        ],
        "makeFlow": [
            round(x[0], 3)
            for x in taos_conn.query(
                "select avg(c201) from s_analog interval(1h) limit 7"
            ).fetch_all()
        ],
        "cost": {
            "energy": "85.16",
            "water": "2.84"
        },
        "economy": {
            "supplement": {
                "mon": "22.06",
                "tue": "23.18",
                "wed": "22.35",
                "thu": "22.98",
                "fri": "23.01",
                "sat": "22.19",
                "sun": "22.23",
            },
            "purification": {
                "mon": "49.36",
                "tue": "55.19",
                "wed": "55.23",
                "thu": "54.42",
                "fri": "55.41",
                "sat": "48.27",
                "sun": "49.36",
            },
        },
        "alarm": queryAlert(),
    }

    # 将数据转换为JSON格式
    json_data = json.dumps(data)
    MQTTClient.publish("homeInfo", json_data)


@app.task
def chainTask():
    task_group1 = group(
        anomaly_df.s(), anomaly_df_PEM.s(), anomaly_df_PG.s(), forecast_df.s()
    )
    task_group1.apply_async()

    task_group2 = group(
        grubbs_t.s(),
        lscp_t.s(),
        spectral_residual_saliency.s(),
        grubbs_t_PEM.s(),
        lscp_t_PEM.s(),
        spectral_residual_saliency_PEM.s(),
        grubbs_t_PG.s(),
        lscp_t_PG.s(),
        spectral_residual_saliency_PG.s(),
    )
    result = task_group2.apply_async()

    task_group3 = group(
        vr_forecast.s(),
        ar_forecast1.s(),
        # ar_forecast2.s()
    )
    task_group3.apply_async()

    print(result.get())


if __name__ == "__main__":
    start_time = time.time()
    # anomaly_df()
    forecast_df()
    # grubbs_r = grubbs_test()
    # lscp_r = lscp()
    # sr_r = spectral_residual_saliency()
    # print(grubbs_r, lscp_r, sr_r)
    vr_forecast()
    # ar_forecast1()
    # ar_forecast2()
    # alertStat()
    homeInfo()
    print(f"used time: {time.time() - start_time}")
