from datetime import timedelta


broker_url = 'redis://127.0.0.1:6379/0'
result_backend = 'redis://127.0.0.1:6379/0'
timezone = 'Asia/Shanghai'
broker_connection_retry_on_startup = True

# 序列化器设置（使用JSON格式）
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']

# task_serializer = 'pickle'
# result_serializer = 'pickle'
# accept_content = ['pickle']

# 队列设置（队列名为default）
task_default_queue = 'default'
task_queues = {
    'default': {
        'exchange': 'default',
        'routing_key': 'default',
    }
}

# 并发 worker 数量 命令里如果设置，以命令为准
worker_concurrency = 4

# tasks 超时时间
task_time_limit = 60 * 30

result_expires = 30 # seconds or timedelta(seconds=30)

beat_schedule = {
    'task1': {
        'task': 'tasks.deleteTask',
        'schedule': timedelta(days=1),
    },
    'task2': {
        'task': 'tasks.chainTask',
        'schedule': timedelta(seconds=10),
        # 'args': (1, 1)
    },
    'task3': {
        'task': 'tasks.alertStat',
        'schedule': timedelta(hours=1),
    },
    'task4': {
        'task': 'tasks.homeInfo',
        'schedule': timedelta(seconds=11),
    },
}
