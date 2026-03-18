"""
Фоновые задачи — авто-сбор монет
"""
import threading
import time
from datetime import datetime

_thread: threading.Thread = None
_stop:   threading.Event  = None


def start(api):
    global _thread, _stop
    if _thread and _thread.is_alive():
        return False
    _stop   = threading.Event()
    _thread = threading.Thread(target=_worker, args=(api,), daemon=True)
    _thread.start()
    api.cfg["auto_revenue"] = True
    api.add_log("Авто-сбор запущен", "ok")
    return True


def stop(api):
    global _thread, _stop
    if _stop: _stop.set()
    if _thread: _thread.join(timeout=3); _thread = None
    api.cfg["auto_revenue"] = False
    api.add_log("Авто-сбор остановлен", "info")


def running() -> bool:
    return _thread is not None and _thread.is_alive()


def _worker(api):
    while not _stop.is_set():
        try:
            data = api.api_revenue()
            if data and isinstance(data, dict):
                added = data.get("add", data.get("coins_earned", 0)) or 0
                if added > 0:
                    api.stats["revenue_collected"] += int(added)
                    api.add_log(f"Собрано {api.fmt(int(added))} монет", "ok")
        except Exception as e:
            api.add_log(f"Авто-сбор ошибка: {e}", "err")

        if api.cfg.get("auto_bonus"):
            try:
                b = api.api_daily_bonus()
                if b and isinstance(b, dict):
                    msg = b.get("message", "")
                    if "уже" not in msg.lower() and "already" not in msg.lower():
                        api.stats["bonus_collected"] += 1
                        api.add_log("Ежедневный бонус получен", "ok")
            except Exception:
                pass

        interval = api.cfg.get("revenue_interval", 300)
        for _ in range(interval):
            if _stop.is_set(): break
            time.sleep(1)
