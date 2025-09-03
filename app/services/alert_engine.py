# app/services/alert_engine.py
from typing import Any, Callable, Dict, List, Tuple

# Supported operators (lowercased)
_OP_MAP: Dict[str, Callable[[float, float], bool]] = {
    "gteq": lambda x, y: x >= y,
    "lteq": lambda x, y: x <= y,
    "eq":   lambda x, y: x == y,
    "ne":   lambda x, y: x != y,
    "gt":   lambda x, y: x > y,
    "lt":   lambda x, y: x < y,
}

class _Condition:
    def __init__(self, vital: str, op: str, value: float, name: str = ""):
        self.vital = vital                     # exact key e.g. "CO2"
        self.op = (op or "").strip().lower()  # tolerate case/whitespace
        self.value = float(value)
        self.name = name or f"{vital}_{op}_{value}"
        if self.op not in _OP_MAP:
            import logging
            logging.warning(f"[ALERT-ENGINE] Unknown operator '{op}' on vital '{vital}'")

    def check(self, record: Dict[str, Any]) -> bool:
        v = record.get(self.vital)
        if v is None:
            return False
        try:
            return _OP_MAP[self.op](float(v), self.value)
        except Exception:
            return False

class Alarm:
    def __init__(self, cfg: Dict[str, Any]):
        # Prefer explicit id if present; otherwise fall back to alarmname
        self.alarm_id = cfg.get("alarmId") or cfg.get("id") or cfg.get("alarmname") or "alarm"
        self.name = cfg.get("alarmname", self.alarm_id)
        self.conditions = [
            _Condition(
                c["conditionVital"],
                c["conditionOp"],
                c["conditionValue"],
                c.get("conditionName", "")
            )
            for c in cfg.get("conditions", [])
        ]

class AlertEngine:
    """
    Minimal engine: evaluates ALL conditions of each alarm on a single record.
    Fires when all are True. No windows, no debounce.
    """
    def __init__(self, alarms_cfg: List[Dict[str, Any]]):
        self.alarms: List[Alarm] = [Alarm(a) for a in alarms_cfg]

    def evaluate_point(self, patient_id: str, ts_ms: int, record: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
        fired: List[Tuple[str, Dict[str, Any]]] = []
        for alarm in self.alarms:
            if all(cond.check(record) for cond in alarm.conditions):
                fired.append((
                    alarm.alarm_id,
                    {
                        "alarmname": alarm.name,
                        "alarmId": alarm.alarm_id,
                        "timestamp": ts_ms,
                        "record": record,  # raw record as received
                    }
                ))
                print(fired)
        return fired
