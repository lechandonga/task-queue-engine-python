"""SQLite 持久化层：任务、去重键、消费位点。

一致性保证：
- 单连接 + 互斥锁，WAL 模式；所有多步写入在同一事务内完成。
- 任务插入与去重键登记是原子的：重复去重键（未过期）返回已有任务，
  不产生第二条记录。
- 位点提交是原子的：只允许单调前进，重复提交或回退抛 ConflictError。
- 崩溃恢复：启动时把 RUNNING 重置为 PENDING，边界清晰。
"""

from __future__ import annotations

import json
import sqlite3
import threading

from . import payload as payload_codec
from .errors import ConflictError, NotFoundError
from .models import Task, TaskStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id     TEXT PRIMARY KEY,
    namespace   TEXT NOT NULL,
    payload     BLOB NOT NULL,
    dedup_key   TEXT,
    status      TEXT NOT NULL,
    attempts    INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    next_run_at REAL NOT NULL DEFAULT 0,
    error       TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_due
    ON tasks(status, next_run_at);
CREATE TABLE IF NOT EXISTS dedup (
    key        TEXT PRIMARY KEY,
    task_id    TEXT NOT NULL,
    expires_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    topic   TEXT NOT NULL,
    offset  INTEGER NOT NULL,
    payload BLOB NOT NULL,
    ts      REAL NOT NULL,
    PRIMARY KEY (topic, offset)
);
CREATE TABLE IF NOT EXISTS leases (
    grp        TEXT NOT NULL,
    topic      TEXT NOT NULL,
    owner      TEXT NOT NULL,
    expires_at REAL NOT NULL,
    PRIMARY KEY (grp, topic)
);
CREATE TABLE IF NOT EXISTS event_claims (
    grp        TEXT NOT NULL,
    topic      TEXT NOT NULL,
    offset     INTEGER NOT NULL,
    owner      TEXT NOT NULL,
    expires_at REAL NOT NULL,
    state      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (grp, topic, offset)
);
CREATE INDEX IF NOT EXISTS idx_event_claims_expiry
    ON event_claims(grp, topic, expires_at);
CREATE TABLE IF NOT EXISTS offsets (
    grp        TEXT NOT NULL,
    topic      TEXT NOT NULL,
    offset     INTEGER NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (grp, topic)
);
"""

_TERMINAL = (TaskStatus.SUCCEEDED, TaskStatus.DEAD, TaskStatus.CANCELLED)


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.RLock()  # 可重入：insert_task 内会调用 get_task
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ---------- 任务提交与去重 ----------

    def insert_task(self, task: Task, max_retries: int, now: float,
                    dedup_ttl: float) -> tuple[Task, bool]:
        """原子插入任务 + 去重键。

        返回 (task, created)。created=False 表示命中未过期去重键，
        返回的是已有任务，不会产生重复执行。
        """
        blob = payload_codec.encode(task.payload)
        with self._lock, self._conn:
            if task.dedup_key is not None:
                row = self._conn.execute(
                    "SELECT task_id, expires_at FROM dedup WHERE key=?",
                    (task.dedup_key,)).fetchone()
                if row is not None:
                    existing_id, expires_at = row
                    if expires_at > now:
                        existing = self.get_task(existing_id)
                        if existing is not None:
                            return existing, False
                    # 已过期或指向的任务不存在：覆盖旧键，允许重新提交
                    self._conn.execute("DELETE FROM dedup WHERE key=?",
                                       (task.dedup_key,))
            self._conn.execute(
                "INSERT INTO tasks(task_id, namespace, payload, dedup_key,"
                " status, attempts, max_retries, next_run_at, error,"
                " created_at, updated_at)"
                " VALUES(?,?,?,?,?,0,?,0,NULL,?,?)",
                (task.task_id, task.namespace, blob, task.dedup_key,
                 TaskStatus.PENDING.value, max_retries, now, now))
            if task.dedup_key is not None:
                self._conn.execute(
                    "INSERT INTO dedup(key, task_id, expires_at) VALUES(?,?,?)",
                    (task.dedup_key, task.task_id, now + dedup_ttl))
        return self.get_task(task.task_id), True

    def get_task(self, task_id: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT task_id, namespace, payload, dedup_key, status,"
                " attempts, created_at, updated_at FROM tasks WHERE task_id=?",
                (task_id,)).fetchone()
        return self._row_to_task(row) if row else None

    def find_dedup(self, dedup_key: str, now: float) -> Task | None:
        """按未过期去重键查已有任务；过期或不存在返回 None。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT task_id FROM dedup WHERE key=? AND expires_at>?",
                (dedup_key, now)).fetchone()
            if row is None:
                return None
            return self.get_task(row[0])

    # ---------- 执行生命周期 ----------

    def claim_due(self, now: float, limit: int) -> list[Task]:
        """原子认领到期任务：PENDING/FAILED_RETRYABLE -> RUNNING。

        单事务内 SELECT+UPDATE，多线程/多进程不会出现重复认领。
        """
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            rows = self._conn.execute(
                "SELECT task_id FROM tasks"
                " WHERE status IN (?, ?) AND next_run_at <= ?"
                " ORDER BY next_run_at, created_at LIMIT ?",
                (TaskStatus.PENDING.value,
                 TaskStatus.FAILED_RETRYABLE.value, now, limit)).fetchall()
            ids = [r[0] for r in rows]
            for tid in ids:
                self._conn.execute(
                    "UPDATE tasks SET status=?, attempts=attempts+1,"
                    " updated_at=? WHERE task_id=?",
                    (TaskStatus.RUNNING.value, now, tid))
            if not ids:
                return []
            q = ",".join("?" * len(ids))
            rows = self._conn.execute(
                f"SELECT task_id, namespace, payload, dedup_key, status,"
                f" attempts, created_at, updated_at FROM tasks"
                f" WHERE task_id IN ({q})", ids).fetchall()
            return [self._row_to_task(r) for r in rows]

    def mark_succeeded(self, task_id: str, now: float) -> None:
        """仅允许 RUNNING -> SUCCEEDED，防止重复生效。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE tasks SET status=?, error=NULL, updated_at=?"
                " WHERE task_id=? AND status=?",
                (TaskStatus.SUCCEEDED.value, now, task_id,
                 TaskStatus.RUNNING.value))
            if cur.rowcount == 0:
                raise ConflictError(
                    f"task {task_id} is not RUNNING; cannot mark succeeded")

    def mark_failed(self, task_id: str, error: str, retryable: bool,
                    next_run_at: float, now: float) -> TaskStatus:
        """RUNNING -> FAILED_RETRYABLE 或 DEAD（超限/不可重试）。原子判定。"""
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT attempts, max_retries, status FROM tasks"
                " WHERE task_id=?", (task_id,)).fetchone()
            if row is None:
                raise NotFoundError(f"task {task_id} not found")
            attempts, max_retries, status = row
            if status != TaskStatus.RUNNING.value:
                raise ConflictError(
                    f"task {task_id} is {status}; cannot mark failed")
            if retryable and attempts <= max_retries:
                new_status = TaskStatus.FAILED_RETRYABLE
                self._conn.execute(
                    "UPDATE tasks SET status=?, error=?, next_run_at=?,"
                    " updated_at=? WHERE task_id=?",
                    (new_status.value, error, next_run_at, now, task_id))
            else:
                new_status = TaskStatus.DEAD
                self._conn.execute(
                    "UPDATE tasks SET status=?, error=?, updated_at=?"
                    " WHERE task_id=?",
                    (new_status.value, error, now, task_id))
            return new_status

    def cancel(self, task_id: str, now: float) -> None:
        """仅允许从非终态取消；已终态任务明确拒绝。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE tasks SET status=?, updated_at=? WHERE task_id=?"
                " AND status IN (?, ?, ?)",
                (TaskStatus.CANCELLED.value, now, task_id,
                 TaskStatus.PENDING.value, TaskStatus.FAILED_RETRYABLE.value,
                 TaskStatus.RUNNING.value))
            if cur.rowcount == 0:
                row = self._conn.execute(
                    "SELECT status FROM tasks WHERE task_id=?",
                    (task_id,)).fetchone()
                if row is None:
                    raise NotFoundError(f"task {task_id} not found")
                raise ConflictError(
                    f"task {task_id} is already {row[0]}; cannot cancel")

    def recover(self, now: float) -> int:
        """重启恢复：RUNNING -> PENDING，返回重置数量。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE tasks SET status=?, next_run_at=0, updated_at=?"
                " WHERE status=?",
                (TaskStatus.PENDING.value, now, TaskStatus.RUNNING.value))
            return cur.rowcount

    # ---------- 死信检索 ----------

    def list_dead(self, namespace: str | None = None) -> list[Task]:
        sql = ("SELECT task_id, namespace, payload, dedup_key, status,"
               " attempts, created_at, updated_at FROM tasks"
               " WHERE status=?")
        args: list = [TaskStatus.DEAD.value]
        if namespace is not None:
            sql += " AND namespace=?"
            args.append(namespace)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [self._row_to_task(r) for r in rows]

    def pending_count(self, namespace: str | None = None) -> int:
        sql = ("SELECT COUNT(*) FROM tasks WHERE status IN (?, ?, ?)")
        args: list = [TaskStatus.PENDING.value,
                      TaskStatus.FAILED_RETRYABLE.value,
                      TaskStatus.RUNNING.value]
        if namespace is not None:
            sql += " AND namespace=?"
            args.append(namespace)
        with self._lock:
            return self._conn.execute(sql, args).fetchone()[0]

    # ---------- 消费位点 ----------

    def get_offset(self, group: str, topic: str) -> int:
        """已提交位点（下一条待消费的 offset），无记录时为 0。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT offset FROM offsets WHERE grp=? AND topic=?",
                (group, topic)).fetchone()
        return row[0] if row else 0

    def commit_offset(self, group: str, topic: str, offset: int,
                      now: float) -> None:
        """原子提交位点：只允许严格前进。

        重复提交相同位点或回退都抛 ConflictError，不会从中间继续。
        """
        if offset < 0:
            raise ConflictError(f"offset must be >= 0, got {offset}")
        with self._lock, self._conn:
            row = self._conn.execute(
                "SELECT offset FROM offsets WHERE grp=? AND topic=?",
                (group, topic)).fetchone()
            current = row[0] if row else 0
            if offset <= current:
                raise ConflictError(
                    f"offset regression for {group}/{topic}:"
                    f" committed={current}, got={offset}")
            self._conn.execute(
                "INSERT INTO offsets(grp, topic, offset, updated_at)"
                " VALUES(?,?,?,?)"
                " ON CONFLICT(grp, topic) DO UPDATE SET"
                " offset=excluded.offset, updated_at=excluded.updated_at",
                (group, topic, offset, now))

    # ---------- 事件日志 ----------

    def append_event(self, topic: str, payload: dict, now: float) -> int:
        """追加事件，返回分配的 offset（每个 topic 内单调递增）。"""
        blob = json.dumps(payload, sort_keys=True).encode("utf-8")
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT MAX(offset) FROM events WHERE topic=?",
                (topic,)).fetchone()
            offset = (row[0] + 1) if row[0] is not None else 0
            self._conn.execute(
                "INSERT INTO events(topic, offset, payload, ts)"
                " VALUES(?,?,?,?)", (topic, offset, blob, now))
            return offset

    def read_events(self, topic: str, from_offset: int,
                    limit: int) -> list[tuple[int, dict]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT offset, payload FROM events"
                " WHERE topic=? AND offset>=? ORDER BY offset LIMIT ?",
                (topic, from_offset, limit)).fetchall()
        return [(r[0], json.loads(r[1].decode("utf-8"))) for r in rows]

    # ---------- 事件认领与确认（组内并行消费） ----------
    #
    # event_claims.state: 0 = 在途（已认领待确认），1 = 已确认（连续提交后删除）。
    # 一行即一条事件对一个消费组的处理状态，UPSERT 的 WHERE 条件保证
    # “同一时刻同一条事件只被一个消费者有效持有”；ack/nack 都校验 owner，
    # 崩溃接手后的迟到 ack 不生效，同一条事件不可能被算成两次成功。

    def claim_events(self, group: str, topic: str, owner: str,
                     max_events: int, ttl: float, inflight_cap: int,
                     now: float) -> list[tuple[int, dict]]:
        """原子认领一批在途之外的事件（含租约过期可接手的事件）。

        规则：
        - 仅认领 offset >= 组已提交位点的事件；
        - 已被其他消费者有效认领（state=0 且未过期）的事件不会重复认领；
        - 组内在途事件数达到 inflight_cap 时不再认领，资源占用有界；
        - 过期认领（消费者崩溃/超时）可被任意 owner 原子接手。
        """
        if max_events <= 0 or inflight_cap <= 0:
            return []
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            committed = self.get_offset(group, topic)
            inflight = self._conn.execute(
                "SELECT COUNT(*) FROM event_claims"
                " WHERE grp=? AND topic=? AND state=0 AND expires_at>?",
                (group, topic, now)).fetchone()[0]
            budget = inflight_cap - inflight
            if budget <= 0:
                return []
            want = min(max_events, budget)
            # 扫描窗口有界：在途最多 inflight_cap 条，再多取 want 条候选。
            # 可认领候选：无认领行，或认领已过期（state=0 且到期）；
            # state=1（已确认，等待连续提交）的事件绝不可再认领。
            rows = self._conn.execute(
                "SELECT e.offset, e.payload FROM events e"
                " WHERE e.topic=? AND e.offset>=?"
                " AND NOT EXISTS ("
                "   SELECT 1 FROM event_claims c WHERE c.grp=? AND c.topic=?"
                "   AND c.offset=e.offset AND ("
                "     c.state=1 OR (c.state=0 AND c.expires_at>?)))"
                " ORDER BY e.offset LIMIT ?",
                (topic, committed, group, topic, now,
                 inflight_cap + want)).fetchall()
            chosen = [(o, json.loads(b.decode("utf-8")))
                      for o, b in rows[:want]]
            for offset, _ in chosen:
                # 只覆盖“无行/过期在途”；state=1 与有效在途被 WHERE 挡住，
                # 并发下也不会抢成功。
                self._conn.execute(
                    "INSERT INTO event_claims(grp, topic, offset, owner,"
                    " expires_at, state) VALUES(?,?,?,?,?,0)"
                    " ON CONFLICT(grp, topic, offset) DO UPDATE SET"
                    " owner=excluded.owner, expires_at=excluded.expires_at,"
                    " state=0"
                    " WHERE event_claims.state=0 AND event_claims.expires_at<=?",
                    (group, topic, offset, owner, now + ttl, now))
            return chosen

    def ack_event(self, group: str, topic: str, offset: int,
                  owner: str, now: float) -> tuple[bool, int]:
        """确认单条事件处理成功，并把组位点连续推进到最大无缺口处。

        返回 (applied, committed_offset)：
        - applied=True：本次确认首次生效（该事件当前仍由该 owner 有效持有）；
        - applied=False：重复确认、被接手后的迟到确认或事件早已越过提交
          位点——不计成功、不产生重复的位点提交。

        乱序完成时，中间未确认（在途/尚未认领）的缺口会挡住边界；
        缺口补齐后下一次 ack 把位点一次性连续推进，不跳号。
        """
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT offset FROM offsets WHERE grp=? AND topic=?",
                (group, topic)).fetchone()
            committed = row[0] if row else 0
            if offset < committed:
                return False, committed  # 已连续提交过：幂等忽略
            cur = self._conn.execute(
                "UPDATE event_claims SET state=1, expires_at=0"
                " WHERE grp=? AND topic=? AND offset=? AND owner=?"
                " AND state=0 AND expires_at>?",
                (group, topic, offset, owner, now))
            if cur.rowcount == 0:
                return False, committed  # 非本人有效持有或重复确认
            next_offset = self._contiguous_offset(group, topic, committed)
            if next_offset > committed:
                self._conn.execute(
                    "INSERT INTO offsets(grp, topic, offset, updated_at)"
                    " VALUES(?,?,?,?)"
                    " ON CONFLICT(grp, topic) DO UPDATE SET"
                    " offset=excluded.offset, updated_at=excluded.updated_at",
                    (group, topic, next_offset, now))
                # 已被连续提交覆盖的确认行不再需要，删除以保持存储有界
                self._conn.execute(
                    "DELETE FROM event_claims"
                    " WHERE grp=? AND topic=? AND state=1 AND offset<?",
                    (group, topic, next_offset))
                return True, next_offset
            return True, committed

    def _contiguous_offset(self, group: str, topic: str,
                           committed: int) -> int:
        """从 committed 出发求最大连续前缀边界。

        每前进一步要求该 offset 已确认(state=1)；遇到在途(state=0)或
        尚无认领行的 offset 即停止。在途行数有界（inflight_cap），
        故此循环步数有界。
        """
        offset = committed
        while True:
            row = self._conn.execute(
                "SELECT state FROM event_claims"
                " WHERE grp=? AND topic=? AND offset=?",
                (group, topic, offset)).fetchone()
            if row is None or row[0] != 1:
                return offset
            offset += 1

    def nack_event(self, group: str, topic: str, offset: int,
                   owner: str) -> bool:
        """显式放弃认领（处理失败），立即可被重新认领。返回是否生效。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "DELETE FROM event_claims"
                " WHERE grp=? AND topic=? AND offset=? AND owner=?"
                " AND state=0",
                (group, topic, offset, owner))
            return cur.rowcount > 0

    def renew_claims(self, group: str, topic: str, owner: str,
                     ttl: float, now: float) -> int:
        """为该 owner 仍持有的全部在途认领续租，返回续期条数。"""
        with self._lock, self._conn:
            cur = self._conn.execute(
                "UPDATE event_claims SET expires_at=?"
                " WHERE grp=? AND topic=? AND owner=? AND state=0",
                (now + ttl, group, topic, owner))
            return cur.rowcount

    def inflight_count(self, group: str, topic: str, now: float) -> int:
        """组内在途（state=0 且未过期）事件数。"""
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM event_claims"
                " WHERE grp=? AND topic=? AND state=0 AND expires_at>?",
                (group, topic, now)).fetchone()
            return row[0]

    # ---------- 消费组租约（组内单活，旧语义保留） ----------

    def acquire_lease(self, group: str, topic: str, owner: str,
                      ttl: float, now: float) -> bool:
        """获取组内消费租约；已被他人持有且未过期时返回 False。"""
        with self._lock, self._conn:
            self._conn.execute("BEGIN IMMEDIATE")
            row = self._conn.execute(
                "SELECT owner, expires_at FROM leases"
                " WHERE grp=? AND topic=?", (group, topic)).fetchone()
            if row is not None and row[1] > now and row[0] != owner:
                return False
            self._conn.execute(
                "INSERT INTO leases(grp, topic, owner, expires_at)"
                " VALUES(?,?,?,?)"
                " ON CONFLICT(grp, topic) DO UPDATE SET"
                " owner=excluded.owner, expires_at=excluded.expires_at",
                (group, topic, owner, now + ttl))
            return True

    def release_lease(self, group: str, topic: str, owner: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "DELETE FROM leases WHERE grp=? AND topic=? AND owner=?",
                (group, topic, owner))

    # ---------- 内部 ----------

    @staticmethod
    def _row_to_task(row) -> Task:
        (task_id, namespace, blob, dedup_key, status,
         attempts, created_at, updated_at) = row
        return Task(task_id=task_id, namespace=namespace,
                    payload=payload_codec.decode(blob), dedup_key=dedup_key,
                    status=TaskStatus(status), attempts=attempts,
                    created_at=created_at, updated_at=updated_at)
