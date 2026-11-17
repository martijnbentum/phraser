'''Real process termination at transaction and receipt boundaries.'''

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import unittest

from phraser import Store
from phraser.repair import apply_change_plan, check_change_state
from phraser.repair import changes, create_change_plan, plan_update
from phraser.storage import key_helper
from test_repairs import RepairCase


def _pause(boundary, ready):
    '''Notify the parent only after the chosen boundary has been reached.'''
    ready.write_text(boundary)
    time.sleep(30)
    raise RuntimeError('Parent did not terminate the test worker')


def _worker(store_path, record_path, boundary, ready, pause_after=1):
    '''Block at an actual write or before publishing the applied receipt.'''
    store = Store(store_path, eager_query_roots=False)
    write_change = changes._write_change
    write_receipt = changes.write_receipt
    writes = 0

    def blocked_write(store, txn, change, field):
        nonlocal writes
        write_change(store, txn, change, field)
        writes += 1
        if boundary == 'during_transaction' and writes == pause_after:
            _pause(boundary, ready)

    def blocked_receipt(path, content):
        if boundary == 'after_commit' and content['status'] == 'applied':
            _pause(boundary, ready)
        write_receipt(path, content)

    changes._write_change = blocked_write
    changes.write_receipt = blocked_receipt
    try: apply_change_plan(store, record_path)
    finally: store.close()


@unittest.skipUnless(hasattr(signal, 'SIGKILL'), 'Requires SIGKILL')
class TestRepairInterruptions(RepairCase):
    def setUp(self):
        super().setUp()
        *_, self.phone = self.tree()
        self.old_index = key_helper.instance_to_label_index_key(self.phone)
        self.new_index = key_helper.label_to_label_index_key('b', 'Phone',
            self.phone.key)
        with self.store.DB.env.begin() as txn:
            plan = create_change_plan(self.store, txn, 'Relabel one Phone')
            plan_update(plan, self.store, txn, self.phone.key, label='b')
        self.path = self.save(plan)
        receipt = json.loads(self.path.read_text())
        self.payload = self.path.parent / receipt['payload']['file']
        self.evidence = self.payload.read_bytes()
        self.payload_mtime = self.payload.stat().st_mtime_ns
        self.initial = self.snapshot()
        self.initial_transaction = self.store.DB.env.info()['last_txnid']
        self.change_count = len(plan)

    def terminate_at(self, boundary):
        '''Reopen only after the writer has died; no environment is forked.'''
        store_path = str(self.store.path)
        self.store.close()
        root = Path(__file__).resolve().parents[1]
        ready = self.root / 'ready'
        env = dict(os.environ)
        existing = env.get('PYTHONPATH', '')
        env['PYTHONPATH'] = str(root)
        if existing: env['PYTHONPATH'] += os.pathsep + existing
        command = [sys.executable, str(Path(__file__).resolve()), '--worker',
            store_path, str(self.path), boundary, str(ready)]
        process = subprocess.Popen(command, cwd=root, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.wait_for_boundary(process, ready)
            self.assertEqual(ready.read_text(), boundary)
            process.kill()
            stdout, stderr = process.communicate(timeout=10)
            self.assertEqual(process.returncode, -signal.SIGKILL,
                stdout + stderr)
        finally:
            if process.poll() is None: process.kill()
            process.communicate(timeout=10)
        with redirect_stdout(io.StringIO()):
            self.store = Store(store_path, eager_query_roots=False)
        self.addCleanup(self.store.close)

    def wait_for_boundary(self, process, ready):
        deadline = time.monotonic() + 20
        while not ready.exists():
            if process.poll() is not None:
                stdout, stderr = process.communicate(timeout=5)
                self.fail('Worker exited before boundary: ' + stdout + stderr)
            if time.monotonic() >= deadline:
                self.fail('Worker did not reach the interruption boundary')
            time.sleep(0.01)

    def assert_inspection_only(self, expected):
        '''Inspect interrupted state without implicit retries or mutation.'''
        receipt_bytes = self.path.read_bytes()
        snapshot = self.snapshot()
        state = check_change_state(self.store, self.path)
        self.assertEqual(state['state'], expected)
        self.assertEqual(state['recorded_status'], 'applying')
        self.assertEqual(state['counts'][expected], self.change_count)
        receipt = json.loads(receipt_bytes)
        self.assertIsNone(receipt['applied_txn_id'])
        self.assertFalse(receipt['verified'])
        with self.assertRaisesRegex(ValueError, 'inspect state'):
            apply_change_plan(self.store, self.path)
        self.assertEqual(self.snapshot(), snapshot)
        self.assertEqual(self.path.read_bytes(), receipt_bytes)
        self.assertEqual(self.payload.read_bytes(), self.evidence)
        self.assertEqual(self.payload.stat().st_mtime_ns, self.payload_mtime)
        return state

    def test_kill_during_transaction_leaves_original_rows_and_indexes(self):
        self.terminate_at('during_transaction')
        self.assertEqual(self.snapshot(), self.initial)
        state = self.assert_inspection_only('original')
        self.assertEqual(state['transaction_id'], self.initial_transaction)
        self.assertTrue(self.store.DB.key_exists(self.old_index,
            'label_segment'))
        self.assertFalse(self.store.DB.key_exists(self.new_index,
            'label_segment'))

    def test_kill_after_commit_preserves_rows_despite_applying_receipt(self):
        self.terminate_at('after_commit')
        state = self.assert_inspection_only('proposed')
        self.assertEqual(state['transaction_id'], self.initial_transaction + 1)
        self.assertEqual(self.store.load(self.phone.key).label, 'b')
        self.assertFalse(self.store.DB.key_exists(self.old_index,
            'label_segment'))
        self.assertTrue(self.store.DB.key_exists(self.new_index,
            'label_segment'))


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--worker':
        _worker(sys.argv[2], sys.argv[3], sys.argv[4], Path(sys.argv[5]))
    else: unittest.main()
