"""Pure unit tests for the Slurm reconcile core (no MongoDB / controller).

DB-backed tests for build_desired_slurm_state and the SyncSlurm operation
live in test_beanie.py alongside the other beanie fixtures.
"""

from ..slurm_sync import (
    AccountState,
    QOSState,
    SlurmSyncState,
    TRESLimit,
    count_deletions,
    parse_association_state,
    parse_qos_state,
    reconcile,
)


class TestTRESLimit:

    def test_parse_bare_mem_is_megabytes(self):
        t = TRESLimit.from_tres_string('cpu=100,mem=512000,gres/gpu:a100=4')
        assert t == TRESLimit(cpus=100, gpus=4, mem_megs=512000)

    def test_parse_suffixed_mem_and_gres_type_stripped(self):
        t = TRESLimit.from_tres_string('cpu=8,mem=16G,gres/gpu=2')
        assert t == TRESLimit(cpus=8, gpus=2, mem_megs=16384)

    def test_unset_and_negative_one_become_none(self):
        assert TRESLimit.from_tres_string('') == TRESLimit()
        assert TRESLimit.from_tres_string('cpu=-1,mem=-1,gres/gpu=-1') == TRESLimit()

    def test_render_unlimited_as_negative_one(self):
        assert TRESLimit().to_tres_string() == 'cpu=-1,mem=-1,gres/gpu=-1'
        assert TRESLimit(cpus=8, gpus=2, mem_megs=16384).to_tres_string() == \
            'cpu=8,mem=16384,gres/gpu=2'


class TestParsers:

    def test_parse_qos_skips_normal_and_maps_columns(self):
        text = (
            'Name|Priority|GrpTRES|MaxTRES|MaxTRESPU|Flags\n'
            'normal|0|||| \n'
            'lab-q|10|cpu=8,mem=16G,gres/gpu=2|cpu=4|cpu=2|DenyOnLimit\n'
        )
        state = parse_qos_state(text)
        assert set(state) == {'lab-q'}
        q = state['lab-q']
        assert q.group == TRESLimit(cpus=8, gpus=2, mem_megs=16384)
        assert q.job == TRESLimit(cpus=4)     # MaxTRES -> job
        assert q.user == TRESLimit(cpus=2)    # MaxTRESPU -> user
        assert q.priority == 10
        assert q.flags == frozenset({'DenyOnLimit'})

    def test_parse_associations_skips_root_splits_rows(self):
        text = (
            'Account|User|Partition|QOS|MaxJobs|GrpJobs|MaxSubmit|MaxWall\n'
            'root|||||||\n'
            'lab||||10|-1|20|7-00:00:00\n'
            'lab|alice|hi|lab-q||||\n'
            'lab|bob|hi|lab-q||||\n'
        )
        assocs, accounts = parse_association_state(text)
        assert 'root' not in accounts
        assert accounts['lab'] == AccountState(
            max_user_jobs=10, max_group_jobs=-1,
            max_submit_jobs=20, max_job_length='7-00:00:00',
        )
        assert assocs == {
            ('alice', 'lab', 'hi'): 'lab-q',
            ('bob', 'lab', 'hi'): 'lab-q',
        }

    def test_parse_account_coordinators_by_index(self):
        from ..slurm_sync import parse_account_coordinators
        # Parsed by position: an abbreviated header is tolerated, root is
        # kept (parse_state splits it out), empty cells are dropped.
        text = (
            'Account|Coords\n'
            'root|admin\n'
            'lab|alice,bob\n'
            'empty|\n'
            '|orphan\n'              # blank account → skipped
        )
        assert parse_account_coordinators(text) == {
            'root': frozenset({'admin'}),
            'lab': frozenset({'alice', 'bob'}),
        }


class TestReconcile:

    def _labels(self, batches):
        return [b.label for b in batches]

    def test_ordering_is_fixed(self):
        batches = reconcile(SlurmSyncState(), SlurmSyncState())
        assert self._labels(batches) == [
            'Add QOS', 'Modify QOS', 'Add accounts', 'Modify accounts',
            'Add user associations', 'Modify user associations',
            'Add coordinators', 'Set user default accounts',
            'Remove coordinators', 'Delete user associations',
            'Delete QOS', 'Delete accounts',
        ]
        # The default re-point must precede association deletes: a user's
        # current default association cannot be deleted.
        labels = self._labels(batches)
        assert labels.index('Set user default accounts') < \
            labels.index('Delete user associations')
        # Coordinator seats need the user to exist, and must be removed
        # while the account still exists.
        assert labels.index('Add coordinators') > \
            labels.index('Add user associations')
        assert labels.index('Remove coordinators') < \
            labels.index('Delete accounts')

    def test_identical_states_produce_no_commands(self):
        q = QOSState(group=TRESLimit(cpus=8), priority=5, flags=frozenset({'DenyOnLimit'}))
        state = SlurmSyncState(
            qos={'q': q},
            accounts={'lab': AccountState()},
            associations={('alice', 'lab', 'hi'): 'q'},
        )
        # Same content, distinct objects.
        other = SlurmSyncState(
            qos={'q': QOSState(group=TRESLimit(cpus=8), priority=5,
                               flags=frozenset({'DenyOnLimit'}))},
            accounts={'lab': AccountState()},
            associations={('alice', 'lab', 'hi'): 'q'},
        )
        batches = reconcile(state, other)
        assert all(not b.specs for b in batches)
        assert count_deletions(batches) == 0

    def test_add_modify_delete_classification(self):
        desired = SlurmSyncState(
            qos={'keep': QOSState(priority=1), 'new': QOSState(priority=2)},
            accounts={'lab': AccountState(max_user_jobs=5)},
            associations={('alice', 'lab', 'hi'): 'keep'},
        )
        current = SlurmSyncState(
            qos={'keep': QOSState(priority=9), 'gone': QOSState()},
            accounts={'lab': AccountState(max_user_jobs=1)},
            associations={('bob', 'lab', 'hi'): 'keep'},
        )
        by_label = {b.label: b for b in reconcile(desired, current)}

        assert [s.args[0] for s in by_label['Add QOS'].specs] == ['new']
        assert [s.args[0] for s in by_label['Modify QOS'].specs] == ['keep']
        assert [s.args[0] for s in by_label['Delete QOS'].specs] == ['gone']
        assert [s.args[0] for s in by_label['Modify accounts'].specs] == ['lab']
        # association add for alice, delete for bob
        add = by_label['Add user associations'].specs
        delete = by_label['Delete user associations'].specs
        assert any('user=alice' in s.args for s in add)
        assert any('user=bob' in s.args for s in delete)
        # deletions counted: gone QOS + bob assoc
        assert count_deletions(reconcile(desired, current)) == 2

    def test_qos_add_renders_full_arg_set(self):
        desired = SlurmSyncState(qos={
            'q': QOSState(group=TRESLimit(cpus=8, mem_megs=16384, gpus=2),
                          priority=10, flags=frozenset({'DenyOnLimit'})),
        })
        batch = next(b for b in reconcile(desired, SlurmSyncState()) if b.label == 'Add QOS')
        cmd = str(batch.specs[0])
        assert cmd.startswith('sacctmgr -iQ add qos q ')
        assert 'GrpTRES=cpu=8,mem=16384,gres/gpu=2' in cmd
        assert 'Priority=10' in cmd
        assert 'Flags=DenyOnLimit' in cmd


class TestCoordinators:

    def _batch(self, desired, current, label):
        return next(b for b in reconcile(desired, current) if b.label == label)

    def test_add_and_remove_classification(self):
        desired = SlurmSyncState(
            accounts={'lab': AccountState()},
            coordinators={('lab', 'alice'), ('lab', 'carol')},
        )
        current = SlurmSyncState(
            accounts={'lab': AccountState()},
            coordinators={('lab', 'alice'), ('lab', 'bob')},
        )
        add = self._batch(desired, current, 'Add coordinators')
        remove = self._batch(desired, current, 'Remove coordinators')
        assert [str(s) for s in add.specs] == [
            'sacctmgr -iQ add coordinator account=lab names=carol',
        ]
        assert [str(s) for s in remove.specs] == [
            'sacctmgr -iQ remove coordinator account=lab names=bob',
        ]
        assert remove.is_deletion
        assert count_deletions(reconcile(desired, current)) == 1

    def test_identical_seats_produce_no_commands(self):
        state = SlurmSyncState(
            accounts={'lab': AccountState()}, coordinators={('lab', 'alice')},
        )
        other = SlurmSyncState(
            accounts={'lab': AccountState()}, coordinators={('lab', 'alice')},
        )
        assert not self._batch(state, other, 'Add coordinators').specs
        assert not self._batch(state, other, 'Remove coordinators').specs

    def test_remove_skipped_for_deleted_account(self):
        # The account itself is going away; its seats go with it and must
        # not be counted twice against the deletion cap.
        desired = SlurmSyncState()
        current = SlurmSyncState(
            accounts={'stale': AccountState()},
            coordinators={('stale', 'alice')},
        )
        batches = reconcile(desired, current)
        by_label = {b.label: b for b in batches}
        assert [s.args[0] for s in by_label['Delete accounts'].specs] == ['stale']
        assert by_label['Remove coordinators'].specs == []
        assert count_deletions(batches) == 1

    def test_remove_skipped_for_unmanaged_account(self):
        # `show account` lists accounts with no associations on this
        # cluster; they're in neither accounts map and are left alone.
        desired = SlurmSyncState(accounts={'lab': AccountState()})
        current = SlurmSyncState(
            accounts={'lab': AccountState()},
            coordinators={('elsewhere', 'alice')},
        )
        assert not self._batch(desired, current, 'Remove coordinators').specs

    def test_root_coordinators_masked_on_both_sides(self):
        # Root's coordinators are inherited by every account: never removed
        # (the seat isn't on this account) and never added (already there).
        desired = SlurmSyncState(
            accounts={'lab': AccountState()},
            coordinators={('lab', 'admin'), ('lab', 'alice')},
        )
        current = SlurmSyncState(
            accounts={'lab': AccountState()},
            coordinators={('lab', 'admin'), ('other', 'admin')},
            root_coordinators=frozenset({'admin'}),
        )
        add = self._batch(desired, current, 'Add coordinators')
        remove = self._batch(desired, current, 'Remove coordinators')
        assert [str(s) for s in add.specs] == [
            'sacctmgr -iQ add coordinator account=lab names=alice',
        ]
        assert remove.specs == []


class TestDumpSAcctMgr:

    QOS_DUMP = (
        'Name|Priority|GrpTRES|MaxTRES|MaxTRESPU|Flags\n'
        'normal|0|||| \n'
        'lab-q|10|cpu=8,mem=16G,gres/gpu=2|||DenyOnLimit\n'
    )
    ASSOC_DUMP = (
        'Account|User|Partition|QOS|MaxJobs|GrpJobs|MaxSubmit|MaxWall\n'
        'root|||||||\n'
        'lab||||-1|-1|-1|-1\n'
        'lab|alice|hi|lab-q||||\n'
    )
    COORD_DUMP = (
        'Account|Coordinators\n'
        'root|admin\n'
        'lab|alice,admin\n'
    )

    async def test_reads_state_from_text(self):
        from ..slurm_sync import DumpSAcctMgr, AccountState, TRESLimit

        mgr = DumpSAcctMgr(
            qos_text=self.QOS_DUMP, associations_text=self.ASSOC_DUMP,
            coordinators_text=self.COORD_DUMP,
        )
        state = await mgr.read_current_state()
        assert set(state.qos) == {'lab-q'}  # normal skipped
        assert state.qos['lab-q'].group == TRESLimit(cpus=8, gpus=2, mem_megs=16384)
        assert state.accounts == {'lab': AccountState()}  # root skipped
        assert state.associations == {('alice', 'lab', 'hi'): 'lab-q'}
        # root's seats are split out; the inherited copy on lab stays in
        # `coordinators` and is masked by reconcile.
        assert state.root_coordinators == frozenset({'admin'})
        assert state.coordinators == {('lab', 'alice'), ('lab', 'admin')}

    async def test_from_files(self, tmp_path):
        from ..slurm_sync import DumpSAcctMgr

        qos_file = tmp_path / 'qos.txt'
        assoc_file = tmp_path / 'assoc.txt'
        coord_file = tmp_path / 'coord.txt'
        qos_file.write_text(self.QOS_DUMP)
        assoc_file.write_text(self.ASSOC_DUMP)
        coord_file.write_text(self.COORD_DUMP)

        mgr = DumpSAcctMgr.from_files(
            qos_file, assoc_file, coordinators_path=coord_file,
        )
        state = await mgr.read_current_state()
        assert set(state.qos) == {'lab-q'}
        assert state.associations == {('alice', 'lab', 'hi'): 'lab-q'}
        assert ('lab', 'alice') in state.coordinators

    async def test_dispatch_records_without_side_effects(self):
        from ..slurm_sync import DumpSAcctMgr, CommandSpec

        mgr = DumpSAcctMgr()
        await mgr.dispatch(CommandSpec('add', 'qos', ('q',)))
        assert mgr.dispatched == ['sacctmgr -iQ add qos q']

    async def test_empty_dumps_yield_empty_state(self):
        from ..slurm_sync import DumpSAcctMgr

        state = await DumpSAcctMgr().read_current_state()
        assert state.qos == {}
        assert state.accounts == {}
        assert state.associations == {}
        assert state.coordinators == set()
        assert state.root_coordinators == frozenset()


class TestDefaultAccounts:

    def test_parse_user_default_accounts_by_index(self):
        from ..slurm_sync import parse_user_default_accounts
        # Abbreviated header ('Def Acct') is tolerated — parsed by position.
        text = (
            'User|Def Acct|Admin\n'
            'alice|labacct|None\n'
            'bob|otheracct|None\n'
            '|skipme|x\n'          # blank user → skipped
        )
        assert parse_user_default_accounts(text) == {
            'alice': 'labacct', 'bob': 'otheracct',
        }

    def test_default_change_emitted_for_new_and_drifted_users(self):
        desired = SlurmSyncState(default_accounts={
            'alice': 'labacct',   # unchanged
            'bob': 'labacct',     # drifted (current other)
            'carol': 'labacct',   # new (absent from current)
        })
        current = SlurmSyncState(default_accounts={
            'alice': 'labacct',
            'bob': 'otheracct',
        })
        batch = next(
            b for b in reconcile(desired, current)
            if b.label == 'Set user default accounts'
        )
        users = {
            s.args[-1].split('=', 1)[1] for s in batch.specs  # where user=<u>
        }
        assert users == {'bob', 'carol'}  # alice unchanged → no command
        for spec in batch.specs:
            assert 'set' in spec.args
            assert 'defaultaccount=labacct' in spec.args

    def test_default_batch_not_counted_as_deletion(self):
        desired = SlurmSyncState(default_accounts={'bob': 'labacct'})
        assert count_deletions(reconcile(desired, SlurmSyncState())) == 0


class TestAsyncSAcctMgrExecPrefix:

    def test_exec_prefix_skips_host_resolution_and_prepends(self):
        # exec_prefix must NOT probe the host for a sacctmgr binary, and must
        # prepend the prefix to every command. 'env' stands in for the real
        # 'docker exec <container>' used in CI.
        from ..slurm_sync import AsyncSAcctMgr, CommandSpec
        mgr = AsyncSAcctMgr(exec_prefix=['env'])
        assert mgr.exec_prefix == ['env']
        rendered = str(mgr.render(CommandSpec(
            'modify', 'user',
            ('set', 'defaultaccount=lab', 'where', 'user=alice'),
        )))
        assert 'env sacctmgr -iQ ' in rendered
        assert rendered.endswith(
            'modify user set defaultaccount=lab where user=alice'
        )

    def test_coordinator_spec_renders_through_prefix(self):
        from ..slurm_sync import AsyncSAcctMgr, CommandSpec
        mgr = AsyncSAcctMgr(exec_prefix=['env'])
        rendered = str(mgr.render(CommandSpec(
            'add', 'coordinator', ('account=lab', 'names=alice'),
        )))
        assert rendered.endswith('add coordinator account=lab names=alice')
