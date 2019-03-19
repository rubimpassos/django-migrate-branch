import re
import subprocess
import time
from importlib import import_module
from pathlib import Path

from django.apps import apps
from django.core.checks import run_checks, Tags
from django.core.management import BaseCommand, CommandError
from django.core.management.sql import emit_pre_migrate_signal, emit_post_migrate_signal
from django.db import DEFAULT_DB_ALIAS, connections
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.recorder import MigrationRecorder
from django.db.migrations.state import ModelState
from django.utils.module_loading import module_has_submodule
from django.utils.text import Truncator


class Command(BaseCommand):
    help = 'Unapply all migrations made between branches'
    branch = None
    verbosity = 1
    interactive = False

    def add_arguments(self, parser):
        parser.add_argument('branch', type=str, help='Branch where to migrate')
        parser.add_argument(
            '--noinput', '--no-input', action='store_false', dest='interactive',
            help='Tells Django to NOT prompt the user for input of any kind.',
        )
        parser.add_argument(
            '--database',
            default=DEFAULT_DB_ALIAS,
            help='Nominates a database to synchronize. Defaults to the "default" database.',
        )
        parser.add_argument(
            '--fake', action='store_true',
            help='Mark migrations as run without actually running them.',
        )
        parser.add_argument(
            '--fake-initial', action='store_true',
            help='Detect if tables already exist and fake-apply initial migrations if so. Make sure '
                 'that the current database schema matches your initial migration before using this '
                 'flag. Django will only check for an existing table name.',
        )
        parser.add_argument(
            '--plan', action='store_true',
            help='Shows a list of the migration actions that will be performed.',
        )

    def _run_checks(self, **kwargs):
        issues = run_checks(tags=[Tags.database])
        issues.extend(super()._run_checks(**kwargs))
        return issues

    def handle(self, *args, **options):

        self.branch = options['branch']
        self.verbosity = options['verbosity']
        self.interactive = options['interactive']

        # Import the 'management' module within each installed app, to register
        # dispatcher events.
        for app_config in apps.get_app_configs():
            if module_has_submodule(app_config.module, "management"):
                import_module('.management', app_config.name)

        # Get the database we're operating from
        db = options['database']
        connection = connections[db]

        # Hook for backends needing any database preparation
        connection.prepare_database()
        # Work out which apps have migrations and which do not
        executor = MigrationExecutor(connection, self.migration_progress_callback)

        # Raise an error if any migrations are applied before their dependencies.
        executor.loader.check_consistent_history(connection)

        targets = []
        migrations = {}
        migration_paths = self.get_diff_migrations()

        # Group diff migrations by app_label
        for p in migration_paths:
            app_label = p.parts[0]
            if app_label not in migrations:
                # Validate app_label.
                try:
                    apps.get_app_config(app_label)
                except LookupError as err:
                    raise CommandError(str(err))
                migrations[app_label] = []
            migrations[app_label].append(p.stem)

        # Get last applied migration for each app
        for app_label, migration_names in migrations.items():
            mr = MigrationRecorder.Migration.objects.filter(app=app_label)

            # Skip if already unapplied
            if not mr.filter(name__in=migration_names).count():
                continue

            mr = mr.exclude(name__in=migration_names).order_by('applied').last()
            if mr:
                targets.append((app_label, mr.name))

        if not targets:
            if self.verbosity >= 1:
                self.stdout.write("  No migrations to unapply.")
            return

        plan = executor.migration_plan(targets)

        if options['plan']:
            self.stdout.write('Planned operations:', self.style.MIGRATE_LABEL)
            if not plan:
                self.stdout.write('  No planned migration operations.')
            for migration, backwards in plan:
                self.stdout.write(str(migration), self.style.MIGRATE_HEADING)
                for operation in migration.operations:
                    message, is_error = self.describe_operation(operation, backwards)
                    style = self.style.WARNING if is_error else None
                    self.stdout.write('    ' + message, style)
            return

        # Print some useful info
        if self.verbosity >= 1:
            self.stdout.write(self.style.MIGRATE_HEADING("Operations to perform:"))
            for app_label, migration_name in targets:
                self.stdout.write(
                    self.style.MIGRATE_LABEL("  Target specific migration: ") +
                    "%s, from %s" % (app_label, migration_name)
                )

        # noinspection PyProtectedMember
        pre_migrate_state = executor._create_project_state(with_applied_migrations=True)
        pre_migrate_apps = pre_migrate_state.apps
        emit_pre_migrate_signal(
            self.verbosity, self.interactive, connection.alias, apps=pre_migrate_apps, plan=plan,
        )

        # Migrate!
        if self.verbosity >= 1:
            self.stdout.write(self.style.MIGRATE_HEADING("Running migrations:"))

        if not plan:
            fake = False
            fake_initial = False
        else:
            fake = options['fake']
            fake_initial = options['fake_initial']

        post_migrate_state = executor.migrate(
            targets, plan=plan, state=pre_migrate_state.clone(), fake=fake,
            fake_initial=fake_initial,
        )
        # post_migrate signals have access to all models. Ensure that all models
        # are reloaded in case any are delayed.
        post_migrate_state.clear_delayed_apps_cache()
        post_migrate_apps = post_migrate_state.apps

        # Re-render models of real apps to include relationships now that
        # we've got a final state. This wouldn't be necessary if real apps
        # models were rendered with relationships in the first place.
        with post_migrate_apps.bulk_update():
            model_keys = []
            for model_state in post_migrate_apps.real_models:
                model_key = model_state.app_label, model_state.name_lower
                model_keys.append(model_key)
                post_migrate_apps.unregister_model(*model_key)
        post_migrate_apps.render_multiple([
            ModelState.from_model(apps.get_model(*model)) for model in model_keys
        ])

        # Send the post_migrate signal, so individual apps can do whatever they need
        # to do at this point.
        emit_post_migrate_signal(
            self.verbosity, self.interactive, connection.alias, apps=post_migrate_apps, plan=plan,
        )

    def migration_progress_callback(self, action, migration=None, fake=False):
        if self.verbosity >= 1:
            compute_time = self.verbosity > 1
            if action == "apply_start":
                if compute_time:
                    self.start = time.time()
                self.stdout.write("  Applying %s..." % migration, ending="")
                self.stdout.flush()
            elif action == "apply_success":
                elapsed = " (%.3fs)" % (time.time() - self.start) if compute_time else ""
                if fake:
                    self.stdout.write(self.style.SUCCESS(" FAKED" + elapsed))
                else:
                    self.stdout.write(self.style.SUCCESS(" OK" + elapsed))
            elif action == "unapply_start":
                if compute_time:
                    self.start = time.time()
                self.stdout.write("  Unapplying %s..." % migration, ending="")
                self.stdout.flush()
            elif action == "unapply_success":
                elapsed = " (%.3fs)" % (time.time() - self.start) if compute_time else ""
                if fake:
                    self.stdout.write(self.style.SUCCESS(" FAKED" + elapsed))
                else:
                    self.stdout.write(self.style.SUCCESS(" OK" + elapsed))
            elif action == "render_start":
                if compute_time:
                    # noinspection PyAttributeOutsideInit
                    self.start = time.time()
                self.stdout.write("  Rendering model states...", ending="")
                self.stdout.flush()
            elif action == "render_success":
                elapsed = " (%.3fs)" % (time.time() - self.start) if compute_time else ""
                self.stdout.write(self.style.SUCCESS(" DONE" + elapsed))

    def get_diff_migrations(self):
        """Return diff migrations between current branch and passed --branch"""
        git_branches = subprocess.getoutput('git branch')
        match = re.findall(r'^\* ([^ ]+)$', git_branches, flags=re.MULTILINE)
        current_branch = match[0] if match else None

        if current_branch is None:
            raise CommandError('Not in a branch. Are you on HEAD?')

        if self.branch not in [b.strip() for b in git_branches.splitlines()]:
            raise CommandError('Local branch %s not found.' % self.branch)

        cmd = 'bash -c '
        cmd += '"git diff --name-only %s..%s | grep -E \'/migrations/\' | sort | uniq"' % (self.branch, current_branch)

        status, output = subprocess.getstatusoutput(cmd)
        if status or 'fatal:' in output:
            raise CommandError('Call to git diff failed\n%s' % output)

        return [Path(p) for p in output.splitlines()]

    @staticmethod
    def describe_operation(operation, backwards):
        """Return a string that describes a migration operation for --plan."""
        prefix = ''
        if hasattr(operation, 'code'):
            code = operation.reverse_code if backwards else operation.code
            action = code.__doc__ if code else ''
        elif hasattr(operation, 'sql'):
            action = operation.reverse_sql if backwards else operation.sql
        else:
            action = ''
            if backwards:
                prefix = 'Undo '
        if action is None:
            action = 'IRREVERSIBLE'
            is_error = True
        else:
            action = str(action).replace('\n', '')
            is_error = False
        if action:
            action = ' -> ' + action
        truncated = Truncator(action)
        return prefix + operation.describe() + truncated.chars(40), is_error
