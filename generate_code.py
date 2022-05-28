# artwork-indexer - update artwork index files at the Internet Archive
#
# Copyright (C) 2021  MetaBrainz Foundation
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

# flake8: noqa E501

import os
from textwrap import dedent
from projects import PROJECTS


curdir = os.path.dirname(__file__)

TG_OP_FULLNAMES = {
    'ins': 'INSERT',
    'upd': 'UPDATE',
    'del': 'DELETE',
}

handlers_source = dedent('''\
    # Automatically generated, do not edit.

    from handlers_base import MusicBrainzEventHandler


    ''')
handler_classes_source = ''
handler_classes_dict_source = 'EVENT_HANDLER_CLASSES = {\n'


for project in PROJECTS:
    abbr = project['abbr']
    art_schema = project['art_schema']
    art_table = project['art_table']
    entity_schema = project['entity_schema']
    entity_table = project['entity_table']
    entity_type = entity_table

    # q = qualified
    q_art_table = f'{art_schema}.{art_table}'
    q_entity_table = f'{entity_schema}.{entity_table}'
    # Note: cover_art_archive.image_type is also used by the
    # event_art_archive schema.
    q_image_type_table = 'cover_art_archive.image_type'

    extra_functions_source = ''
    extra_triggers_source = ''

    for im in project['indexed_metadata']:
        im_schema = im['schema']
        im_table = im['table']
        q_im_table = f'{im_schema}.{im_table}'

        for tg_op in im['tg_ops']:
            extra_functions_source += f'\nCREATE OR REPLACE FUNCTION a_{tg_op}_{im_table}() RETURNS trigger AS $$\n'
            extra_functions_source += 'BEGIN\n'

            tg_rowvar = 'OLD' if tg_op == 'del' else 'NEW'
            indent = '    '

            col_comparisons = []
            if tg_op == 'upd':
                for col in im['indexed_columns']:
                    col_name = col['name']
                    if col['nullable']:
                        col_comparisons.append(f'OLD.{col_name} IS DISTINCT FROM NEW.{col_name}')
                    else:
                        col_comparisons.append(f'OLD.{col_name} != NEW.{col_name}')

            if col_comparisons:
                col_comparisons_source = ' OR '.join(col_comparisons)
                extra_functions_source += f'{indent}IF ({col_comparisons_source}) THEN\n'
                indent += '    '

            extra_functions_source += f'{indent}INSERT INTO artwork_indexer.event_queue (entity_type, action, message)'

            if q_im_table == q_entity_table:
                extra_functions_source += f"\n{indent}VALUES ('{entity_type}', 'index', jsonb_build_object('gid', {tg_rowvar}.gid))\n"
            else:
                indent += '    '
                extra_functions_source += ' (\n'
                extra_functions_source += f"{indent}SELECT '{entity_type}', 'index', jsonb_build_object('gid', {q_entity_table}.gid)\n"
                extra_functions_source += f'{indent}FROM {q_entity_table}\n'

                for join in im.get('joins', ()):
                    (lhs_schema, lhs_table, lhs_col) = join['lhs']
                    (rhs_schema, rhs_table, rhs_col) = join['rhs']

                    q_lhs_table = f'{lhs_schema}.{lhs_table}'
                    q_rhs_table = f'{rhs_schema}.{rhs_table}'

                    extra_functions_source += f'{indent}JOIN {q_lhs_table} ON {q_lhs_table}.{lhs_col} = {q_rhs_table}.{rhs_col}\n'

                im_condition = im.get('condition')
                if im_condition:
                    extra_functions_source += f'{indent}WHERE {im_condition.format(tg_rowvar=tg_rowvar)}\n'

                indent = indent[:-4]
                extra_functions_source += f'{indent})\n'

            extra_functions_source += f'{indent}ON CONFLICT DO NOTHING;\n'
            if col_comparisons:
                indent = indent[:-4]
                extra_functions_source += f'{indent}END IF;\n'

            extra_functions_source += f'\n{indent}RETURN {tg_rowvar};\n'
            extra_functions_source += 'END;\n'
            extra_functions_source += '$$ LANGUAGE plpgsql;\n'

            tg_fn_name = f'a_{tg_op}_{im_table}'
            tg_name = f'{tg_fn_name}_{abbr}'

            extra_triggers_source += dedent(f'''
                DROP TRIGGER IF EXISTS {tg_name} ON {q_im_table};

                CREATE TRIGGER {tg_name} AFTER {TG_OP_FULLNAMES[tg_op]}
                    ON {q_im_table} FOR EACH ROW
                    EXECUTE PROCEDURE {art_schema}.{tg_fn_name}();
            ''')

    def index_artwork_stmt(gids, parent):
        stmt = 'INSERT INTO artwork_indexer.event_queue ('
        stmt += 'entity_type, action, message'
        if parent:
            stmt += ', depends_on'
        stmt += ') VALUES '
        stmt += ', '.join([
            (
                f"('{entity_type}', 'index', jsonb_build_object('gid', {gid})" +
                (f", array[{parent}])" if parent else ')')
            ) for gid in gids
        ])
        stmt += ' ON CONFLICT ';
        if parent:
            stmt += "(entity_type, action, message) WHERE state = 'queued' "
            stmt += f"DO UPDATE SET depends_on = (coalesce(artwork_indexer.event_queue.depends_on, '{{}}') || {parent});"
        else:
            stmt += 'DO NOTHING;'
        return stmt

    def delete_artwork_stmt(artwork_id, gid, suffix, parent, return_var):
        stmt = 'INSERT INTO artwork_indexer.event_queue ('
        stmt += 'entity_type, action, message'
        if parent:
            stmt += ', depends_on'
        stmt += ') VALUES ('
        stmt += f"'{entity_type}', 'delete_image', "
        stmt += 'jsonb_build_object('
        stmt += f"'artwork_id', {artwork_id}, "
        stmt += f"'gid', {gid}, "
        stmt += f"'suffix', {suffix}"
        stmt += ')'
        if parent:
            stmt += f", array[{parent}]"
        stmt += f') RETURNING id INTO STRICT {return_var};'
        return stmt

    functions_source = dedent(f'''\
        -- Automatically generated, do not edit.

        \\set ON_ERROR_STOP 1

        BEGIN;

        SET LOCAL search_path = {art_schema};

        CREATE OR REPLACE FUNCTION a_ins_{art_table}() RETURNS trigger AS $$
        DECLARE
            {entity_type}_gid UUID;
        BEGIN
            SELECT {q_entity_table}.gid
            INTO STRICT {entity_type}_gid
            FROM {q_entity_table}
            WHERE {q_entity_table}.id = NEW.{entity_type};

            {index_artwork_stmt((f'{entity_type}_gid',), None)}

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION a_upd_{art_table}() RETURNS trigger AS $$
        DECLARE
            suffix TEXT;
            old_{entity_type}_gid UUID;
            new_{entity_type}_gid UUID;
            copy_event_id BIGINT;
            delete_event_id BIGINT;
        BEGIN
            SELECT {q_image_type_table}.suffix, old_{entity_type}.gid, new_{entity_type}.gid
            INTO STRICT suffix, old_{entity_type}_gid, new_{entity_type}_gid
            FROM {q_art_table}
            JOIN {q_image_type_table} USING (mime_type)
            JOIN {q_entity_table} old_{entity_type} ON old_{entity_type}.id = OLD.{entity_type}
            JOIN {q_entity_table} new_{entity_type} ON new_{entity_type}.id = NEW.{entity_type}
            WHERE {q_art_table}.id = OLD.id;

            IF OLD.{entity_type} != NEW.{entity_type} THEN
                -- The {entity_type} column changed, meaning two entities were merged.
                -- We'll copy the image to the new {entity_type} and delete it from
                -- the old one. The deletion event should have the copy event as its
                -- parent, so that it doesn't run until that completes.
                --
                -- We have no ON CONFLICT specifiers on the copy_image or delete_image,
                -- events, because they should *not* conflict with any existing event.

                INSERT INTO artwork_indexer.event_queue (entity_type, action, message)
                VALUES ('{entity_type}', 'copy_image', jsonb_build_object(
                    'artwork_id', OLD.id,
                    'old_gid', old_{entity_type}_gid,
                    'new_gid', new_{entity_type}_gid,
                    'suffix', suffix
                ))
                RETURNING id INTO STRICT copy_event_id;

                {delete_artwork_stmt('OLD.id', f'old_{entity_type}_gid', 'suffix', 'copy_event_id', 'delete_event_id')}

                -- If there's an existing, queued index event, reset its parent to our
                -- deletion event (i.e. delay it until after the deletion executes).
                {index_artwork_stmt((f'old_{entity_type}_gid', f'new_{entity_type}_gid'), 'delete_event_id')}
            ELSE
                {index_artwork_stmt((f'old_{entity_type}_gid', f'new_{entity_type}_gid'), None)}
            END IF;

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION a_del_{art_table}()
        RETURNS trigger AS $$
        DECLARE
            suffix TEXT;
            {entity_type}_gid UUID;
            delete_event_id BIGINT;
        BEGIN
            SELECT {q_image_type_table}.suffix, {q_entity_table}.gid
            INTO STRICT suffix, {entity_type}_gid
            FROM {q_entity_table}
            JOIN {q_image_type_table} ON {q_image_type_table}.mime_type = OLD.mime_type
            WHERE {q_entity_table}.id = OLD.{entity_type};

            {delete_artwork_stmt('OLD.id', f'{entity_type}_gid', 'suffix', None, 'delete_event_id')}
            {index_artwork_stmt((f'{entity_type}_gid',), 'delete_event_id')}

            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION a_ins_{art_table}_type() RETURNS trigger AS $$
        DECLARE
            {entity_type}_gid UUID;
        BEGIN
            SELECT {q_entity_table}.gid
            INTO STRICT {entity_type}_gid
            FROM {q_entity_table}
            JOIN {q_art_table} ON {q_entity_table}.id = {q_art_table}.{entity_type}
            WHERE {q_art_table}.id = NEW.id;

            {index_artwork_stmt((f'{entity_type}_gid',), None)}

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION a_del_{art_table}_type() RETURNS trigger AS $$
        DECLARE
            {entity_type}_gid UUID;
        BEGIN
            SELECT {q_entity_table}.gid
            INTO {entity_type}_gid
            FROM {q_entity_table}
            JOIN {q_art_table} ON {q_entity_table}.id = {q_art_table}.{entity_type}
            WHERE {q_art_table}.id = OLD.id;

            -- If no row is found, it's likely because the artwork itself has been
            -- deleted, which cascades to this table.
            IF FOUND THEN
                {index_artwork_stmt((f'{entity_type}_gid',), None)}
            END IF;

            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;

        CREATE OR REPLACE FUNCTION a_del_{entity_table}() RETURNS trigger AS $$
        BEGIN
            INSERT INTO artwork_indexer.event_queue (entity_type, action, message)
            VALUES ('{entity_type}', 'deindex', jsonb_build_object('gid', OLD.gid))
            ON CONFLICT DO NOTHING;

            -- Delete any previous 'index' events that were queued; it's unlikely
            -- these exist, but if they do we can avoid having them run and fail.
            DELETE FROM artwork_indexer.event_queue
            WHERE state = 'queued'
            AND entity_type = '{entity_type}'
            AND action = 'index'
            AND message = jsonb_build_object('gid', OLD.gid::text);

            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
        ''')

    functions_source += extra_functions_source + '\n'
    functions_source += 'COMMIT;\n'

    functions_fpath = os.path.join(curdir, f'sql/{abbr}_functions.sql')
    with open(functions_fpath, 'w') as fp:
        fp.write(functions_source)

    triggers_source = dedent(f'''\
        -- Automatically generated, do not edit.

        \\set ON_ERROR_STOP 1

        BEGIN;

        SET LOCAL search_path = '{art_schema}';
        SET LOCAL client_min_messages = warning;

        -- We drop the triggers first to simulate "CREATE OR REPLACE,"
        -- which isn't implemented for "CREATE TRIGGER."

        DROP TRIGGER IF EXISTS a_ins_{art_table}_{abbr} ON {art_schema}.{art_table};

        CREATE TRIGGER a_ins_{art_table}_{abbr} AFTER INSERT
            ON {art_schema}.{art_table} FOR EACH ROW
            EXECUTE PROCEDURE {art_schema}.a_ins_{art_table}();

        DROP TRIGGER IF EXISTS a_upd_{art_table}_{abbr} ON {art_schema}.{art_table};

        CREATE TRIGGER a_upd_{art_table}_{abbr} AFTER UPDATE
            ON {art_schema}.{art_table} FOR EACH ROW
            EXECUTE PROCEDURE {art_schema}.a_upd_{art_table}();

        DROP TRIGGER IF EXISTS a_del_{art_table}_{abbr} ON {art_schema}.{art_table};

        CREATE TRIGGER a_del_{art_table}_{abbr} AFTER DELETE
            ON {art_schema}.{art_table} FOR EACH ROW
            EXECUTE PROCEDURE {art_schema}.a_del_{art_table}();

        DROP TRIGGER IF EXISTS a_ins_{art_table}_type_{abbr} ON {art_schema}.{art_table}_type;

        CREATE TRIGGER a_ins_{art_table}_type_{abbr} AFTER INSERT
            ON {art_schema}.{art_table}_type FOR EACH ROW
            EXECUTE PROCEDURE {art_schema}.a_ins_{art_table}_type();

        DROP TRIGGER IF EXISTS a_del_{art_table}_type_{abbr} ON {art_schema}.{art_table}_type;

        CREATE TRIGGER a_del_{art_table}_type_{abbr} AFTER DELETE
            ON {art_schema}.{art_table}_type FOR EACH ROW
            EXECUTE PROCEDURE {art_schema}.a_del_{art_table}_type();

        DROP TRIGGER IF EXISTS a_del_{entity_table}_{abbr} ON {entity_schema}.{entity_table};

        CREATE TRIGGER a_del_{entity_table}_{abbr} AFTER DELETE
            ON {entity_schema}.{entity_table} FOR EACH ROW
            EXECUTE PROCEDURE {art_schema}.a_del_{entity_table}();
        ''')

    triggers_source += extra_triggers_source + '\n'
    triggers_source += 'COMMIT;\n'

    triggers_fpath = os.path.join(curdir, f'sql/{abbr}_triggers.sql')
    with open(triggers_fpath, 'w') as fp:
        fp.write(triggers_source)

    handler_class_name = entity_type.title() + 'EventHandler'
    handler_classes_source += dedent(f'''\
        class {handler_class_name}(MusicBrainzEventHandler):

            @property
            def artwork_schema(self):
                return '{art_schema}'

            @property
            def domain(self):
                return '{project['domain']}'

            @property
            def entity_type(self):
                return '{entity_type}'

            @property
            def ia_collection(self):
                return '{project['ia_collection']}'

            @property
            def ws_inc_params(self):
                return '{project['ws_inc_params']}'


    ''')
    handler_classes_dict_source += \
        f"    '{entity_type}': {handler_class_name},\n"


handler_classes_dict_source += '}'
handlers_source += handler_classes_source
handlers_source += handler_classes_dict_source
handlers_source += '\n'

handlers_fpath = os.path.join(curdir, 'handlers.py')
with open(handlers_fpath, 'w') as fp:
    fp.write(handlers_source)
